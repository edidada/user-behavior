package org.example.behavior.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.example.behavior.common.constant.RedisKeys;
import org.example.behavior.common.util.JsonUtils;
import org.example.behavior.common.util.UserIdMasker;
import org.example.behavior.domain.command.CouponPushCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * Pushes coupon commands to the coupon service. A Redis SETNX key makes the
 * push idempotent: the same command can be replayed after a failure without
 * sending a second coupon. On a downstream failure the dedup key is released
 * so Flink's restart / retry can send it again.
 */
public class CouponPushSink extends RichSinkFunction<CouponPushCommand> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(CouponPushSink.class);

    private static final int DEDUP_TTL_SECONDS = 24 * 60 * 60;

    private final String redisHost;

    private final int redisPort;

    private final String couponEndpoint;

    private transient JedisPool jedisPool;

    private transient HttpClient httpClient;

    private transient ObjectMapper mapper;

    public CouponPushSink(String redisHost, int redisPort, String couponEndpoint) {
        this.redisHost = redisHost;
        this.redisPort = redisPort;
        this.couponEndpoint = couponEndpoint;
    }

    @Override
    public void open(Configuration parameters) {
        jedisPool = new JedisPool(redisHost, redisPort);
        httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(3))
                .build();
        mapper = JsonUtils.mapper();
    }

    @Override
    public void invoke(CouponPushCommand command, Context context) throws Exception {
        if (command == null || command.getUserId() == null || command.getIdempotentId() == null) {
            return;
        }
        String dedupKey = RedisKeys.couponDedup(command.getIdempotentId());
        if (!tryAcquire(dedupKey)) {
            LOG.debug("Duplicate coupon push skipped for user {}",
                    UserIdMasker.mask(command.getUserId()));
            return;
        }

        HttpRequest request = HttpRequest.newBuilder(URI.create(couponEndpoint))
                .timeout(Duration.ofSeconds(5))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(command)))
                .build();

        HttpResponse<String> response =
                httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() >= 300) {
            release(dedupKey);
            LOG.warn("Coupon service returned {} for user {}: {}",
                    response.statusCode(), UserIdMasker.mask(command.getUserId()), response.body());
        } else {
            LOG.info("Pushed coupon {} to user {} triggered by {}",
                    command.getCouponId(), UserIdMasker.mask(command.getUserId()),
                    command.getTriggerReason());
        }
    }

    @Override
    public void close() {
        if (jedisPool != null) {
            jedisPool.close();
        }
    }

    private boolean tryAcquire(String dedupKey) {
        try (Jedis jedis = jedisPool.getResource()) {
            long acquired = jedis.setnx(dedupKey, "1");
            if (acquired == 0L) {
                return false;
            }
            jedis.expire(dedupKey, DEDUP_TTL_SECONDS);
            return true;
        } catch (Exception e) {
            // If Redis is unavailable we prefer at-least-once delivery; the
            // coupon service must still be idempotent on its own.
            LOG.warn("Redis dedup unavailable, pushing without deduplication", e);
            return true;
        }
    }

    private void release(String dedupKey) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.del(dedupKey);
        } catch (Exception e) {
            LOG.warn("Failed to release dedup key {}", dedupKey, e);
        }
    }
}
