package org.example.behavior.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.example.behavior.common.constant.RedisKeys;
import org.example.behavior.domain.command.CouponPushCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.params.SetParams;

import java.util.ArrayList;
import java.util.List;

/**
 * Standalone idempotency stage from the README: a Redis SETNX key per
 * idempotent ID. Downstream sinks only receive commands that passed dedup;
 * duplicates are dropped. Failed commands can be routed to a dead-letter sink.
 */
public class DedupSink extends RichSinkFunction<CouponPushCommand> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(DedupSink.class);

    /** 24h TTL, after which the same idempotency ID is no longer meaningful. */
    public static final long DEDUP_TTL_SECONDS = 24 * 60 * 60L;

    private final String host;
    private final int port;
    private final long ttlSeconds;

    private transient JedisPool jedisPool;
    private transient List<CouponPushCommand> localBuffer;

    public DedupSink(String host, int port) {
        this(host, port, DEDUP_TTL_SECONDS);
    }

    public DedupSink(String host, int port, long ttlSeconds) {
        this.host = host;
        this.port = port;
        this.ttlSeconds = ttlSeconds;
    }

    @Override
    public void open(Configuration parameters) {
        jedisPool = new JedisPool(host, port);
        localBuffer = new ArrayList<>();
    }

    /**
     * Atomically claims the idempotency ID. Returns {@code true} for the first
     * occurrence, {@code false} for a duplicate. When Redis is unavailable the
     * command is kept in a local buffer and allowed through with a warning,
     * preferring at-least-once delivery.
     */
    public boolean isFirstOccurrence(CouponPushCommand command) {
        if (command == null || command.getIdempotentId() == null) {
            return false;
        }
        String key = RedisKeys.couponDedup(command.getIdempotentId());
        try (Jedis jedis = jedisPool.getResource()) {
            String result = jedis.set(key, "1",
                    SetParams.setParams().nx().ex(ttlSeconds));
            return "OK".equalsIgnoreCase(result);
        } catch (Exception e) {
            localBuffer.add(command);
            LOG.warn("Redis dedup unavailable; buffered command {} locally",
                    command.getIdempotentId(), e);
            return true;
        }
    }

    /** Commands buffered while Redis was down, for a retry / dead-letter path. */
    public List<CouponPushCommand> drainLocalBuffer() {
        List<CouponPushCommand> drained = new ArrayList<>(localBuffer);
        localBuffer.clear();
        return drained;
    }

    @Override
    public void close() {
        if (jedisPool != null) {
            jedisPool.close();
        }
    }
}
