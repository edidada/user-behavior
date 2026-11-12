package org.example.behavior.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.example.behavior.common.constant.RedisKeys;
import org.example.behavior.domain.profile.UserValueProfile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Writes user value profiles to Redis. A hash holds the profile fields and a
 * zset keeps the high value user ranking for the front end.
 *
 * <p>The 1h and 24h window fragments are written to the same hash; only the
 * fields belonging to the fragment's granularity are updated.
 */
public class RedisProfileSink extends RichSinkFunction<UserValueProfile> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(RedisProfileSink.class);

    private final String host;

    private final int port;

    private transient JedisPool jedisPool;

    public RedisProfileSink(String host, int port) {
        this.host = host;
        this.port = port;
    }

    @Override
    public void open(Configuration parameters) {
        jedisPool = new JedisPool(host, port);
    }

    @Override
    public void invoke(UserValueProfile profile, Context context) {
        if (profile == null || profile.getUserId() == null) {
            return;
        }
        String key = RedisKeys.profile(profile.getUserId());
        Map<String, String> fields = toHash(profile);
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.hset(key, fields);
            if ("24h".equals(profile.getGranularity())) {
                jedis.zadd(RedisKeys.HIGH_VALUE_RANK, profile.getAmount24h(), profile.getUserId());
            }
        } catch (Exception e) {
            LOG.error("Failed to write profile for user {} to Redis", profile.getUserId(), e);
        }
    }

    @Override
    public void close() {
        if (jedisPool != null) {
            jedisPool.close();
        }
    }

    private static Map<String, String> toHash(UserValueProfile profile) {
        Map<String, String> fields = new LinkedHashMap<>();
        fields.put("userId", profile.getUserId());
        fields.put("windowEnd", String.valueOf(profile.getWindowEnd()));
        fields.put("updatedAt", String.valueOf(profile.getUpdatedAt()));

        if ("1h".equals(profile.getGranularity())) {
            fields.put("amount1h", String.valueOf(profile.getAmount1h()));
        } else {
            fields.put("amount24h", String.valueOf(profile.getAmount24h()));
            fields.put("orderCount24h", String.valueOf(profile.getOrderCount24h()));
            fields.put("payCount24h", String.valueOf(profile.getPayCount24h()));
            fields.put("avgOrderAmount", String.valueOf(profile.getAvgOrderAmount()));
            if (profile.getUserLevel() != null) {
                fields.put("userLevel", profile.getUserLevel().name());
            }
        }
        return fields;
    }
}
