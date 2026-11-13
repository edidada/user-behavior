package org.example.behavior.strategy.cooldown;

import org.example.behavior.common.constant.RedisKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.params.SetParams;

/**
 * Redis SETNX based cooldown checker, called by the strategy engine in
 * production. A single atomic {@code SET key value NX EX seconds} both tests
 * and starts the cooldown window, so two parallel subtasks cannot both push a
 * coupon to the same user for the same rule.
 */
public class RedisCooldownChecker implements CooldownChecker {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(RedisCooldownChecker.class);

    private final String host;
    private final int port;

    private transient JedisPool jedisPool;

    public RedisCooldownChecker(String host, int port) {
        this.host = host;
        this.port = port;
    }

    @Override
    public boolean tryAcquire(String userId, String ruleId, long cooldownSeconds) {
        if (cooldownSeconds <= 0) {
            return true;
        }
        if (jedisPool == null) {
            open();
        }
        String key = RedisKeys.cooldown(userId, ruleId);
        try (Jedis jedis = jedisPool.getResource()) {
            String result = jedis.set(key, "1",
                    SetParams.setParams().nx().ex(cooldownSeconds));
            return "OK".equalsIgnoreCase(result);
        } catch (Exception e) {
            // Degrade to allowing the push rather than losing the marketing
            // opportunity; the coupon service remains the final idempotency gate.
            LOG.warn("Redis cooldown unavailable for key {}", key, e);
            return true;
        }
    }

    private synchronized void open() {
        if (jedisPool == null) {
            jedisPool = new JedisPool(host, port);
        }
    }

    public void close() {
        if (jedisPool != null) {
            jedisPool.close();
            jedisPool = null;
        }
    }
}
