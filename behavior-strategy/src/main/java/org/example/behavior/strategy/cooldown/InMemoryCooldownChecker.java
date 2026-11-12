package org.example.behavior.strategy.cooldown;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Process-local cooldown checker. Suitable for unit tests and LocalStreamEnvironment
 * runs; use a Redis backed implementation in production.
 */
public class InMemoryCooldownChecker implements CooldownChecker {

    private static final long serialVersionUID = 1L;

    private final ConcurrentHashMap<String, Long> lastPushAt = new ConcurrentHashMap<>();

    @Override
    public boolean tryAcquire(String userId, String ruleId, long cooldownSeconds) {
        if (cooldownSeconds <= 0) {
            return true;
        }
        String key = userId + ":" + ruleId;
        long now = System.currentTimeMillis();
        long cooldownMillis = cooldownSeconds * 1000L;
        AtomicBoolean allowed = new AtomicBoolean(false);
        lastPushAt.compute(key, (ignored, last) -> {
            if (last == null || now - last >= cooldownMillis) {
                allowed.set(true);
                return now;
            }
            return last;
        });
        return allowed.get();
    }
}
