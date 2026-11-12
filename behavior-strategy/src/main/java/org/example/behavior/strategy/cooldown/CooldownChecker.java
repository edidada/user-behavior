package org.example.behavior.strategy.cooldown;

import java.io.Serializable;

/**
 * Prevents over-contacting a user. The production implementation is backed by
 * Redis SETNX, the default in-memory one is used for tests and local runs.
 */
public interface CooldownChecker extends Serializable {

    /**
     * @return {@code true} when the push is allowed and the cooldown window is started.
     */
    boolean tryAcquire(String userId, String ruleId, long cooldownSeconds);
}
