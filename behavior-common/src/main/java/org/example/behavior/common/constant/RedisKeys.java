package org.example.behavior.common.constant;

/**
 * Redis key layout.
 *
 * <ul>
 *   <li>{@code user:behavior:profile:{userId}} - hash with the real-time user profile.</li>
 *   <li>{@code user:highvalue:rank} - zset used by the front end for paging high value users.</li>
 *   <li>{@code user:coupon:push:{idempotentId}} - SETNX key used to deduplicate coupon pushes.</li>
 *   <li>{@code user:coupon:cooldown:{userId}:{ruleId}} - SETNX key used to implement cooldown.</li>
 * </ul>
 */
public final class RedisKeys {

    public static final String PREFIX = "user:";

    public static final String BEHAVIOR_PREFIX = PREFIX + "behavior:";

    public static final String COUPON_PREFIX = PREFIX + "coupon:";

    public static final String PROFILE_PREFIX = BEHAVIOR_PREFIX + "profile:";

    public static final String HIGH_VALUE_RANK = PREFIX + "highvalue:rank";

    public static final String COUPON_DEDUP_PREFIX = COUPON_PREFIX + "push:";

    public static final String COOLDOWN_PREFIX = COUPON_PREFIX + "cooldown:";

    private RedisKeys() {
    }

    public static String profile(String userId) {
        return PROFILE_PREFIX + userId;
    }

    public static String couponDedup(String idempotentId) {
        return COUPON_DEDUP_PREFIX + idempotentId;
    }

    public static String cooldown(String userId, String ruleId) {
        return COOLDOWN_PREFIX + userId + ":" + ruleId;
    }
}
