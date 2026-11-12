package org.example.behavior.common.util;

import java.time.Instant;

/**
 * Helpers for validating and normalizing event timestamps.
 */
public final class EventTimeUtils {

    public static final long DEFAULT_MAX_FUTURE_SKEW_MILLIS = 5 * 60 * 1000L;

    public static final long DEFAULT_MAX_PAST_AGE_MILLIS = 7 * 24 * 60 * 60 * 1000L;

    private EventTimeUtils() {
    }

    public static long now() {
        return System.currentTimeMillis();
    }

    /**
     * An event timestamp is usable when it is positive, not too far in the future
     * (clock skew) and not older than the retention window.
     */
    public static boolean isReasonable(long timestamp) {
        return isReasonable(timestamp, DEFAULT_MAX_FUTURE_SKEW_MILLIS, DEFAULT_MAX_PAST_AGE_MILLIS);
    }

    public static boolean isReasonable(long timestamp, long maxFutureSkewMillis, long maxPastAgeMillis) {
        if (timestamp <= 0) {
            return false;
        }
        long now = now();
        return timestamp <= now + maxFutureSkewMillis && timestamp >= now - maxPastAgeMillis;
    }

    public static Instant toInstant(long timestamp) {
        return Instant.ofEpochMilli(timestamp);
    }
}
