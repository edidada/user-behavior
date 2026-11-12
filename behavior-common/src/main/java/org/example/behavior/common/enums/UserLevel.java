package org.example.behavior.common.enums;

/**
 * User value level derived from the 24h spending amount.
 */
public enum UserLevel {
    NORMAL(0D),
    SILVER(1_000D),
    GOLD(10_000D),
    PLATINUM(50_000D);

    private final double threshold;

    UserLevel(double threshold) {
        this.threshold = threshold;
    }

    public double getThreshold() {
        return threshold;
    }

    public static UserLevel fromAmount(double amount24h) {
        if (amount24h >= PLATINUM.threshold) {
            return PLATINUM;
        }
        if (amount24h >= GOLD.threshold) {
            return GOLD;
        }
        if (amount24h >= SILVER.threshold) {
            return SILVER;
        }
        return NORMAL;
    }

    public boolean atLeast(UserLevel other) {
        return ordinal() >= other.ordinal();
    }
}
