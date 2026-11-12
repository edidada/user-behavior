package org.example.behavior.common.util;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * Builds stable, compact idempotency keys for coupon pushes.
 */
public final class IdempotentKeyGenerator {

    private IdempotentKeyGenerator() {
    }

    public static String of(String userId, String ruleId, long eventTimestamp) {
        return hash(userId + "|" + ruleId + "|" + eventTimestamp);
    }

    public static String hash(String raw) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] bytes = digest.digest(raw.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(bytes, 0, 16);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available", e);
        }
    }
}
