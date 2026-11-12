package org.example.behavior.common.util;

/**
 * Masks user identifiers before they are written into logs or external systems.
 */
public final class UserIdMasker {

    private static final String MASK = "****";

    private UserIdMasker() {
    }

    public static String mask(String userId) {
        if (userId == null || userId.isBlank()) {
            return MASK;
        }
        String trimmed = userId.trim();
        if (trimmed.length() <= 4) {
            return MASK;
        }
        int prefix = Math.min(3, (trimmed.length() - 1) / 2);
        int suffix = Math.min(3, trimmed.length() - prefix - 1);
        return trimmed.substring(0, prefix)
                + MASK
                + trimmed.substring(trimmed.length() - suffix);
    }
}
