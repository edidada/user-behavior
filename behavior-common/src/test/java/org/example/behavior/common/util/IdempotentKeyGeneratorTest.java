package org.example.behavior.common.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class IdempotentKeyGeneratorTest {

    @Test
    void producesStableKeyForSameInput() {
        assertEquals(
                IdempotentKeyGenerator.of("u1", "rule-1", 1000L),
                IdempotentKeyGenerator.of("u1", "rule-1", 1000L));
    }

    @Test
    void producesDifferentKeysForDifferentInput() {
        assertNotEquals(
                IdempotentKeyGenerator.of("u1", "rule-1", 1000L),
                IdempotentKeyGenerator.of("u1", "rule-1", 1001L));
    }

    @Test
    void keyIsCompact() {
        String key = IdempotentKeyGenerator.of("user-123", "rule-cart", 123456789L);

        assertEquals(32, key.length());
    }
}
