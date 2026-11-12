package org.example.behavior.strategy;

import org.example.behavior.strategy.cooldown.InMemoryCooldownChecker;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InMemoryCooldownCheckerTest {

    @Test
    void allowsFirstPushAndBlocksTheNext() {
        InMemoryCooldownChecker checker = new InMemoryCooldownChecker();

        assertTrue(checker.tryAcquire("u1", "rule-1", 60));
        assertFalse(checker.tryAcquire("u1", "rule-1", 60));
    }

    @Test
    void differentRulesHaveIndependentWindows() {
        InMemoryCooldownChecker checker = new InMemoryCooldownChecker();

        assertTrue(checker.tryAcquire("u1", "rule-1", 60));
        assertTrue(checker.tryAcquire("u1", "rule-2", 60));
    }

    @Test
    void zeroCooldownAlwaysAllows() {
        InMemoryCooldownChecker checker = new InMemoryCooldownChecker();

        assertTrue(checker.tryAcquire("u1", "rule-1", 0));
        assertTrue(checker.tryAcquire("u1", "rule-1", 0));
    }
}
