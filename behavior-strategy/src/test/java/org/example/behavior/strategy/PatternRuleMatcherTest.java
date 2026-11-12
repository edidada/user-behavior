package org.example.behavior.strategy;

import org.example.behavior.common.constant.CepPatterns;
import org.example.behavior.common.enums.UserLevel;
import org.example.behavior.domain.result.PatternMatchResult;
import org.example.behavior.domain.rule.MarketingRule;
import org.example.behavior.strategy.matcher.PatternRuleMatcher;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PatternRuleMatcherTest {

    private final PatternRuleMatcher matcher = new PatternRuleMatcher();

    @Test
    void picksHighestPriorityEnabledRule() {
        MarketingRule low = rule("low", CepPatterns.CART_NO_PAY_TIMEOUT, 10, true, Set.of());
        MarketingRule high = rule("high", CepPatterns.CART_NO_PAY_TIMEOUT, 20, true, Set.of());

        Optional<MarketingRule> matched = matcher.match(match(CepPatterns.CART_NO_PAY_TIMEOUT,
                UserLevel.NORMAL), List.of(low, high));

        assertTrue(matched.isPresent());
        assertEquals("high", matched.get().getId());
    }

    @Test
    void skipsDisabledRules() {
        MarketingRule disabled = rule("disabled", CepPatterns.CART_NO_PAY_TIMEOUT, 100, false, Set.of());

        Optional<MarketingRule> matched = matcher.match(match(CepPatterns.CART_NO_PAY_TIMEOUT,
                UserLevel.NORMAL), List.of(disabled));

        assertTrue(matched.isEmpty());
    }

    @Test
    void respectsAudience() {
        MarketingRule goldOnly = rule("gold-only", CepPatterns.CART_NO_PAY_TIMEOUT, 10, true,
                Set.of(UserLevel.GOLD));

        assertTrue(matcher.match(match(CepPatterns.CART_NO_PAY_TIMEOUT, UserLevel.NORMAL),
                List.of(goldOnly)).isEmpty());
        assertTrue(matcher.match(match(CepPatterns.CART_NO_PAY_TIMEOUT, UserLevel.GOLD),
                List.of(goldOnly)).isPresent());
    }

    @Test
    void wildcardMatchesAnyPattern() {
        MarketingRule any = rule("any", PatternRuleMatcher.ANY_PATTERN, 10, true, Set.of());

        assertTrue(matcher.match(match(CepPatterns.VIEW_CART_ORDER, UserLevel.NORMAL),
                List.of(any)).isPresent());
    }

    private static PatternMatchResult match(String pattern, UserLevel level) {
        return PatternMatchResult.builder()
                .userId("u1")
                .patternName(pattern)
                .matchedAt(1_000L)
                .userLevel(level)
                .build();
    }

    private static MarketingRule rule(String id,
                                      String pattern,
                                      int priority,
                                      boolean enabled,
                                      Set<UserLevel> audience) {
        return MarketingRule.builder()
                .id(id)
                .name(id)
                .patternName(pattern)
                .couponTemplateId("CPN_" + id)
                .cooldownSeconds(60)
                .audienceLevels(audience)
                .enabled(enabled)
                .priority(priority)
                .build();
    }
}
