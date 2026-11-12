package org.example.behavior.strategy;

import org.example.behavior.common.constant.CepPatterns;
import org.example.behavior.common.enums.CouponType;
import org.example.behavior.common.enums.UserLevel;
import org.example.behavior.domain.command.CouponPushCommand;
import org.example.behavior.domain.result.PatternMatchResult;
import org.example.behavior.domain.rule.MarketingRule;
import org.example.behavior.strategy.cooldown.InMemoryCooldownChecker;
import org.example.behavior.strategy.coupon.DefaultCouponSelector;
import org.example.behavior.strategy.matcher.PatternRuleMatcher;
import org.example.behavior.strategy.repository.InMemoryRuleRepository;
import org.example.behavior.strategy.service.StrategyDecisionService;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StrategyDecisionServiceTest {

    @Test
    void emitsCommandForMatchingPattern() {
        StrategyDecisionService service = newService(rule(3600));

        Optional<CouponPushCommand> command = service.decide(match(CepPatterns.CART_NO_PAY_TIMEOUT));

        assertTrue(command.isPresent());
        assertEquals("CPN_CART", command.get().getCouponId());
        assertEquals("rule-cart", command.get().getRuleId());
        assertEquals(CepPatterns.CART_NO_PAY_TIMEOUT, command.get().getTriggerReason());
        assertEquals(CouponType.CASH, command.get().getCouponType());
        assertEquals(32, command.get().getIdempotentId().length());
    }

    @Test
    void cooldownBlocksSecondPush() {
        StrategyDecisionService service = newService(rule(3600));
        PatternMatchResult match = match(CepPatterns.CART_NO_PAY_TIMEOUT);

        assertTrue(service.decide(match).isPresent());
        assertTrue(service.decide(match).isEmpty());
    }

    @Test
    void noCommandWhenNoRuleMatches() {
        StrategyDecisionService service = newService(rule(3600));

        assertTrue(service.decide(match(CepPatterns.VIEW_CART_ORDER)).isEmpty());
    }

    @Test
    void decidesAgainstAnExplicitRuleSnapshot() {
        StrategyDecisionService service = newService(rule(60));
        MarketingRule other = rule(60);
        other.setPatternName(CepPatterns.VIEW_CART_ORDER);

        assertTrue(service.decide(match(CepPatterns.CART_NO_PAY_TIMEOUT),
                List.of(other)).isEmpty());
        assertTrue(service.decide(match(CepPatterns.CART_NO_PAY_TIMEOUT),
                List.of(other, rule(60))).isPresent());
    }

    @Test
    void noCommandWhenCouponTemplateMissing() {
        MarketingRule broken = MarketingRule.builder()
                .id("broken")
                .patternName(CepPatterns.CART_NO_PAY_TIMEOUT)
                .couponTemplateId(" ")
                .enabled(true)
                .cooldownSeconds(0)
                .audienceLevels(Set.of())
                .build();
        StrategyDecisionService service = newService(broken);

        assertTrue(service.decide(match(CepPatterns.CART_NO_PAY_TIMEOUT)).isEmpty());
    }

    private static StrategyDecisionService newService(MarketingRule rule) {
        return new StrategyDecisionService(
                new InMemoryRuleRepository(List.of(rule)),
                new PatternRuleMatcher(),
                new DefaultCouponSelector(),
                new InMemoryCooldownChecker());
    }

    private static MarketingRule rule(long cooldownSeconds) {
        return MarketingRule.builder()
                .id("rule-cart")
                .name("cart abandon")
                .patternName(CepPatterns.CART_NO_PAY_TIMEOUT)
                .couponTemplateId("CPN_CART")
                .couponType(CouponType.CASH)
                .cooldownSeconds(cooldownSeconds)
                .audienceLevels(Set.of())
                .enabled(true)
                .priority(100)
                .build();
    }

    private static PatternMatchResult match(String pattern) {
        return PatternMatchResult.builder()
                .userId("u1")
                .patternName(pattern)
                .matchedAt(1_000L)
                .userLevel(UserLevel.NORMAL)
                .build();
    }
}
