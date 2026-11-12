package org.example.behavior.strategy.service;

import org.example.behavior.common.enums.TriggerChannel;
import org.example.behavior.common.util.IdempotentKeyGenerator;
import org.example.behavior.domain.command.CouponPushCommand;
import org.example.behavior.domain.result.PatternMatchResult;
import org.example.behavior.domain.rule.MarketingRule;
import org.example.behavior.strategy.cooldown.CooldownChecker;
import org.example.behavior.strategy.coupon.CouponSelector;
import org.example.behavior.strategy.matcher.PatternRuleMatcher;
import org.example.behavior.strategy.repository.RuleRepository;

import java.io.Serializable;
import java.util.Optional;

/**
 * Orchestrates rule matching, coupon selection and cooldown checking. The job
 * only depends on this class, so marketing rules can evolve without touching
 * the Flink topology. Pure Java, fully unit testable.
 */
public class StrategyDecisionService implements Serializable {

    private static final long serialVersionUID = 1L;

    private final RuleRepository ruleRepository;
    private final PatternRuleMatcher patternRuleMatcher;
    private final CouponSelector couponSelector;
    private final CooldownChecker cooldownChecker;
    private final TriggerChannel defaultChannel;

    public StrategyDecisionService(RuleRepository ruleRepository,
                                   PatternRuleMatcher patternRuleMatcher,
                                   CouponSelector couponSelector,
                                   CooldownChecker cooldownChecker) {
        this(ruleRepository, patternRuleMatcher, couponSelector, cooldownChecker, TriggerChannel.APP_PUSH);
    }

    public StrategyDecisionService(RuleRepository ruleRepository,
                                   PatternRuleMatcher patternRuleMatcher,
                                   CouponSelector couponSelector,
                                   CooldownChecker cooldownChecker,
                                   TriggerChannel defaultChannel) {
        this.ruleRepository = ruleRepository;
        this.patternRuleMatcher = patternRuleMatcher;
        this.couponSelector = couponSelector;
        this.cooldownChecker = cooldownChecker;
        this.defaultChannel = defaultChannel == null ? TriggerChannel.APP_PUSH : defaultChannel;
    }

    public Optional<CouponPushCommand> decide(PatternMatchResult match) {
        if (match == null || match.getUserId() == null || match.getUserId().isBlank()) {
            return Optional.empty();
        }

        Optional<MarketingRule> matchedRule =
                patternRuleMatcher.match(match, ruleRepository.findAll());
        if (matchedRule.isEmpty()) {
            return Optional.empty();
        }

        MarketingRule rule = matchedRule.get();
        Optional<String> couponId = couponSelector.selectCoupon(rule, match.getUserLevel());
        if (couponId.isEmpty()) {
            return Optional.empty();
        }

        if (!cooldownChecker.tryAcquire(match.getUserId(), rule.getId(), rule.getCooldownSeconds())) {
            return Optional.empty();
        }

        return Optional.of(CouponPushCommand.builder()
                .userId(match.getUserId())
                .ruleId(rule.getId())
                .couponId(couponId.get())
                .couponType(rule.getCouponType())
                .triggerReason(match.getPatternName())
                .channel(defaultChannel)
                .idempotentId(IdempotentKeyGenerator.of(
                        match.getUserId(), rule.getId(), match.getMatchedAt()))
                .createdAt(System.currentTimeMillis())
                .build());
    }
}
