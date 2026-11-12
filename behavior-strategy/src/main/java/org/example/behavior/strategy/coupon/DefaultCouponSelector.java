package org.example.behavior.strategy.coupon;

import org.example.behavior.common.enums.UserLevel;
import org.example.behavior.domain.rule.MarketingRule;

import java.util.Optional;

/**
 * Selects the coupon template configured on the rule. A production
 * implementation would additionally check coupon stock and campaign budget.
 */
public class DefaultCouponSelector implements CouponSelector {

    private static final long serialVersionUID = 1L;

    @Override
    public Optional<String> selectCoupon(MarketingRule rule, UserLevel userLevel) {
        if (rule == null || rule.getCouponTemplateId() == null || rule.getCouponTemplateId().isBlank()) {
            return Optional.empty();
        }
        return Optional.of(rule.getCouponTemplateId());
    }
}
