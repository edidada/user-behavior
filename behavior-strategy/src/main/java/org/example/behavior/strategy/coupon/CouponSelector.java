package org.example.behavior.strategy.coupon;

import org.example.behavior.common.enums.UserLevel;
import org.example.behavior.domain.rule.MarketingRule;

import java.io.Serializable;
import java.util.Optional;

/**
 * Selects the concrete coupon to push for a matched rule. Kept as an interface
 * so inventory/budget checks can be plugged in without touching the job.
 */
public interface CouponSelector extends Serializable {

    Optional<String> selectCoupon(MarketingRule rule, UserLevel userLevel);
}
