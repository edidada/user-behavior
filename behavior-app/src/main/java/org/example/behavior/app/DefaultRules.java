package org.example.behavior.app;

import org.example.behavior.common.constant.CepPatterns;
import org.example.behavior.common.enums.CouponType;
import org.example.behavior.domain.rule.MarketingRule;

import java.util.List;
import java.util.Set;

/**
 * Bootstrap rules for local runs. In production the same {@code MarketingRule}
 * objects are refreshed from a configuration center or a broadcast stream.
 */
public final class DefaultRules {

    private DefaultRules() {
    }

    public static List<MarketingRule> initialRules() {
        return List.of(
                MarketingRule.builder()
                        .id("rule-cart-abandon")
                        .name("购物车未支付挽回")
                        .patternName(CepPatterns.CART_NO_PAY_TIMEOUT)
                        .couponTemplateId("CPN_CART_10")
                        .couponType(CouponType.CASH)
                        .cooldownSeconds(3600)
                        .audienceLevels(Set.of())
                        .enabled(true)
                        .priority(100)
                        .build(),
                MarketingRule.builder()
                        .id("rule-view-cart-order")
                        .name("下单后复购激励")
                        .patternName(CepPatterns.VIEW_CART_ORDER)
                        .couponTemplateId("CPN_ORDER_20")
                        .couponType(CouponType.DISCOUNT)
                        .cooldownSeconds(7200)
                        .audienceLevels(Set.of())
                        .enabled(true)
                        .priority(90)
                        .build());
    }
}
