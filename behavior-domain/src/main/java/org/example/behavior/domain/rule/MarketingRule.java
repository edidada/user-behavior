package org.example.behavior.domain.rule;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.example.behavior.common.enums.CouponType;
import org.example.behavior.common.enums.UserLevel;

import java.io.Serializable;
import java.util.Set;

/**
 * A configurable marketing rule. Rules are matched against detected behavior patterns.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MarketingRule implements Serializable {

    private static final long serialVersionUID = 1L;

    private String id;

    private String name;

    /** Pattern name, see {@code CepPatterns}. */
    private String patternName;

    private String couponTemplateId;

    private CouponType couponType;

    /** Minimum interval between two pushes of this rule for the same user. */
    private long cooldownSeconds;

    /**
     * Targeted user levels. {@code null} or empty means the rule applies to
     * every user level.
     */
    private Set<UserLevel> audienceLevels;

    private boolean enabled;

    /** Higher value wins when several rules match the same event. */
    private int priority;
}
