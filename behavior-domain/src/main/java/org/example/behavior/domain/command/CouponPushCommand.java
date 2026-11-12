package org.example.behavior.domain.command;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.example.behavior.common.enums.CouponType;
import org.example.behavior.common.enums.TriggerChannel;

import java.io.Serializable;

/**
 * Instruction produced by the strategy engine and consumed by the reach-out system.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CouponPushCommand implements Serializable {

    private static final long serialVersionUID = 1L;

    private String userId;

    private String ruleId;

    private String couponId;

    private CouponType couponType;

    /** Why the coupon is pushed, e.g. the detected pattern name. */
    private String triggerReason;

    private TriggerChannel channel;

    /** Stable key used for SETNX based deduplication. */
    private String idempotentId;

    private long createdAt;
}
