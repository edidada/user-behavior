package org.example.behavior.domain.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.example.behavior.common.enums.BehaviorType;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * A single normalized user behavior event coming from the tracking SDK.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UserBehaviorEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    private String eventId;

    private String userId;

    private String skuId;

    private BehaviorType behaviorType;

    /** Event time in epoch millis. */
    private long timestamp;

    private String sessionId;

    private String channel;

    /** Order/payment amount, only meaningful for ORDER and PAY. */
    private BigDecimal amount;
}
