package org.example.behavior.domain.profile;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.example.behavior.common.enums.UserLevel;

import java.io.Serializable;

/**
 * Real-time user value profile written to Redis and served to the front end.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UserValueProfile implements Serializable {

    private static final long serialVersionUID = 1L;

    private String userId;

    /** {@code "1h"} or {@code "24h"}; tells the sink which fields to write. */
    private String granularity;

    /** Spending in the last hour, updated by the 1h sliding window. */
    private double amount1h;

    /** Spending in the last 24 hours, updated by the 24h sliding window. */
    private double amount24h;

    private long orderCount24h;

    private long payCount24h;

    private double avgOrderAmount;

    private UserLevel userLevel;

    /** End of the window that produced this profile, epoch millis. */
    private long windowEnd;

    private long updatedAt;
}
