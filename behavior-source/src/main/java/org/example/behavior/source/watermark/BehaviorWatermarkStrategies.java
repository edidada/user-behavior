package org.example.behavior.source.watermark;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.example.behavior.domain.event.UserBehaviorEvent;

import java.time.Duration;

/**
 * Watermark strategies for the behavior stream: bounded out-of-orderness plus
 * idleness so quiet partitions do not hold the watermark back.
 */
public final class BehaviorWatermarkStrategies {

    private BehaviorWatermarkStrategies() {
    }

    public static WatermarkStrategy<UserBehaviorEvent> defaults() {
        return boundedOutOfOrder(Duration.ofSeconds(5), Duration.ofMinutes(1));
    }

    public static WatermarkStrategy<UserBehaviorEvent> boundedOutOfOrder(Duration maxOutOfOrder,
                                                                         Duration idleTimeout) {
        return WatermarkStrategy.<UserBehaviorEvent>forBoundedOutOfOrderness(maxOutOfOrder)
                .withTimestampAssigner((event, recordTimestamp) -> event.getTimestamp())
                .withIdleness(idleTimeout);
    }
}
