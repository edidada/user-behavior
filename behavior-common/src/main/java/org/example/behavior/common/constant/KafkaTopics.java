package org.example.behavior.common.constant;

/**
 * Kafka topic names used by the behavior pipeline.
 */
public final class KafkaTopics {

    public static final String BEHAVIOR_EVENTS = "user-behavior-events";

    public static final String DIRTY_BEHAVIOR_EVENTS = "user-behavior-dirty";

    public static final String COUPON_PUSH_COMMANDS = "user-coupon-push-commands";

    private KafkaTopics() {
    }
}
