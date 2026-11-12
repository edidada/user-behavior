package org.example.behavior.source.source;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.example.behavior.domain.event.UserBehaviorEvent;
import org.example.behavior.source.deserialization.UserBehaviorEventDeserializationSchema;

/**
 * Builds the Kafka source for raw behavior events.
 */
public final class BehaviorKafkaSource {

    private BehaviorKafkaSource() {
    }

    public static KafkaSource<UserBehaviorEvent> build(String bootstrapServers,
                                                       String topic,
                                                       String groupId) {
        return KafkaSource.<UserBehaviorEvent>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setDeserializer(new UserBehaviorEventDeserializationSchema())
                .setProperty("commit.offsets.on.checkpoint", "true")
                .build();
    }
}
