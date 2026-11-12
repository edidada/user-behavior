package org.example.behavior.source.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

/**
 * Kafka source for marketing rule updates. Rules are JSON documents that the
 * app parses into {@code MarketingRule} and broadcasts to the decision
 * operators, so rule changes take effect without a job restart.
 */
public final class RuleKafkaSource {

    private RuleKafkaSource() {
    }

    public static KafkaSource<String> build(String bootstrapServers,
                                            String topic,
                                            String groupId) {
        return KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("commit.offsets.on.checkpoint", "true")
                .build();
    }
}
