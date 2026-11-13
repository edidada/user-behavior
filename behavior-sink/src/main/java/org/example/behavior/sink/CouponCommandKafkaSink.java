package org.example.behavior.sink;

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.example.behavior.common.util.JsonUtils;
import org.example.behavior.domain.command.CouponPushCommand;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;

/**
 * Coupon reach-out via Kafka instead of an HTTP API: emits the command to the
 * reach-out system's topic. KafkaSink with AT_LEAST_ONCE plus the command's
 * idempotent ID lets the downstream reach-out system deduplicate on replay.
 */
public final class CouponCommandKafkaSink {

    private CouponCommandKafkaSink() {
    }

    public static KafkaSink<CouponPushCommand> build(String bootstrapServers, String topic) {
        return KafkaSink.<CouponPushCommand>builder()
                .setBootstrapServers(bootstrapServers)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setRecordSerializer(new CommandSerializer(topic))
                .build();
    }

    /** Attaches the Kafka sink to the command stream. */
    public static void attach(DataStream<CouponPushCommand> commands,
                              String bootstrapServers,
                              String topic) {
        commands.sinkTo(build(bootstrapServers, topic))
                .name("coupon-command-kafka-sink")
                .uid("coupon-command-kafka-sink");
    }

    private static class CommandSerializer
            implements KafkaRecordSerializationSchema<CouponPushCommand> {

        private static final long serialVersionUID = 1L;

        private final String topic;

        private CommandSerializer(String topic) {
            this.topic = topic;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(CouponPushCommand element,
                                                        KafkaSinkContext context,
                                                        Long timestamp) {
            byte[] key = element.getUserId().getBytes(StandardCharsets.UTF_8);
            byte[] value = JsonUtils.toJson(element).getBytes(StandardCharsets.UTF_8);
            return new ProducerRecord<>(topic, key, value);
        }
    }
}
