package org.example.behavior.source.deserialization;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.example.behavior.domain.event.UserBehaviorEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Deserializes Kafka JSON payloads into {@link UserBehaviorEvent}. Malformed
 * records are logged and skipped so a single bad message cannot stop the source;
 * field-level validation is handled later by the normalizer.
 */
public class UserBehaviorEventDeserializationSchema
        implements KafkaRecordDeserializationSchema<UserBehaviorEvent> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(UserBehaviorEventDeserializationSchema.class);

    private final com.fasterxml.jackson.databind.ObjectMapper mapper =
            new com.fasterxml.jackson.databind.ObjectMapper()
                    .configure(com.fasterxml.jackson.databind.DeserializationFeature
                            .FAIL_ON_UNKNOWN_PROPERTIES, false);

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        // The mapper is initialized eagerly; nothing to open.
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<UserBehaviorEvent> out)
            throws IOException {
        byte[] value = record.value();
        if (value == null || value.length == 0) {
            return;
        }
        try {
            UserBehaviorEvent event = mapper.readValue(value, UserBehaviorEvent.class);
            if (event != null) {
                out.collect(event);
            }
        } catch (Exception e) {
            LOG.warn("Skipping malformed behavior event at topic={} partition={} offset={}",
                    record.topic(), record.partition(), record.offset(), e);
        }
    }

    @Override
    public TypeInformation<UserBehaviorEvent> getProducedType() {
        return TypeInformation.of(UserBehaviorEvent.class);
    }
}
