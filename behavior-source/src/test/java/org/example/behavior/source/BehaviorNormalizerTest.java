package org.example.behavior.source;

import org.example.behavior.common.enums.BehaviorType;
import org.example.behavior.common.util.EventTimeUtils;
import org.example.behavior.domain.event.UserBehaviorEvent;
import org.example.behavior.source.function.BehaviorNormalizer;
import org.example.behavior.source.protocol.LegacyEventMapper;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class BehaviorNormalizerTest {

    @Test
    void fillsMissingFieldsForValidEvent() {
        long now = EventTimeUtils.now();
        UserBehaviorEvent event = UserBehaviorEvent.builder()
                .userId("  u1  ")
                .behaviorType(BehaviorType.CART)
                .skuId("sku-1")
                .timestamp(now)
                .build();

        assertNull(BehaviorNormalizer.normalizeAndValidate(event));
        assertEquals("u1", event.getUserId());
        assertEquals("unknown", event.getChannel());
        assertNotNull(event.getSessionId());
        assertNotNull(event.getEventId());
    }

    @Test
    void rejectsEventWithoutUserId() {
        UserBehaviorEvent event = UserBehaviorEvent.builder()
                .behaviorType(BehaviorType.VIEW)
                .skuId("sku-1")
                .timestamp(EventTimeUtils.now())
                .build();

        assertEquals("missing userId", BehaviorNormalizer.normalizeAndValidate(event));
    }

    @Test
    void rejectsUnreasonableTimestamp() {
        UserBehaviorEvent event = UserBehaviorEvent.builder()
                .userId("u1")
                .behaviorType(BehaviorType.VIEW)
                .skuId("sku-1")
                .timestamp(1L)
                .build();

        assertNotNull(BehaviorNormalizer.normalizeAndValidate(event));
    }

    @Test
    void mapsV1Payload() {
        Map<String, Object> v1 = Map.of(
                "uid", "u1",
                "item_id", "sku-9",
                "event", "pay",
                "ts", 1_700_000_000_000L,
                "session", "s1",
                "src", "app",
                "amount", "99.5");

        UserBehaviorEvent event = LegacyEventMapper.fromV1(v1);

        assertEquals("u1", event.getUserId());
        assertEquals("sku-9", event.getSkuId());
        assertEquals(BehaviorType.PAY, event.getBehaviorType());
        assertEquals(new BigDecimal("99.5"), event.getAmount());
    }

    @Test
    void mapsV2Payload() {
        Map<String, Object> v2 = Map.of(
                "userId", "u2",
                "skuId", "sku-2",
                "behaviorType", "ORDER",
                "timestamp", 1_700_000_001_000L);

        UserBehaviorEvent event = LegacyEventMapper.fromV2(v2);

        assertEquals("u2", event.getUserId());
        assertEquals(BehaviorType.ORDER, event.getBehaviorType());
        assertEquals(1_700_000_001_000L, event.getTimestamp());
    }
}
