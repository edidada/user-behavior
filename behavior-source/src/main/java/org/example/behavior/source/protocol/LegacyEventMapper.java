package org.example.behavior.source.protocol;

import org.example.behavior.common.enums.BehaviorType;
import org.example.behavior.common.util.JsonUtils;
import org.example.behavior.domain.event.UserBehaviorEvent;

import java.math.BigDecimal;
import java.util.Map;

/**
 * Maps tracking payloads from the legacy v1 protocol and the current v2 protocol
 * onto the unified {@link UserBehaviorEvent}.
 *
 * <pre>
 * v1: {"uid","item_id","event","ts","session","src"}
 * v2: {"userId","skuId","behaviorType","timestamp","sessionId","channel","amount"}
 * </pre>
 */
public final class LegacyEventMapper {

    private LegacyEventMapper() {
    }

    public static UserBehaviorEvent fromV1Json(String json) {
        return fromV1(JsonUtils.toMap(json));
    }

    public static UserBehaviorEvent fromV1(Map<String, Object> raw) {
        if (raw == null) {
            return null;
        }
        return UserBehaviorEvent.builder()
                .userId(asString(raw.get("uid")))
                .skuId(asString(raw.get("item_id")))
                .behaviorType(BehaviorType.fromCode(asString(raw.get("event"))))
                .timestamp(asLong(raw.get("ts")))
                .sessionId(asString(raw.get("session")))
                .channel(asString(raw.get("src")))
                .amount(asDecimal(raw.get("amount")))
                .build();
    }

    public static UserBehaviorEvent fromV2(Map<String, Object> raw) {
        if (raw == null) {
            return null;
        }
        return UserBehaviorEvent.builder()
                .eventId(asString(raw.get("eventId")))
                .userId(asString(raw.get("userId")))
                .skuId(asString(raw.get("skuId")))
                .behaviorType(BehaviorType.fromCode(asString(raw.get("behaviorType"))))
                .timestamp(asLong(raw.get("timestamp")))
                .sessionId(asString(raw.get("sessionId")))
                .channel(asString(raw.get("channel")))
                .amount(asDecimal(raw.get("amount")))
                .build();
    }

    private static String asString(Object value) {
        return value == null ? null : String.valueOf(value);
    }

    private static long asLong(Object value) {
        if (value instanceof Number number) {
            return number.longValue();
        }
        if (value == null) {
            return 0L;
        }
        try {
            return Long.parseLong(String.valueOf(value).trim());
        } catch (NumberFormatException e) {
            return 0L;
        }
    }

    private static BigDecimal asDecimal(Object value) {
        if (value instanceof BigDecimal decimal) {
            return decimal;
        }
        if (value == null) {
            return null;
        }
        try {
            return new BigDecimal(String.valueOf(value).trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
