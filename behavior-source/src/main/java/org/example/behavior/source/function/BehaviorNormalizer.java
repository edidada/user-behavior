package org.example.behavior.source.function;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.example.behavior.common.enums.BehaviorType;
import org.example.behavior.common.util.EventTimeUtils;
import org.example.behavior.common.util.IdempotentKeyGenerator;
import org.example.behavior.common.util.JsonUtils;
import org.example.behavior.domain.event.UserBehaviorEvent;

import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * Fills in missing fields, corrects timestamps and filters illegal events.
 * Dirty events are emitted through a side output for downstream monitoring.
 */
public class BehaviorNormalizer extends ProcessFunction<UserBehaviorEvent, UserBehaviorEvent> {

    private static final long serialVersionUID = 1L;

    public static final OutputTag<String> DIRTY_EVENTS =
            new OutputTag<String>("dirty-behavior-events") {
                private static final long serialVersionUID = 1L;
            };

    private static final DateTimeFormatter SESSION_DATE =
            DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneOffset.UTC);

    @Override
    public void processElement(UserBehaviorEvent event,
                               Context ctx,
                               Collector<UserBehaviorEvent> out) {
        String problem = normalizeAndValidate(event);
        if (problem != null) {
            ctx.output(DIRTY_EVENTS, problem + " | " + safeJson(event));
            return;
        }
        out.collect(event);
    }

    /**
     * Normalizes the event in place.
     *
     * @return {@code null} when the event is valid, otherwise the rejection reason.
     */
    public static String normalizeAndValidate(UserBehaviorEvent event) {
        if (event == null) {
            return "null event";
        }
        if (event.getUserId() == null || event.getUserId().isBlank()) {
            return "missing userId";
        }
        event.setUserId(event.getUserId().trim());

        if (event.getBehaviorType() == null) {
            return "missing or unknown behaviorType";
        }
        if (event.getTimestamp() <= 0) {
            event.setTimestamp(EventTimeUtils.now());
        }
        if (!EventTimeUtils.isReasonable(event.getTimestamp())) {
            return "unreasonable timestamp " + event.getTimestamp();
        }
        if (event.getChannel() == null || event.getChannel().isBlank()) {
            event.setChannel("unknown");
        }
        if (event.getSessionId() == null || event.getSessionId().isBlank()) {
            event.setSessionId(defaultSessionId(event));
        }
        if (event.getEventId() == null || event.getEventId().isBlank()) {
            event.setEventId(defaultEventId(event));
        }
        if (requiresSku(event.getBehaviorType())
                && (event.getSkuId() == null || event.getSkuId().isBlank())) {
            return "missing skuId for " + event.getBehaviorType();
        }
        return null;
    }

    private static boolean requiresSku(BehaviorType type) {
        return type == BehaviorType.VIEW || type == BehaviorType.CART || type == BehaviorType.ORDER;
    }

    private static String defaultSessionId(UserBehaviorEvent event) {
        return event.getUserId() + "-"
                + SESSION_DATE.format(EventTimeUtils.toInstant(event.getTimestamp()));
    }

    private static String defaultEventId(UserBehaviorEvent event) {
        return IdempotentKeyGenerator.hash(
                event.getUserId() + "|" + event.getSkuId() + "|"
                        + event.getBehaviorType() + "|" + event.getTimestamp());
    }

    private static String safeJson(UserBehaviorEvent event) {
        try {
            return JsonUtils.toJson(event);
        } catch (Exception e) {
            return String.valueOf(event);
        }
    }
}
