package org.example.behavior.job;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.example.behavior.common.constant.CepPatterns;
import org.example.behavior.common.enums.BehaviorType;
import org.example.behavior.domain.event.UserBehaviorEvent;
import org.example.behavior.domain.result.PatternMatchResult;
import org.example.behavior.job.function.CartAbandonmentFunction;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CartAbandonmentFunctionTest {

    private static final long TIMEOUT_MILLIS = 60_000L;

    @Test
    void emitsMatchWhenCartIsNotPaidInTime() throws Exception {
        KeyedOneInputStreamOperatorTestHarness<String, UserBehaviorEvent, PatternMatchResult> harness =
                newHarness();
        try {
            harness.open();
            long cartTimestamp = 1_000L;
            harness.processElement(new StreamRecord<>(
                    event("u1", BehaviorType.CART, "sku-1", cartTimestamp), cartTimestamp));

            harness.processWatermark(cartTimestamp + TIMEOUT_MILLIS);

            List<PatternMatchResult> results = harness.extractOutputValues();
            assertEquals(1, results.size());
            PatternMatchResult result = results.get(0);
            assertEquals("u1", result.getUserId());
            assertEquals(CepPatterns.CART_NO_PAY_TIMEOUT, result.getPatternName());
            assertEquals(List.of("sku-1"), result.getMatchedSkuIds());
        } finally {
            harness.close();
        }
    }

    @Test
    void clearsTimerAfterPayment() throws Exception {
        KeyedOneInputStreamOperatorTestHarness<String, UserBehaviorEvent, PatternMatchResult> harness =
                newHarness();
        try {
            harness.open();
            long cartTimestamp = 1_000L;
            harness.processElement(new StreamRecord<>(
                    event("u1", BehaviorType.CART, "sku-1", cartTimestamp), cartTimestamp));
            harness.processElement(new StreamRecord<>(
                    event("u1", BehaviorType.PAY, "sku-1", cartTimestamp + 5_000L),
                    cartTimestamp + 5_000L));

            harness.processWatermark(cartTimestamp + TIMEOUT_MILLIS * 2);

            assertTrue(harness.extractOutputValues().isEmpty());
        } finally {
            harness.close();
        }
    }

    private static KeyedOneInputStreamOperatorTestHarness<String, UserBehaviorEvent, PatternMatchResult>
    newHarness() throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new CartAbandonmentFunction(TIMEOUT_MILLIS)),
                UserBehaviorEvent::getUserId,
                Types.STRING);
    }

    private static UserBehaviorEvent event(String userId,
                                           BehaviorType type,
                                           String skuId,
                                           long timestamp) {
        return UserBehaviorEvent.builder()
                .userId(userId)
                .behaviorType(type)
                .skuId(skuId)
                .timestamp(timestamp)
                .build();
    }
}
