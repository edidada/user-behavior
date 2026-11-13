package org.example.behavior.job;

import org.example.behavior.common.enums.BehaviorType;
import org.example.behavior.domain.event.UserBehaviorEvent;
import org.example.behavior.job.aggregate.SpendAccumulator;
import org.example.behavior.job.aggregate.SpendAggregateFunction;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SpendAggregateFunctionTest {

    private final SpendAggregateFunction function = new SpendAggregateFunction();

    @Test
    void incrementsOrderAndPayCounters() {
        SpendAccumulator acc = function.createAccumulator();
        function.add(event(BehaviorType.ORDER, null), acc);
        function.add(event(BehaviorType.PAY, new BigDecimal("100.5")), acc);
        function.add(event(BehaviorType.PAY, new BigDecimal("50")), acc);

        assertEquals(1, acc.getOrderCount());
        assertEquals(2, acc.getPayCount());
        assertEquals(150.5, acc.getTotalAmount());
    }

    @Test
    void ignoresNullAmountsAndViewEvents() {
        SpendAccumulator acc = function.createAccumulator();
        function.add(event(BehaviorType.VIEW, null), acc);
        function.add(event(BehaviorType.PAY, null), acc);

        assertEquals(1, acc.getPayCount());
        assertEquals(0D, acc.getTotalAmount());
    }

    @Test
    void mergesAccumulators() {
        SpendAccumulator a = new SpendAccumulator(10D, 1, 1);
        SpendAccumulator b = new SpendAccumulator(20D, 2, 3);

        SpendAccumulator merged = function.merge(a, b);

        assertEquals(30D, merged.getTotalAmount());
        assertEquals(3, merged.getOrderCount());
        assertEquals(4, merged.getPayCount());
    }

    private static UserBehaviorEvent event(BehaviorType type, BigDecimal amount) {
        return UserBehaviorEvent.builder()
                .behaviorType(type)
                .amount(amount)
                .build();
    }
}
