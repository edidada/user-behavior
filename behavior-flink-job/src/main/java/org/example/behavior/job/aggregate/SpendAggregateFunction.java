package org.example.behavior.job.aggregate;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.example.behavior.common.enums.BehaviorType;
import org.example.behavior.domain.event.UserBehaviorEvent;

import java.math.BigDecimal;

/**
 * Incrementally accumulates order count and paid amount. Incremental
 * aggregation keeps only a small accumulator in state, no matter how many
 * events fall into the window.
 */
public class SpendAggregateFunction
        implements AggregateFunction<UserBehaviorEvent, SpendAccumulator, SpendAccumulator> {

    private static final long serialVersionUID = 1L;

    @Override
    public SpendAccumulator createAccumulator() {
        return new SpendAccumulator();
    }

    @Override
    public SpendAccumulator add(UserBehaviorEvent event, SpendAccumulator acc) {
        if (event == null) {
            return acc;
        }
        if (event.getBehaviorType() == BehaviorType.ORDER) {
            acc.setOrderCount(acc.getOrderCount() + 1);
        } else if (event.getBehaviorType() == BehaviorType.PAY) {
            acc.setPayCount(acc.getPayCount() + 1);
            acc.setTotalAmount(acc.getTotalAmount() + amountOf(event));
        }
        return acc;
    }

    @Override
    public SpendAccumulator getResult(SpendAccumulator acc) {
        return acc;
    }

    @Override
    public SpendAccumulator merge(SpendAccumulator a, SpendAccumulator b) {
        return new SpendAccumulator(
                a.getTotalAmount() + b.getTotalAmount(),
                a.getOrderCount() + b.getOrderCount(),
                a.getPayCount() + b.getPayCount());
    }

    private static double amountOf(UserBehaviorEvent event) {
        BigDecimal amount = event.getAmount();
        return amount == null ? 0D : amount.doubleValue();
    }
}
