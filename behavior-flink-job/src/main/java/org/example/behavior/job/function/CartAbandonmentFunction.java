package org.example.behavior.job.function;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.behavior.common.constant.CepPatterns;
import org.example.behavior.common.enums.BehaviorType;
import org.example.behavior.domain.event.UserBehaviorEvent;
import org.example.behavior.domain.result.PatternMatchResult;

import java.util.List;

/**
 * Manual sequence detection with timers: after a cart event, if the user has
 * not paid within {@code timeoutMillis} of event time, emit a
 * {@code CART_NO_PAY_TIMEOUT} match.
 *
 * <p>Compared with CEP this is easier to tune per user and can hold arbitrary
 * state, so both implementations are kept and can be routed by rule complexity.
 */
public class CartAbandonmentFunction
        extends KeyedProcessFunction<String, UserBehaviorEvent, PatternMatchResult> {

    private static final long serialVersionUID = 1L;

    public static final long DEFAULT_TIMEOUT_MILLIS = 30 * 60 * 1000L;

    private final long timeoutMillis;

    private transient ValueState<Long> timerState;

    private transient ValueState<String> skuState;

    public CartAbandonmentFunction() {
        this(DEFAULT_TIMEOUT_MILLIS);
    }

    public CartAbandonmentFunction(long timeoutMillis) {
        this.timeoutMillis = timeoutMillis;
    }

    @Override
    public void open(Configuration parameters) {
        timerState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("cart-timer", Types.LONG));
        skuState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("cart-sku", Types.STRING));
    }

    @Override
    public void processElement(UserBehaviorEvent event,
                               Context ctx,
                               Collector<PatternMatchResult> out) throws Exception {
        BehaviorType type = event.getBehaviorType();
        if (type == BehaviorType.CART) {
            Long previous = timerState.value();
            if (previous != null) {
                ctx.timerService().deleteEventTimeTimer(previous);
            }
            long fireAt = event.getTimestamp() + timeoutMillis;
            ctx.timerService().registerEventTimeTimer(fireAt);
            timerState.update(fireAt);
            skuState.update(event.getSkuId());
        } else if (type == BehaviorType.PAY) {
            Long pending = timerState.value();
            if (pending != null) {
                ctx.timerService().deleteEventTimeTimer(pending);
            }
            timerState.clear();
            skuState.clear();
        }
    }

    @Override
    public void onTimer(long timestamp,
                        OnTimerContext ctx,
                        Collector<PatternMatchResult> out) throws Exception {
        if (timerState.value() == null) {
            return;
        }
        String skuId = skuState.value();
        out.collect(PatternMatchResult.builder()
                .userId(ctx.getCurrentKey())
                .patternName(CepPatterns.CART_NO_PAY_TIMEOUT)
                .matchedAt(timestamp)
                .detail("cart without payment after " + timeoutMillis + "ms")
                .matchedSkuIds(skuId == null ? List.of() : List.of(skuId))
                .build());
        timerState.clear();
        skuState.clear();
    }
}
