package org.example.behavior.job.function;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.behavior.common.enums.BehaviorType;
import org.example.behavior.domain.event.UserBehaviorEvent;
import org.example.behavior.domain.state.UserBehaviorState;

import java.util.ArrayList;

/**
 * Manual sequence detection with {@code ValueState<List<Behavior>>}: keeps the
 * most recent N behaviors for each user, updates the last-active time and the
 * cart-without-pay counter, as described in the README. Unlike the CEP based
 * detector this state is easy to query and tune per user.
 *
 * <p>The output stream can be used for dynamic sequence rules; the cart
 * timeout match itself is emitted by {@link CartAbandonmentFunction}.
 */
public class BehaviorSequenceFunction
        extends KeyedProcessFunction<String, UserBehaviorEvent, UserBehaviorState> {

    private static final long serialVersionUID = 1L;

    public static final int DEFAULT_MAX_SEQUENCE_LENGTH = 20;

    private final int maxSequenceLength;

    private transient ValueState<UserBehaviorState> state;

    public BehaviorSequenceFunction() {
        this(DEFAULT_MAX_SEQUENCE_LENGTH);
    }

    public BehaviorSequenceFunction(int maxSequenceLength) {
        this.maxSequenceLength = Math.max(1, maxSequenceLength);
    }

    @Override
    public void open(Configuration parameters) {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>(
                "user-behavior-sequence", Types.POJO(UserBehaviorState.class)));
    }

    @Override
    public void processElement(UserBehaviorEvent event,
                               Context ctx,
                               Collector<UserBehaviorState> out) throws Exception {
        if (event == null || event.getBehaviorType() == null) {
            return;
        }
        UserBehaviorState current = state.value();
        if (current == null) {
            current = UserBehaviorState.builder()
                    .userId(ctx.getCurrentKey())
                    .recentBehaviors(new ArrayList<>())
                    .build();
        }
        if (current.getRecentBehaviors() == null) {
            current.setRecentBehaviors(new ArrayList<>());
        }

        current.getRecentBehaviors().add(event);
        while (current.getRecentBehaviors().size() > maxSequenceLength) {
            current.getRecentBehaviors().remove(0);
        }
        current.setLastActiveTime(Math.max(current.getLastActiveTime(), event.getTimestamp()));
        current.setUpdatedAt(System.currentTimeMillis());

        if (event.getBehaviorType() == BehaviorType.CART) {
            current.setCartWithoutPayCount(current.getCartWithoutPayCount() + 1);
        } else if (event.getBehaviorType() == BehaviorType.PAY) {
            current.setCartWithoutPayCount(0);
        }

        state.update(current);
        out.collect(current);
    }
}
