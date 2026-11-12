package org.example.behavior.job.cep;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.example.behavior.common.enums.BehaviorType;
import org.example.behavior.domain.event.UserBehaviorEvent;
import org.example.behavior.domain.result.PatternMatchResult;

import java.time.Duration;

/**
 * Link A: CEP based detection of the "view -&gt; cart -&gt; order" sequence.
 * Fixed patterns such as this one are a natural fit for CEP; dynamic
 * sequences are handled by the timer based function in the same package.
 */
public final class CepPatternDetector {

    private CepPatternDetector() {
    }

    public static Pattern<UserBehaviorEvent, ?> viewCartOrder(Duration within) {
        return Pattern.<UserBehaviorEvent>begin("view")
                .where(new SimpleCondition<UserBehaviorEvent>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public boolean filter(UserBehaviorEvent event) {
                        return event.getBehaviorType() == BehaviorType.VIEW;
                    }
                })
                .next("cart")
                .where(new SimpleCondition<UserBehaviorEvent>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public boolean filter(UserBehaviorEvent event) {
                        return event.getBehaviorType() == BehaviorType.CART;
                    }
                })
                .next("order")
                .where(new SimpleCondition<UserBehaviorEvent>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public boolean filter(UserBehaviorEvent event) {
                        return event.getBehaviorType() == BehaviorType.ORDER;
                    }
                })
                .within(Time.milliseconds(within.toMillis()));
    }

    public static SingleOutputStreamOperator<PatternMatchResult> attach(
            KeyedStream<UserBehaviorEvent, String> events, Duration within) {
        Pattern<UserBehaviorEvent, ?> pattern = viewCartOrder(within);
        PatternStream<UserBehaviorEvent> patternStream = CEP.pattern(events, pattern);
        return patternStream
                .process(new ViewCartOrderProcessFunction())
                .returns(PatternMatchResult.class)
                .name("cep-view-cart-order")
                .uid("cep-view-cart-order");
    }
}
