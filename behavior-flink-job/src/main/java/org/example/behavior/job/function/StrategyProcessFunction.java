package org.example.behavior.job.function;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;
import org.example.behavior.domain.command.CouponPushCommand;
import org.example.behavior.domain.result.PatternMatchResult;
import org.example.behavior.strategy.api.StrategyDecisionApi;

/**
 * Bridge between the Flink stream and the plain-Java strategy engine. The job
 * depends only on {@link StrategyDecisionApi}, never on rule internals.
 */
public class StrategyProcessFunction
        extends RichFlatMapFunction<PatternMatchResult, CouponPushCommand> {

    private static final long serialVersionUID = 1L;

    private final StrategyDecisionApi decisionService;

    public StrategyProcessFunction(StrategyDecisionApi decisionService) {
        this.decisionService = decisionService;
    }

    @Override
    public void flatMap(PatternMatchResult match, Collector<CouponPushCommand> out) {
        decisionService.decide(match).ifPresent(out::collect);
    }
}
