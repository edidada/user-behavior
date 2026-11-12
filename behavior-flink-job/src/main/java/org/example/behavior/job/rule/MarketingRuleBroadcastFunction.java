package org.example.behavior.job.rule;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.example.behavior.domain.command.CouponPushCommand;
import org.example.behavior.domain.result.PatternMatchResult;
import org.example.behavior.domain.rule.MarketingRule;
import org.example.behavior.strategy.api.StrategyDecisionApi;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Design point 2 from the README: marketing rules are distributed through a
 * broadcast stream, so changing a rule does not require restarting the job.
 * Rules live in Flink broadcast state; the decision logic itself stays in the
 * Flink-free strategy module and is called with a rule snapshot.
 */
public class MarketingRuleBroadcastFunction
        extends BroadcastProcessFunction<PatternMatchResult, MarketingRule, CouponPushCommand> {

    private static final long serialVersionUID = 1L;

    /**
     * {@code MarketingRule.audienceLevels} is a Set, which Flink's generic type
     * reflector treats as a generic type. An explicit POJO descriptor keeps
     * broadcast state stable across savepoints.
     */
    public static final MapStateDescriptor<String, MarketingRule> RULE_STATE =
            new MapStateDescriptor<>(
                    "marketing-rules",
                    Types.STRING,
                    Types.POJO(MarketingRule.class));

    private final StrategyDecisionApi decisionService;

    public MarketingRuleBroadcastFunction(MapStateDescriptor<String, MarketingRule> ruleState,
                                          StrategyDecisionApi decisionService) {
        this.decisionService = decisionService;
    }

    @Override
    public void processBroadcastElement(MarketingRule rule,
                                        Context ctx,
                                        Collector<CouponPushCommand> out) throws Exception {
        if (rule == null || rule.getId() == null || rule.getId().isBlank()) {
            return;
        }
        BroadcastState<String, MarketingRule> state = ctx.getBroadcastState(RULE_STATE);
        if (rule.isEnabled()) {
            state.put(rule.getId(), rule);
        } else {
            state.remove(rule.getId());
        }
    }

    @Override
    public void processElement(PatternMatchResult match,
                               ReadOnlyContext ctx,
                               Collector<CouponPushCommand> out) throws Exception {
        ReadOnlyBroadcastState<String, MarketingRule> state =
                ctx.getBroadcastState(RULE_STATE);
        List<MarketingRule> rules = new ArrayList<>();
        for (Map.Entry<String, MarketingRule> entry : state.immutableEntries()) {
            rules.add(entry.getValue());
        }
        decisionService.decide(match, rules).ifPresent(out::collect);
    }
}
