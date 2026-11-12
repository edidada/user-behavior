package org.example.behavior.job.cep;

import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.util.Collector;
import org.example.behavior.common.constant.CepPatterns;
import org.example.behavior.domain.event.UserBehaviorEvent;
import org.example.behavior.domain.result.PatternMatchResult;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Converts a CEP match into the strategy engine's input model.
 */
public class ViewCartOrderProcessFunction
        extends PatternProcessFunction<UserBehaviorEvent, PatternMatchResult> {

    private static final long serialVersionUID = 1L;

    @Override
    public void processMatch(Map<String, List<UserBehaviorEvent>> match,
                             Context ctx,
                             Collector<PatternMatchResult> out) {
        List<UserBehaviorEvent> orders = match.get("order");
        if (orders == null || orders.isEmpty()) {
            return;
        }
        UserBehaviorEvent order = orders.get(orders.size() - 1);
        List<String> skuIds = match.values().stream()
                .flatMap(List::stream)
                .map(UserBehaviorEvent::getSkuId)
                .filter(Objects::nonNull)
                .distinct()
                .toList();

        out.collect(PatternMatchResult.builder()
                .userId(order.getUserId())
                .patternName(CepPatterns.VIEW_CART_ORDER)
                .matchedAt(order.getTimestamp())
                .detail("view -> cart -> order")
                .matchedSkuIds(skuIds)
                .build());
    }
}
