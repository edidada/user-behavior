package org.example.behavior.strategy.api;

import org.example.behavior.domain.command.CouponPushCommand;
import org.example.behavior.domain.result.PatternMatchResult;
import org.example.behavior.domain.rule.MarketingRule;

import java.io.Serializable;
import java.util.Collection;
import java.util.Optional;

/**
 * The only strategy surface the Flink job depends on. Keeping it in an api
 * package lets the job be compiled and tested against a stub while the rule
 * engine keeps evolving.
 */
public interface StrategyDecisionApi extends Serializable {

    /** Decides with the rules currently held by the configured repository. */
    Optional<CouponPushCommand> decide(PatternMatchResult match);

    /**
     * Decides with an explicit rule snapshot, used by the broadcast rule
     * stream where rules live in Flink broadcast state rather than in a
     * repository.
     */
    Optional<CouponPushCommand> decide(PatternMatchResult match, Collection<MarketingRule> rules);
}
