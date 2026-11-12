package org.example.behavior.strategy.repository;

import org.example.behavior.domain.rule.MarketingRule;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;

/**
 * Source of marketing rules. Implementations may read from a database, a
 * configuration center or a Flink broadcast stream.
 */
public interface RuleRepository extends Serializable {

    List<MarketingRule> findAll();

    default Optional<MarketingRule> findById(String ruleId) {
        if (ruleId == null) {
            return Optional.empty();
        }
        return findAll().stream()
                .filter(rule -> ruleId.equals(rule.getId()))
                .findFirst();
    }
}
