package org.example.behavior.strategy.repository;

import org.example.behavior.domain.rule.MarketingRule;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Simple repository used by the job bootstrap and by unit tests. Rules are
 * replaced atomically so a running job can refresh them without restarting.
 */
public class InMemoryRuleRepository implements RuleRepository {

    private static final long serialVersionUID = 1L;

    private volatile List<MarketingRule> rules = Collections.emptyList();

    public InMemoryRuleRepository() {
    }

    public InMemoryRuleRepository(List<MarketingRule> rules) {
        replaceAll(rules);
    }

    @Override
    public List<MarketingRule> findAll() {
        return rules;
    }

    public void replaceAll(List<MarketingRule> newRules) {
        this.rules = newRules == null
                ? Collections.emptyList()
                : Collections.unmodifiableList(new ArrayList<>(newRules));
    }
}
