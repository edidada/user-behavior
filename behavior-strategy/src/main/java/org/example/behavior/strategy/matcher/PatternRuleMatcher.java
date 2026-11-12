package org.example.behavior.strategy.matcher;

import org.example.behavior.common.enums.UserLevel;
import org.example.behavior.domain.result.PatternMatchResult;
import org.example.behavior.domain.rule.MarketingRule;

import java.io.Serializable;
import java.util.Collection;
import java.util.Comparator;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Matches a detected behavior pattern against the configured marketing rules.
 */
public class PatternRuleMatcher implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final String ANY_PATTERN = "*";

    public Optional<MarketingRule> match(PatternMatchResult result, Collection<MarketingRule> rules) {
        if (result == null || rules == null || rules.isEmpty()) {
            return Optional.empty();
        }
        return rules.stream()
                .filter(MarketingRule::isEnabled)
                .filter(rule -> patternMatches(rule, result.getPatternName()))
                .filter(rule -> audienceMatches(rule, result.getUserLevel()))
                .max(Comparator.comparingInt(MarketingRule::getPriority));
    }

    private boolean patternMatches(MarketingRule rule, String patternName) {
        if (rule.getPatternName() == null || rule.getPatternName().isBlank()) {
            return false;
        }
        return ANY_PATTERN.equals(rule.getPatternName())
                || Objects.equals(rule.getPatternName(), patternName);
    }

    private boolean audienceMatches(MarketingRule rule, UserLevel userLevel) {
        Set<UserLevel> audience = rule.getAudienceLevels();
        if (audience == null || audience.isEmpty()) {
            return true;
        }
        return userLevel != null && audience.contains(userLevel);
    }
}
