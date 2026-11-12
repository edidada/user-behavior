package org.example.behavior.domain.result;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.example.behavior.common.enums.UserLevel;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Output of the pattern detection link, consumed by the strategy engine.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PatternMatchResult implements Serializable {

    private static final long serialVersionUID = 1L;

    private String userId;

    private String patternName;

    private long matchedAt;

    private UserLevel userLevel;

    private String detail;

    @Builder.Default
    private List<String> matchedSkuIds = new ArrayList<>();
}
