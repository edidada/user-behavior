package org.example.behavior.domain.state;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.example.behavior.domain.event.UserBehaviorEvent;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Per-user state kept by the KeyedProcessFunction based sequence detector.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UserBehaviorState implements Serializable {

    private static final long serialVersionUID = 1L;

    private String userId;

    /** Most recent N behaviors, oldest first. */
    @Builder.Default
    private List<UserBehaviorEvent> recentBehaviors = new ArrayList<>();

    private long lastActiveTime;

    private int cartWithoutPayCount;

    private long updatedAt;
}
