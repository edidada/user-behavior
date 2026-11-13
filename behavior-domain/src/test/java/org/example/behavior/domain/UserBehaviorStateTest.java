package org.example.behavior.domain;

import org.example.behavior.common.enums.BehaviorType;
import org.example.behavior.common.enums.UserLevel;
import org.example.behavior.domain.event.UserBehaviorEvent;
import org.example.behavior.domain.state.UserBehaviorState;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UserBehaviorStateTest {

    @Test
    void builderDefaultsToMutableSequence() {
        UserBehaviorState state = UserBehaviorState.builder()
                .userId("u1")
                .build();

        assertNotNull(state.getRecentBehaviors());
        assertTrue(state.getRecentBehaviors().isEmpty());
        state.getRecentBehaviors().add(UserBehaviorEvent.builder()
                .behaviorType(BehaviorType.VIEW)
                .build());
        assertEquals(1, state.getRecentBehaviors().size());
    }

    @Test
    void stateIsSerializable() {
        UserBehaviorState state = new UserBehaviorState(
                "u1", new ArrayList<>(), 100L, 2, 200L);

        assertEquals("u1", state.getUserId());
        assertEquals(100L, state.getLastActiveTime());
        assertEquals(2, state.getCartWithoutPayCount());
    }

    @Test
    void userLevelIsDerivedFrom24hAmount() {
        assertEquals(UserLevel.NORMAL, UserLevel.fromAmount(0));
        assertEquals(UserLevel.SILVER, UserLevel.fromAmount(1_000));
        assertEquals(UserLevel.GOLD, UserLevel.fromAmount(10_000));
        assertEquals(UserLevel.PLATINUM, UserLevel.fromAmount(50_000));
        assertTrue(UserLevel.PLATINUM.atLeast(UserLevel.GOLD));
    }
}
