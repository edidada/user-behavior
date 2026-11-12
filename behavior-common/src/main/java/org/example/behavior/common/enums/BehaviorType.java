package org.example.behavior.common.enums;

/**
 * Raw user behavior types emitted by the tracking SDK.
 */
public enum BehaviorType {
    VIEW,
    CART,
    ORDER,
    PAY;

    public static BehaviorType fromCode(String code) {
        if (code == null) {
            return null;
        }
        for (BehaviorType type : values()) {
            if (type.name().equalsIgnoreCase(code.trim())) {
                return type;
            }
        }
        return null;
    }
}
