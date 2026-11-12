package org.example.behavior.common.exception;

/**
 * Unchecked exception for any behavior pipeline failure that cannot be recovered locally.
 */
public class BehaviorException extends RuntimeException {

    public BehaviorException(String message) {
        super(message);
    }

    public BehaviorException(String message, Throwable cause) {
        super(message, cause);
    }
}
