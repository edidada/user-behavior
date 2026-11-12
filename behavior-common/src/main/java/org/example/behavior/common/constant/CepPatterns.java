package org.example.behavior.common.constant;

/**
 * Names of the behavior patterns detected by the job. They are also used as the
 * "pattern expression" that marketing rules match against.
 */
public final class CepPatterns {

    /** view -&gt; cart -&gt; order within a short window. */
    public static final String VIEW_CART_ORDER = "VIEW_CART_ORDER";

    /** cart without a following payment, detected by a KeyedProcessFunction timer. */
    public static final String CART_NO_PAY_TIMEOUT = "CART_NO_PAY_TIMEOUT";

    private CepPatterns() {
    }
}
