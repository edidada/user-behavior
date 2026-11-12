package org.example.behavior.job.aggregate;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Accumulator for the spending aggregation window.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SpendAccumulator implements Serializable {

    private static final long serialVersionUID = 1L;

    private double totalAmount;

    private long orderCount;

    private long payCount;
}
