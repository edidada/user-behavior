package org.example.behavior.job.aggregate;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.example.behavior.common.enums.UserLevel;
import org.example.behavior.domain.profile.UserValueProfile;

/**
 * Turns a spending accumulator into a profile fragment. The 24h chain fills
 * the daily fields and the level; the 1h chain only fills {@code amount1h}.
 * Both fragments are written to the same Redis hash by the profile sink.
 */
public class ProfileWindowFunction
        extends ProcessWindowFunction<SpendAccumulator, UserValueProfile, String, TimeWindow> {

    private static final long serialVersionUID = 1L;

    public enum WindowKind {
        ONE_HOUR,
        ONE_DAY
    }

    private final WindowKind kind;

    public ProfileWindowFunction(WindowKind kind) {
        this.kind = kind;
    }

    @Override
    public void process(String userId,
                        Context context,
                        Iterable<SpendAccumulator> elements,
                        Collector<UserValueProfile> out) {
        java.util.Iterator<SpendAccumulator> iterator = elements.iterator();
        SpendAccumulator acc = iterator.hasNext() ? iterator.next() : new SpendAccumulator();

        UserValueProfile.UserValueProfileBuilder builder = UserValueProfile.builder()
                .userId(userId)
                .granularity(kind == WindowKind.ONE_DAY ? "24h" : "1h")
                .windowEnd(context.window().getEnd())
                .updatedAt(System.currentTimeMillis());

        if (kind == WindowKind.ONE_DAY) {
            builder.amount24h(acc.getTotalAmount())
                    .orderCount24h(acc.getOrderCount())
                    .payCount24h(acc.getPayCount())
                    .avgOrderAmount(acc.getPayCount() == 0
                            ? 0D
                            : acc.getTotalAmount() / acc.getPayCount())
                    .userLevel(UserLevel.fromAmount(acc.getTotalAmount()));
        } else {
            builder.amount1h(acc.getTotalAmount());
        }
        out.collect(builder.build());
    }
}
