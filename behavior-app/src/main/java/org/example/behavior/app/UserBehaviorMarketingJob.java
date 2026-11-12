package org.example.behavior.app;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.example.behavior.common.constant.KafkaTopics;
import org.example.behavior.common.util.JsonUtils;
import org.example.behavior.domain.command.CouponPushCommand;
import org.example.behavior.domain.event.UserBehaviorEvent;
import org.example.behavior.domain.profile.UserValueProfile;
import org.example.behavior.domain.result.PatternMatchResult;
import org.example.behavior.domain.rule.MarketingRule;
import org.example.behavior.job.aggregate.ProfileWindowFunction;
import org.example.behavior.job.aggregate.SpendAggregateFunction;
import org.example.behavior.job.cep.CepPatternDetector;
import org.example.behavior.job.function.CartAbandonmentFunction;
import org.example.behavior.job.function.StrategyProcessFunction;
import org.example.behavior.job.rule.MarketingRuleBroadcastFunction;
import org.example.behavior.sink.CouponPushSink;
import org.example.behavior.sink.RedisProfileSink;
import org.example.behavior.source.function.BehaviorNormalizer;
import org.example.behavior.source.source.BehaviorKafkaSource;
import org.example.behavior.source.source.RuleKafkaSource;
import org.example.behavior.source.watermark.BehaviorWatermarkStrategies;
import org.example.behavior.strategy.cooldown.InMemoryCooldownChecker;
import org.example.behavior.strategy.coupon.DefaultCouponSelector;
import org.example.behavior.strategy.matcher.PatternRuleMatcher;
import org.example.behavior.strategy.repository.InMemoryRuleRepository;
import org.example.behavior.strategy.repository.RuleRepository;
import org.example.behavior.strategy.service.StrategyDecisionService;

import java.time.Duration;

/**
 * Job entry point. Assembles both links:
 *
 * <ul>
 *   <li>Link A: CEP "view -&gt; cart -&gt; order" plus the cart-without-pay
 *       timer, feeding the strategy engine and the coupon push sink.</li>
 *   <li>Link B: 1h and 24h sliding window spending aggregation, feeding the
 *       Redis profile sink used by the front end.</li>
 * </ul>
 */
public final class UserBehaviorMarketingJob {

    private UserBehaviorMarketingJob() {
    }

    public static void main(String[] args) throws Exception {
        JobConfig config = JobConfig.load(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(config.getInt("job.parallelism", 1));
        env.enableCheckpointing(config.getLong("job.checkpoint.interval.ms", 60_000L));

        KafkaSource<UserBehaviorEvent> kafkaSource = BehaviorKafkaSource.build(
                config.get("kafka.brokers", "localhost:9092"),
                config.get("kafka.topic", KafkaTopics.BEHAVIOR_EVENTS),
                config.get("kafka.group.id", "user-behavior-marketing"));

        DataStream<UserBehaviorEvent> raw = env.fromSource(
                kafkaSource, WatermarkStrategy.noWatermarks(), "behavior-kafka-source");

        SingleOutputStreamOperator<UserBehaviorEvent> normalized = raw
                .process(new BehaviorNormalizer())
                .name("normalize-behavior")
                .uid("normalize-behavior");

        normalized.getSideOutput(BehaviorNormalizer.DIRTY_EVENTS).print("dirty-behavior");

        DataStream<UserBehaviorEvent> events = normalized.assignTimestampsAndWatermarks(
                BehaviorWatermarkStrategies.defaults());

        KeyedStream<UserBehaviorEvent, String> keyed =
                events.keyBy(UserBehaviorEvent::getUserId);

        // ---------------- Link A: pattern detection + strategy ----------------
        long cepWithinMinutes = config.getLong("cep.within.minutes", 30L);
        SingleOutputStreamOperator<PatternMatchResult> cepMatches =
                CepPatternDetector.attach(keyed, Duration.ofMinutes(cepWithinMinutes));

        long cartTimeoutMillis = config.getLong("cart.timeout.minutes", 30L) * 60_000L;
        SingleOutputStreamOperator<PatternMatchResult> timerMatches = keyed
                .process(new CartAbandonmentFunction(cartTimeoutMillis))
                .returns(PatternMatchResult.class)
                .name("cart-abandonment")
                .uid("cart-abandonment");

        DataStream<PatternMatchResult> matches = cepMatches.union(timerMatches);

        StrategyDecisionService decisionService = buildDecisionService();

        DataStream<CouponPushCommand> commands;
        if (config.getBoolean("rules.broadcast.enabled", false)) {
            // Rules are loaded from Kafka and broadcast, so rule changes apply
            // without restarting the job.
            KafkaSource<String> ruleSource = RuleKafkaSource.build(
                    config.get("kafka.brokers", "localhost:9092"),
                    config.get("kafka.rule.topic", KafkaTopics.MARKETING_RULES),
                    config.get("kafka.group.id", "user-behavior-marketing") + "-rules");

            DataStream<MarketingRule> rules = env
                    .fromSource(ruleSource, WatermarkStrategy.noWatermarks(), "marketing-rule-source")
                    .map(json -> JsonUtils.fromJson(json, MarketingRule.class))
                    .returns(MarketingRule.class)
                    .name("parse-marketing-rule")
                    .uid("parse-marketing-rule");

            commands = matches
                    .connect(rules)
                    .broadcast(MarketingRuleBroadcastFunction.RULE_STATE)
                    .process(new MarketingRuleBroadcastFunction(decisionService))
                    .returns(CouponPushCommand.class)
                    .name("broadcast-strategy-decision")
                    .uid("broadcast-strategy-decision");
        } else {
            commands = matches
                    .flatMap(new StrategyProcessFunction(decisionService))
                    .returns(CouponPushCommand.class)
                    .name("strategy-decision")
                    .uid("strategy-decision");
        }

        commands.addSink(new CouponPushSink(
                        config.get("redis.host", "localhost"),
                        config.getInt("redis.port", 6379),
                        config.get("coupon.endpoint", "http://localhost:8080/api/coupons/push")))
                .name("coupon-push-sink")
                .uid("coupon-push-sink");

        // ---------------- Link B: spending aggregation -------------------------
        SingleOutputStreamOperator<UserValueProfile> hourlyProfiles = keyed
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
                .aggregate(new SpendAggregateFunction(),
                        new ProfileWindowFunction(ProfileWindowFunction.WindowKind.ONE_HOUR))
                .returns(UserValueProfile.class)
                .name("profile-1h")
                .uid("profile-1h");

        SingleOutputStreamOperator<UserValueProfile> dailyProfiles = keyed
                .window(SlidingEventTimeWindows.of(Time.hours(24), Time.minutes(5)))
                .aggregate(new SpendAggregateFunction(),
                        new ProfileWindowFunction(ProfileWindowFunction.WindowKind.ONE_DAY))
                .returns(UserValueProfile.class)
                .name("profile-24h")
                .uid("profile-24h");

        hourlyProfiles.union(dailyProfiles)
                .addSink(new RedisProfileSink(
                        config.get("redis.host", "localhost"),
                        config.getInt("redis.port", 6379)))
                .name("redis-profile-sink")
                .uid("redis-profile-sink");

        env.execute("user-behavior-marketing");
    }

    private static StrategyDecisionService buildDecisionService() {
        RuleRepository ruleRepository =
                new InMemoryRuleRepository(DefaultRules.initialRules());
        return new StrategyDecisionService(
                ruleRepository,
                new PatternRuleMatcher(),
                new DefaultCouponSelector(),
                new InMemoryCooldownChecker());
    }
}
