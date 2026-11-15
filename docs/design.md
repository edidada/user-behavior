# 详细设计

> 对应根目录 `README.md` 的架构总览，本文深入到类与数据流级别。面向要修改代码的开发者。

## 1. 系统定位

基于用户实时行为（浏览 VIEW / 加购 CART / 下单 ORDER / 支付 PAY）：

- **链路 A（模式检测 + 动态营销）**：检测行为模式（如"浏览→加购→未支付超时"），经策略引擎决策后推送优惠券
- **链路 B（高价值用户聚合）**：实时聚合 1h / 24h 消费金额，生成用户价值画像写入 Redis 供前端查询

## 2. 运行时拓扑

```
Kafka topic: user-behavior-events (JSON, v2 协议)
   │  KafkaSource ─ 反序列化失败的消息仅告警跳过
   ▼
BehaviorNormalizer (ProcessFunction)
   │  补全 eventId/sessionId/channel、校验时间戳与 skuId
   │──脏数据──▶ side output DIRTY_EVENTS → print("dirty-behavior")
   ▼
assignTimestampsAndWatermarks(乱序 5s + 空闲分区超时 1min)
   ▼
keyBy(userId) ─────────────────────────────────────────────┐
   │                                                       │
   ▼ 链路 A：模式检测                                       ▼ 链路 B：价值聚合
CepPatternDetector                                        1h / 24h SlidingEventTimeWindows
  VIEW→CART→ORDER within cep.within.minutes                 (步长 5min)
CartAbandonmentFunction                                    SpendAggregateFunction (增量聚合)
  加购后 cart.timeout.minutes 未支付 → onTimer               ProfileWindowFunction (算客单价/等级)
BehaviorSequenceFunction                                   │
  维护最近 N 条行为序列 ValueState (可选链路)                 │
   │ union                                                 │
   ▼                                                       ▼
策略决策（二选一，由 rules.broadcast.enabled 控制）          RedisProfileSink
  - StrategyProcessFunction          (静态规则)              Hash: user:behavior:profile:{userId}
  - MarketingRuleBroadcastFunction   (广播规则)              ZSet: user:highvalue:rank
   ▼
CouponPushCommand
   ├─ coupon.push.mode=http   → CouponPushSink      (Redis SETNX 幂等 → HTTP 调优惠券服务)
   └─ coupon.push.mode=kafka  → CouponCommandKafkaSink (AT_LEAST_ONCE → user-coupon-push-commands)
```

## 3. 模块与依赖

```
behavior-common ← behavior-domain ← behavior-strategy
                        ↑                  ↑
                behavior-source            |
                        ↑                  |
                behavior-flink-job ────────┘
                        ↑
                behavior-sink
                        ↑
                behavior-app（唯一打包入口）
```

| 模块 | 职责 | 关键约束 |
|------|------|---------|
| `behavior-common` | 常量、枚举、工具、异常 | 仅依赖 Jackson/Lombok/SLF4J，**不依赖 Flink** |
| `behavior-domain` | 纯 POJO：事件、状态、画像、规则、指令 | 仅依赖 common，可独立单测 |
| `behavior-source` | KafkaSource 构建、规范化、脏数据过滤、水印 | 只依赖 domain |
| `behavior-flink-job` | CEP / KeyedProcessFunction 检测、窗口聚合、广播 | 依赖 source、strategy 接口 |
| `behavior-strategy` | 规则匹配、选券、冷却、决策编排 | **不依赖 Flink**，纯 Java 可 JUnit 全覆盖 |
| `behavior-sink` | Redis 画像写入、优惠券推送（HTTP/Kafka）、去重 | 依赖 domain + jedis + connector-kafka |
| `behavior-app` | 组装拓扑、配置加载、shade 打包 | 唯一 main 入口 |

## 4. 核心类速查

| 类 | 位置 | 说明 |
|----|------|------|
| `UserBehaviorMarketingJob` | app | main：加载配置、组装两条链路、`env.execute()` |
| `JobConfig` | app | classpath `application.properties` + 命令行 `--key=value` 覆盖 |
| `DefaultRules` | app | 本地/默认启动规则（两条，见 §7） |
| `BehaviorKafkaSource` | source | 事件源；`committedOffsets(EARLIEST)`、checkpoint 提交位移 |
| `RuleKafkaSource` | source | 规则源；`committedOffsets(LATEST)`，字符串 JSON |
| `UserBehaviorEventDeserializationSchema` | source | JSON→事件；坏消息记日志跳过，不断流 |
| `BehaviorNormalizer` | source | 静态方法 `normalizeAndValidate` 可脱离 Flink 单测；脏数据走 side output |
| `BehaviorWatermarkStrategies` | source | 乱序 5s + idle 1min |
| `LegacyEventMapper` | source | v1 协议字段映射（`uid/item_id/event/ts` → v2） |
| `CepPatternDetector` | flink-job | `VIEW→CART→ORDER within(30m)` 的 CEP 模式 |
| `ViewCartOrderProcessFunction` | flink-job | CEP 命中 → `PatternMatchResult(VIEW_CART_ORDER)` |
| `CartAbandonmentFunction` | flink-job | 加购注册 event-time 定时器，PAY 清除，超时发 `CART_NO_PAY_TIMEOUT`；重复加购先删旧定时器 |
| `BehaviorSequenceFunction` | flink-job | `ValueState<UserBehaviorState>` 维护最近 N=20 条序列、最后活跃时间、加购未支付计数；PAY 清零计数 |
| `SpendAggregateFunction` | flink-job | 增量累加：ORDER 计数、PAY 计数与金额（null 金额按 0） |
| `ProfileWindowFunction` | flink-job | 24h 链路填客单价、`UserLevel`；1h 链路只填 `amount1h` |
| `StrategyProcessFunction` | flink-job | 静态规则模式下的桥接：调用 `StrategyDecisionApi` |
| `MarketingRuleBroadcastFunction` | flink-job | 广播流模式：规则存 `MapState`，enabled=false 表示删除规则 |
| `StrategyDecisionService` | strategy | 编排：匹配规则 → 选券 → 冷却 → 组装指令 |
| `PatternRuleMatcher` | strategy | enabled + patternName（`*` 通配）+ audienceLevels 过滤，取 priority 最高 |
| `DefaultCouponSelector` | strategy | 直接返回规则的 `couponTemplateId`（生产可扩展库存/预算检查） |
| `InMemoryCooldownChecker` | strategy | 进程内冷却，本地/测试用 |
| `RedisCooldownChecker` | strategy | `SET key 1 NX EX cooldownSeconds` 原子检查+占用；Redis 不可用降级为放行 |
| `CouponPushSink` | sink | SETNX 幂等 → HTTP POST；非 2xx 释放幂等键让 Flink 重试 |
| `CouponCommandKafkaSink` | sink | `KafkaSink` + `AT_LEAST_ONCE`，key=userId，value=JSON |
| `DedupSink` | sink | 独立幂等去重阶段（SETNX + 24h TTL），Redis 故障时本地缓冲并放行 |
| `RedisProfileSink` | sink | 1h/24h 两个片段写同一个 Hash；仅 24h 片段更新 ZSet 排名 |

## 5. 数据模型（behavior-domain）

| 对象 | 关键字段 | 生产者 → 消费者 |
|------|---------|----------------|
| `UserBehaviorEvent` | eventId, userId, skuId, behaviorType, timestamp, sessionId, channel, amount(BigDecimal) | Kafka → 全链路 |
| `UserBehaviorState` | recentBehaviors(≤20), lastActiveTime, cartWithoutPayCount, updatedAt | BehaviorSequenceFunction 内部状态/输出 |
| `PatternMatchResult` | userId, patternName, matchedAt, userLevel, detail, matchedSkuIds | 检测层 → 策略层 |
| `MarketingRule` | id, name, patternName, couponTemplateId, couponType, cooldownSeconds, audienceLevels, enabled, priority | 配置/广播流 → 策略层 |
| `CouponPushCommand` | userId, ruleId, couponId, couponType, triggerReason, channel, idempotentId, createdAt | 策略层 → 触达层 |
| `UserValueProfile` | amount1h / amount24h / orderCount24h / payCount24h / avgOrderAmount / userLevel / granularity / windowEnd | 聚合层 → Redis |

`UserLevel` 按 24h 消费额划分：NORMAL 0 / SILVER 1,000 / GOLD 10,000 / PLATINUM 50,000。

## 6. 外部依赖约定

### Kafka Topic

| Topic | 方向 | 说明 |
|-------|------|------|
| `user-behavior-events` | 输入 | 行为事件 JSON（v2 协议字段），消费组 `user-behavior-marketing` |
| `user-marketing-rules` | 输入(可选) | `MarketingRule` JSON，规则热更新，消费组 `user-behavior-marketing-rules` |
| `user-coupon-push-commands` | 输出(可选) | `coupon.push.mode=kafka` 时的优惠券触达指令 |

### Redis Key（`RedisKeys`）

| Key | 类型 | 用途 | TTL |
|-----|------|------|-----|
| `user:behavior:profile:{userId}` | Hash | 实时画像（1h/24h 字段合并写入） | 无 |
| `user:highvalue:rank` | ZSet | 高价值用户排名（score=24h 消费额） | 无 |
| `user:coupon:push:{idempotentId}` | String | 优惠券推送幂等 SETNX | 24h |
| `user:coupon:cooldown:{userId}:{ruleId}` | String | 触达冷却 SETNX | rule.cooldownSeconds |

`idempotentId = IdempotentKeyGenerator.of(userId, ruleId, matchedAt)`，即幂等键天然含"同一用户同一规则同一命中时刻"去重。

## 7. 策略决策流程

`StrategyDecisionService.decide(match)`：

1. `PatternRuleMatcher.match`：规则必须 enabled、patternName 匹配（`*` 通配）、audienceLevels 为空或包含 userLevel；多条命中取 priority 最大
2. `CouponSelector.selectCoupon`：返回规则的券模板
3. `CooldownChecker.tryAcquire(userId, ruleId, cooldownSeconds)`：原子占用冷却窗口，未占到 → 放弃
4. 组装 `CouponPushCommand`（幂等键见上）

内置规则（`DefaultRules`）：

| 规则 ID | 模式 | 券 | 类型 | 冷却 | 优先级 |
|---------|------|----|------|------|--------|
| `rule-cart-abandon` | `CART_NO_PAY_TIMEOUT` | `CPN_CART_10` | CASH | 3600s | 100 |
| `rule-view-cart-order` | `VIEW_CART_ORDER` | `CPN_ORDER_20` | DISCOUNT | 7200s | 90 |

## 8. 关键设计决策与理由

- **strategy 不依赖 Flink**：营销规则频繁变更，规则引擎必须能用表驱动 JUnit 独立验证；Flink 侧只依赖 `StrategyDecisionApi` 接口。
- **CEP 与 KeyedProcessFunction 并存**：CEP 适合固定模式（VIEW→CART→ORDER），定时器方案适合动态序列与自定义状态；按规则复杂度路由，`BehaviorSequenceFunction` 输出可接动态规则。
- **事件时间语义**：水位线乱序 5s + 空闲分区 1min，窗口与定时器均用 event time；时间戳合理性由 Normalizer 兜底（未来偏移 >5min 或 >7 天前判脏）。
- **幂等三层**：策略层冷却 SETNX → 触达层幂等键 SETNX → 优惠券服务自身幂等。任一层 Redis 故障均降级为放行（at-least-once），由下游幂等兜底。
- **规则广播流**：`MapStateDescriptor(STRING, POJO(MarketingRule))`；`enabled=false` 的事件即删除规则，改规则无需重启作业。
- **flink-connector-base 显式声明**：connector-kafka 3.1.0-1.18 将其声明为 provided（不传递），sink 模块直接引用 `DeliveryGuarantee`，必须在自身 POM 声明，否则编译失败（见 `pom.xml` 注释）。

## 9. 扩展点

| 需求 | 改动位置 |
|------|---------|
| 新检测模式 | `CepPatterns` 加常量 + flink-job 新增检测函数，输出同名 patternName |
| 新营销规则 | 广播 topic 发 `MarketingRule` JSON（或改 `DefaultRules`）；无需改代码 |
| 券库存/预算 | 实现 `CouponSelector` 新实现替换 `DefaultCouponSelector` |
| 优惠券推送渠道 | `coupon.push.mode` 加分支，或实现新的 sink |
| 规则持久化 | 实现 `RuleRepository`（当前仅 `InMemoryRuleRepository`），接 DB/配置中心 |
| 动态序列规则 | 消费 `BehaviorSequenceFunction` 输出的 `UserBehaviorState` 自行匹配 |

## 10. 已知边界

- `DedupSink` 目前是独立工具类，未接入主拓扑（`CouponPushSink` 内部已含幂等逻辑）
- `DefaultCouponSelector` 不检查券库存/预算
- 消费金额用 `double` 累加（`SpendAccumulator`），对营销画像够用，若做对账需改为 long 分单位
- 广播规则在作业重启后从 topic 重新消费（LATEST 起点意味着重启后需重发全量规则，或改为 EARLIEST/持久化）
