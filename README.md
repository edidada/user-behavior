# README
基于用户实时行为（浏览、加购、下单）动态调整策略。Flink 利用 KeyedProcessFunction 或 CEP 库检测行为模式（如“浏览-加购-未支付”），在用户离开前实时推送优惠券；或实时聚合消费金额，识别 top 高价值用户并写入 Redis 供前端调用。 maven多模块工程

针对“用户实时行为分析与动态营销”这个场景，Maven 多模块设计的关键在于：**行为事件采集、模式检测、策略决策、触达执行**四层解耦。与库存场景不同，这里更强调**状态管理**（用户行为序列）和**规则可配置**（营销策略频繁变更）。下面给出可直接落地的方案。

---

## 一、模块划分总览

```
user-behavior-parent (pom)
├── behavior-common            公共模块：常量、枚举、工具、序列化
├── behavior-domain            领域模型：行为事件、用户画像、策略规则、触达指令
├── behavior-source            数据接入：Kafka Source、行为规范化、脏数据过滤
├── behavior-flink-job         核心作业：CEP/ProcessFunction 模式检测 + 聚合
├── behavior-strategy          策略引擎：规则解析、决策、优惠券选品
├── behavior-sink              触达执行：Redis 写入、消息推送、优惠券 API 调用
└── behavior-app               启动模块：组装作业、fat-jar、提交脚本
```

> 中小团队可压缩为 `common / domain / job / app` 四模块，`source/sink/strategy` 先并入 `job`，通过包隔离，后续再拆。

---

## 二、各模块职责与依赖

### 1. `behavior-common`（无业务依赖）
- 常量：Kafka topic、Redis key 前缀（`user:behavior:*`、`user:highvalue:*`）、CEP 模式名
- 枚举：`BehaviorType`（VIEW/CART/ORDER/PAY）、`UserLevel`、`CouponType`、`TriggerChannel`
- 工具：事件时间解析、JSON 封装、用户 ID 脱敏、幂等 key 生成
- 通用异常

**依赖**：仅 Jackson / Lombok / SLF4J，不依赖 Flink。

### 2. `behavior-domain`（领域模型，纯 POJO）
- 输入事件：`UserBehaviorEvent`（userId、skuId、behaviorType、timestamp、sessionId、channel）
- 状态对象：`UserBehaviorState`（最近 N 条行为序列、最后活跃时间、加购未支付计数）
- 聚合结果：`UserValueProfile`（userId、近 1h/24h 消费额、订单数、客单价、等级）
- 策略对象：`MarketingRule`（规则 ID、模式表达式、优惠券模板、冷却时间、生效人群）
- 输出指令：`CouponPushCommand`（userId、couponId、触发原因、渠道、幂等 ID）

**依赖**：`behavior-common` + Lombok。保持纯 POJO，便于单测与跨模块复用。

### 3. `behavior-source`（数据接入层）
- `BehaviorKafkaSource`：`KafkaSource<UserBehaviorEvent>` 构建
- `BehaviorNormalizer`：字段补全、时间戳校正、非法事件过滤（`ProcessFunction`）
- `WatermarkStrategy` 封装：有界乱序 + 空闲分区处理
- 埋点协议版本兼容（v1/v2 事件映射）

**依赖**：`behavior-domain`、flink-connector-kafka。**不含业务逻辑**，只做“干净事件流”。

### 4. `behavior-flink-job`（核心计算）
两条并行链路：

**链路 A：行为模式检测（CEP / KeyedProcessFunction）**
- `keyBy(userId)` → `CEP.pattern()` 检测“浏览→加购→未支付(超时)”序列
- 或 `KeyedProcessFunction` + `ValueState<List<Behavior>>` 手动维护序列（更灵活、易调参）
- 定时器：加购后 T 分钟未支付触发（`onTimer`）
- 输出 `PatternMatchResult` 给策略模块

**链路 B：高价值用户实时聚合**
- `keyBy(userId)` → 滑动窗口（1h/24h）`AggregateFunction` 累加消费额
- `ProcessWindowFunction` 计算客单价、等级
- 输出 `UserValueProfile` 写 Redis（供前端实时查询）

**依赖**：`behavior-domain`、`behavior-source`、`behavior-strategy`（接口）、Flink streaming/java、cep。

### 5. `behavior-strategy`（策略引擎，不依赖 Flink）
- `RuleRepository` 接口：加载 `MarketingRule`（DB/配置中心/广播流）
- `PatternRuleMatcher`：模式匹配结果 → 命中规则
- `CouponSelector`：按人群、库存、预算选优惠券
- `CooldownChecker`：基于 Redis 的触达冷却（防骚扰）
- `StrategyDecisionService`：编排以上，输出 `CouponPushCommand`

**依赖**：`behavior-domain`、`behavior-common`。**纯 Java，可 JUnit 全覆盖**，这是本设计的最大价值点——营销规则频繁变，必须能独立测试。

### 6. `behavior-sink`（触达执行层）
- `RedisProfileSink`：`UserValueProfile` 写 Redis（`RichSinkFunction` 或异步）
- `CouponPushSink`：调用优惠券服务 API / 发 Kafka 给触达系统
- `DedupSink`：基于幂等 ID 去重（Redis SETNX）
- 失败重试与降级（本地缓冲 + 死信队列）

**依赖**：`behavior-domain`、Redis client、HTTP client。

### 7. `behavior-app`（启动与打包）
- `UserBehaviorMarketingJob` main：组装 Source → 双链路 → Strategy → Sink
- 外置配置（`application.properties` / ConfigMap）
- Shade 打 fat-jar（Flink 核心 `provided`）
- 提交脚本

**依赖**：`behavior-flink-job`、`behavior-strategy`、`behavior-sink`。

---

## 三、依赖关系图

```
behavior-common ← behavior-domain ← behavior-strategy
                        ↑                  ↑
                behavior-source            |
                        ↑                  |
                behavior-flink-job ────────┘
                        ↑
                behavior-sink
                        ↑
                behavior-app（聚合所有）
```

关键约束：
- `strategy` **不依赖 Flink**，保证规则引擎可独立单测
- `source/sink` 只依赖 `domain`，不互相依赖
- `app` 是唯一打包入口

---

## 四、关键 POM 设计

**父 POM 统一版本**：
```xml
<properties>
  <flink.version>1.18.1</flink.version>
  <java.version>17</java.version>
  <jedis.version>5.1.0</jedis.version>
</properties>
```

**Flink 核心依赖 `provided`**（集群提供）：
```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-streaming-java</artifactId>
  <version>${flink.version}</version>
  <scope>provided</scope>
</dependency>
<!-- CEP 需显式引入，同样 provided -->
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-cep</artifactId>
  <version>${flink.version}</version>
  <scope>provided</scope>
</dependency>
```

**`behavior-strategy` 刻意不引 Flink**，只依赖 domain + Redis client（冷却检查）。

**`behavior-app` Shade 排除 Flink 核心**，只打业务模块 + 连接器 + Redis client。

---

## 五、模块间解耦的两个关键设计

### 1. 策略用接口隔离 Flink
`behavior-flink-job` 只依赖 `StrategyDecisionService` 接口（定义在 `behavior-strategy` 的 api 包），实现通过构造注入。作业逻辑可用 Mock 单测，规则变更不动作业代码。

### 2. 规则配置走广播流
`MarketingRule` 通过 `BroadcastProcessFunction` 或配置中心实时下发，避免改规则重启作业。`RuleRepository` 接口在 strategy，广播流实现放 job 模块，保持 strategy 纯净。

---

## 六、两条链路的模块内实现要点

### 链路 A：CEP 模式检测
```java
Pattern<UserBehaviorEvent, ?> pattern = Pattern
  .<UserBehaviorEvent>begin("view").where(e -> e.getType() == VIEW)
  .next("cart").where(e -> e.getType() == CART)
  .next("noPay").where(e -> e.getType() == ORDER)
  .within(Time.minutes(30));
```
配合 `onTimer` 处理“加购后未支付”超时场景。CEP 适合固定模式，`KeyedProcessFunction` 适合动态序列——建议**两者都保留**，按规则复杂度路由。

### 链路 B：高价值用户聚合
```java
keyBy(userId)
  .window(SlidingEventTimeWindows.of(Time.hours(24), Time.minutes(5)))
  .aggregate(new SpendAggregate(), new ProfileWindowFunction())
  .addSink(new RedisProfileSink());
```
Redis 结构建议：`Hash` 存画像字段，`ZSet` 存高价值用户排名供前端分页。

---

## 七、测试与部署

| 模块 | 测试方式 |
|------|---------|
| common / domain | 纯 JUnit |
| strategy | JUnit + Mockito，规则表驱动测试（核心） |
| flink-job | Flink `MiniCluster` + `TestHarness` 测 CEP/定时器 |
| source / sink | 嵌入式 Kafka / 本地 Redis 集成测试 |
| app | `LocalStreamEnvironment` 端到端跑通 |

**部署**：`behavior-app` 产出单 fat-jar，配置外置，一次构建多环境部署。两条链路可拆成两个 job 独立提交，按流量弹性扩缩容。

---

## 八、演进建议

后续若扩展推荐、实时数仓，建议再拆：
- `behavior-flink-common`：通用 Source/Sink、水印策略、状态工具
- 按场景建多个 job 模块（`job-marketing`、`job-recommend`、`job-profile`），共享 `flink-common` + `domain`

从“单场景多模块”平滑演进到“实时计算平台”，避免后期大重构。

---

## 九、构建与运行

**环境**：JDK 17、Maven 3.9+。

```bash
# 全量构建 + 单元测试（common / strategy 的纯 JUnit 测试）
mvn clean verify

# 只打 behavior-app 的 fat-jar
mvn clean package -DskipTests

# 产物
behavior-app/target/behavior-app-1.0.0-SNAPSHOT.jar
```

**提交作业**（Flink 集群已提供 `flink-streaming-java` / `flink-cep`）：

```bash
flink run -c org.example.behavior.app.UserBehaviorMarketingJob \
  behavior-app/target/behavior-app-1.0.0-SNAPSHOT.jar \
  --kafka.brokers=kafka:9092 \
  --redis.host=redis \
  --coupon.endpoint=http://coupon-service/api/coupons/push
```

配置优先级：命令行 `--key=value` > `behavior-app/src/main/resources/application.properties`。

**CI**：`.github/workflows/ci.yml` 在 push / PR 时执行 `mvn verify`，并把 fat-jar 作为构建产物上传。
