# 如何复现本项目

> 目标：在本地把"造行为事件 → Flink 检测/聚合 → 优惠券指令 → Redis 画像"整条链路跑通并亲眼验证。
> 本机环境现状：**Kafka 4.4 已安装且 broker 正在运行**（`D:\develops\kafka\kafka_2.13-4.4.0`，KRaft 模式，监听 9092），**Redis 已在 6379 运行**。该 Kafka 由多个工具共用，**不要重启 broker、不要重新 format**，只需创建本项目专属 topic。

## 1. 环境与工具确认

| 项 | 现状 | 复现要求 |
|----|------|---------|
| JDK 17 | 需已安装 | 编译运行 Flink 作业 |
| Maven 3.9+ | 需已安装 | `mvn verify` 构建 |
| Kafka 4.4 | `D:\develops\kafka\kafka_2.13-4.4.0`，broker 运行中（KRaft，无需 ZooKeeper） | 直接连 `localhost:9092`，只建 topic |
| Redis | 127.0.0.1:6379 运行中 | 直接使用；画像/幂等/冷却都写它 |
| MySQL 8.4 | 127.0.0.1:3306 运行中（可选） | `rules.repository=mysql` 时使用，见 §10 |
| IDE | IntelliJ IDEA | 运行 main（provided 依赖在 IDE 运行类路径中可用） |

快速自检：

```bat
set KAFKA=D:\develops\kafka\kafka_2.13-4.4.0
set BS=localhost:9092

:: broker 可用性
%KAFKA%\bin\windows\kafka-broker-api-versions.bat --bootstrap-server %BS% | findstr "4.4"
:: Redis 可用性
redis-cli ping
```

## 2. 创建本项目专属 topic（共用 broker，不动其他工具）

```bat
set KAFKA=D:\develops\kafka\kafka_2.13-4.4.0
set BS=localhost:9092

%KAFKA%\bin\windows\kafka-topics.bat --bootstrap-server %BS% --create --topic user-behavior-events --partitions 3 --replication-factor 1
%KAFKA%\bin\windows\kafka-topics.bat --bootstrap-server %BS% --create --topic user-coupon-push-commands --partitions 1 --replication-factor 1
:: 可选：规则热更新演示
%KAFKA%\bin\windows\kafka-topics.bat --bootstrap-server %BS% --create --topic user-marketing-rules --partitions 1 --replication-factor 1

%KAFKA%\bin\windows\kafka-topics.bat --bootstrap-server %BS% --list
```

> 命名带 `user-behavior/user-coupon/user-marketing` 前缀，与其他工具的 topic 天然隔离；消费组 `user-behavior-marketing` 也是专属的。

## 3. 构建与启动作业

```bash
mvn clean verify
```

然后在 IDE 中运行 `behavior-app/src/main/java/org/example/behavior/app/UserBehaviorMarketingJob` 的 main。本地复现建议用以下 Program arguments 覆盖默认值：

```
--cart.timeout.minutes=1
--coupon.push.mode=kafka
```

- `cart.timeout.minutes=1`：把"加购未支付"超时从 30 分钟缩到 1 分钟，1 分钟内即可看到触发
- `coupon.push.mode=kafka`：本地没有优惠券服务，把触达指令发到 `user-coupon-push-commands`，用 console consumer 直接观察（默认 http 会去打 8080 报错）

> 若想完整验证 Redis 冷却链路再加 `--cooldown.redis.enabled=true`。

### 3.1 无 IDE：裸 `java -cp` 运行（两个必踩的坑）

本地没有 IDE 时，用 test classpath（含 provided 的 Flink 核心）直接跑 main。**两个坑**：

1. **JDK 17 必须 `--add-opens`**：Flink 的 Kryo/Chill 序列化器首次使用时反射注册集合序列化器，缺 `--add-opens java.base/java.util=ALL-UNNAMED` 会在第一次输出匹配结果（跨网络序列化）时抛 `InaccessibleObjectException`，作业陷入静默重启循环（Flink 发行版的启动脚本会自动加这些参数，裸 java 不会）。
2. **SLF4J 无后端 = 全程静默**：工程只依赖 `slf4j-api`，本地运行时挂上 log4j2（本地仓库有 2.17.2），否则出问题连堆栈都看不到。

完整启动命令（Git Bash，路径按需替换）：

```bash
REPO=D:/develops/maven/mavenrepository
export MYSQL_PASSWORD=...   # rules.repository=mysql 时需要
CP="behavior-app/target/classes;$REPO/org/apache/logging/log4j/log4j-core/2.17.2/log4j-core-2.17.2.jar;$REPO/org/apache/logging/log4j/log4j-api/2.17.2/log4j-api-2.17.2.jar;$REPO/org/apache/logging/log4j/log4j-slf4j-impl/2.17.2/log4j-slf4j-impl-2.17.2.jar;$(cat behavior-app/target/cp.txt)"
java --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED \
     --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
     --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED \
     -cp "$CP" org.example.behavior.app.UserBehaviorMarketingJob ...
```

其中 `cp.txt` 由 `mvn -pl behavior-app dependency:build-classpath -Dmdep.outputFile=target/cp.txt -Dmdep.classpathScope=test` 生成（test scope 才会把 provided 的 Flink 核心带进来）。

排障时的诊断技巧（本次实战验证有效）：
- 消费组"无活跃成员"但位移已提交 = 作业在重启循环，去日志找 `switched from RUNNING to FAILED`
- `jstack <pid>` 看算子线程是否阻塞；`jps -l` 确认没有旧作业僵尸进程残留

## 4. 造事件并验证链路 A（模式检测 → 优惠券）

开两个窗口，一个消费触达指令，一个生产事件：

```bat
:: 窗口 1：观察优惠券指令
%KAFKA%\bin\windows\kafka-console-consumer.bat --bootstrap-server %BS% --topic user-coupon-push-commands --from-beginning

:: 窗口 2：生产行为事件（每行一条 JSON）
%KAFKA%\bin\windows\kafka-console-producer.bat --bootstrap-server %BS% --topic user-behavior-events
```

### 场景 1：浏览→加购→下单（触发 `rule-view-cart-order`）

依次粘贴（时间戳自行换成当前毫秒值，可用 `powershell [DateTimeOffset]::Now.ToUnixTimeMilliseconds()`）：

```json
{"eventId":"e1","userId":"u1","skuId":"sku-9","behaviorType":"VIEW","timestamp":1765500000000,"channel":"app"}
{"eventId":"e2","userId":"u1","skuId":"sku-9","behaviorType":"CART","timestamp":1765500001000,"channel":"app"}
{"eventId":"e3","userId":"u1","skuId":"sku-9","behaviorType":"ORDER","timestamp":1765500002000,"channel":"app"}
```

**预期**（窗口 1 出现）：

```json
{"userId":"u1","ruleId":"rule-view-cart-order","couponId":"CPN_ORDER_20","couponType":"DISCOUNT","triggerReason":"VIEW_CART_ORDER","channel":"APP_PUSH","idempotentId":"...","createdAt":...}
```

### 场景 2：加购后不支付，1 分钟超时（触发 `rule-cart-abandon`）

只发一条 CART（新用户 u2）：

```json
{"eventId":"e4","userId":"u2","skuId":"sku-1","behaviorType":"CART","timestamp":1765500100000,"channel":"app"}
```

**预期**：约 1 分钟后（水位线推进过 `timestamp + 60s`）窗口 1 出现 `ruleId=rule-cart-abandon, couponId=CPN_CART_10, triggerReason=CART_NO_PAY_TIMEOUT`。若没出现，多发几条未来时间戳的 VIEW 推进水位线即可。

> 提示：console producer 发出的行即一条消息；timestamp 必须是"接近当前时间"的毫秒值（Normalizer 会把 >7 天前或未来 5 分钟以上的判为脏数据）。

### 同规则冷却验证

同一用户重复触发同一规则：第二次推送会被冷却拦截（默认进程内冷却即可演示，3600s 内只推一次）。

## 5. 造事件并验证链路 B（消费画像 → Redis）

```bash
redis-cli
```

生产端给用户 u1 连续支付（金额累加跨过等级门槛：SILVER 1000 / GOLD 10000 / PLATINUM 50000）：

```json
{"eventId":"e5","userId":"u1","skuId":"sku-9","behaviorType":"PAY","timestamp":1765500200000,"amount":"200.5","channel":"app"}
{"eventId":"e6","userId":"u1","skuId":"sku-3","behaviorType":"PAY","timestamp":1765500201000,"amount":"1500","channel":"app"}
```

**预期**（窗口滑动步长 5 分钟，等窗口触发后执行）：

```bash
redis-cli HGETALL user:behavior:profile:u1
# amount24h=1700.5  payCount24h=2  orderCount24h=0  avgOrderAmount=850.25  userLevel=SILVER ...

redis-cli ZREVRANGE user:highvalue:rank 0 -1 WITHSCORES
# u1  1700.5
```

## 6. 验证规则热更新（可选进阶）

1. IDE 运行配置里加 `--rules.broadcast.enabled=true` 重启作业
2. 向 `user-marketing-rules` 发规则 JSON（`kafka-console-producer.bat --topic user-marketing-rules`）：

```json
{"id":"rule-cart-abandon","name":"购物车未支付挽回","patternName":"CART_NO_PAY_TIMEOUT","couponTemplateId":"CPN_CART_99","couponType":"CASH","cooldownSeconds":3600,"audienceLevels":[],"enabled":true,"priority":100}
```

3. 重复场景 2，观察窗口 1 中 `couponId` 已变为 `CPN_CART_99` —— 全程无需重启作业
4. 发 `{"id":"rule-cart-abandon","enabled":false}` 下线规则，场景 2 不再触发

## 7. 本地快速排错

| 现象 | 排查 |
|------|------|
| 作业启动连不上 Kafka | 确认 9092 在监听（`netstat -ano | findstr :9092`）；共型 broker 不允许重启 |
| 事件没产生任何输出 | 控制台看 `dirty-behavior` 侧输出——多半是 timestamp 不合理或缺 skuId |
| 触发过一次后再也不触发 | 冷却生效（3600s/7200s）；换 userId 或清 `user:coupon:cooldown:*` |
| 窗口画像迟迟不出 | 水位线没推进：再发一条时间戳更新的事件即可（窗口按 event time 触发） |
| 重新跑想从头消费 | 消费组已有 checkpoint 位移；改 `--kafka.group.id=u1-test-2`，或停作业后 `kafka-consumer-groups.bat --reset-offsets --to-earliest --topic user-behavior-events --execute` |
| IDE 运行报找不到 Flink 类 | 确认用的是 `behavior-app` 模块的 main；`flink-clients` 是 provided，IDE 默认包含，若用 `mvn exec` 需临时改 compile |
| 控制台工具"找不到主类" | Kafka 4.x 类名变了：消费组命令是 `org.apache.kafka.tools.consumer.group.ConsumerGroupCommand`，console consumer 是 `org.apache.kafka.tools.consumer.ConsoleConsumer`；且别把 stderr 丢掉 |

## 8. 跑单元测试（无需外部依赖）

```bash
mvn verify
```

覆盖：common 工具、domain 状态、source 规范化与 v1/v2 映射、strategy 规则匹配/决策/冷却、flink-job 定时器与聚合（`SpendAggregateFunctionTest`）。

## 9. 提交到真实 Flink 集群

见 `deployment.md` §2–3（fat-jar + `flink run` 或 `behavior-app/scripts/submit-job.sh`）。

## 10. MySQL 8 规则库增删改（可选）

`MysqlRuleRepository` 实现了规则的增（insert）、删（delete）、改（update）、查（findAll/findById），strategy 模块保持零 Flink 依赖。

**建库**（凭据不落仓库，密码走环境变量）：

```bash
# 1. 设置用户环境变量（一次性，之后新开的终端都能读到）
setx MYSQL_PASSWORD "<你的密码>"

# 2. 建库建表 + 导入两条种子规则（脚本不含密码）
mysql -uroot -p < docs/mysql-schema.sql
```

**运行 CRUD 测试**（无 `MYSQL_PASSWORD` 时自动跳过，CI 安全）：

```bash
export MYSQL_PASSWORD=<你的密码>
mvn -pl behavior-strategy test -Dtest=MysqlRuleRepositoryTest
```

测试内容即完整增删改流程：insert → findById 校验 → 改券模板/优先级/人群 → 删除 → 确认 0 行；含 `audienceLevels` JSON 列往返和主键冲突校验。

**作业使用 MySQL 规则库**：启动参数加 `--rules.repository=mysql`（密码自动读 `MYSQL_PASSWORD`）。作业启动时 `findAll()` 加载全部启用规则；之后对表做增删改不会自动生效（静态模式仅启动时读一次）——运行期变更请走 §6 的广播模式。

> 手工对规则做增删改：直接用本机 `D:\develops\mysql-8.4.8-winx64\bin\mysql.exe -uroot -p` 进库执行 `INSERT/UPDATE/DELETE`，表结构见 `docs/mysql-schema.sql`；或调用 `MysqlRuleRepository` 的对应方法。
