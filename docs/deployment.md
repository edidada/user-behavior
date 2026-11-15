# 部署

> 构建产物只有一个 fat-jar；Flink 核心（streaming-java / clients / connector-base / cep）为 provided，由集群提供。配置全部可命令行覆盖，一次构建多环境部署。

## 1. 环境要求

| 组件 | 版本 | 说明 |
|------|------|------|
| JDK | 17 | 编译与运行 |
| Maven | 3.9+ | 构建 |
| Flink | 1.18.x | 集群/本地运行时，需自带 `flink-streaming-java`、`flink-clients`、`flink-cep`、`flink-connector-base`（flink-dist 默认包含） |
| Kafka | 3.x / 4.x | 已验证 4.4（KRaft）；Flink connector 3.1.0-1.18 自带 kafka-clients 3.2.x，协议向下兼容 |
| Redis | 5+ | 画像 / 高价值排名 / 幂等 / 冷却（冷却可用进程内模式替代） |

> 注意：fat-jar shade 时排除了 `flink-streaming-java` 与 `flink-clients`，但**包含** connector-kafka 与 jedis。若集群 lib 缺少 `flink-connector-base`，Kafka sink 模式启动会报 `NoClassDefFoundError: .../DeliveryGuarantee`，此时把 `behavior-sink/pom.xml` 中该依赖的 `provided` 改为 compile 重新打包即可。

## 2. 构建

```bash
# 全量构建 + 单测
mvn clean verify

# 只出 fat-jar
mvn clean package -DskipTests

# 产物
behavior-app/target/behavior-app-1.0.0-SNAPSHOT.jar
```

CI（`.github/workflows/ci.yml`）在 push/PR 时执行 `mvn -B -ntp verify` 并上传 fat-jar 为构建产物 `behavior-app`。

## 3. 提交到 Flink 集群

使用提交脚本（内部即 `flink run`）：

```bash
cd behavior-app/scripts
./submit-job.sh \
  --kafka.brokers=kafka:9092 \
  --redis.host=redis \
  --coupon.endpoint=http://coupon-service/api/coupons/push
```

或直接：

```bash
flink run -c org.example.behavior.app.UserBehaviorMarketingJob \
  behavior-app/target/behavior-app-1.0.0-SNAPSHOT.jar \
  --kafka.brokers=kafka:9092 \
  --redis.host=redis \
  --coupon.endpoint=http://coupon-service/api/coupons/push
```

带 savepoint 恢复（升级发版）：

```bash
# 停旧作业并保留状态
flink stop --savepointPath hdfs:///flink/savepoints <jobId>
# 从 savepoint 启动
flink run -s hdfs:///flink/savepoints/savepoint-xxxx -c org.example.behavior.app.UserBehaviorMarketingJob \
  behavior-app/target/behavior-app-1.0.0-SNAPSHOT.jar ...
```

## 4. 配置矩阵

优先级：**命令行 `--key=value` > jar 内 `application.properties`**（`JobConfig` 实现）。

| 配置 | 默认值 | 说明 |
|------|--------|------|
| `kafka.brokers` | `localhost:9092` | Kafka 集群地址 |
| `kafka.topic` | `user-behavior-events` | 行为事件 topic |
| `kafka.group.id` | `user-behavior-marketing` | 消费组（规则源追加 `-rules` 后缀） |
| `kafka.rule.topic` | `user-marketing-rules` | 规则广播 topic |
| `kafka.coupon.topic` | `user-coupon-push-commands` | Kafka 触达 topic |
| `rules.broadcast.enabled` | `false` | `true` 时从 Kafka 广播加载规则，改规则不重启作业 |
| `cooldown.redis.enabled` | `false` | `true` 用 Redis SETNX 全局冷却；`false` 进程内冷却（多并行度下不严格） |
| `redis.host` / `redis.port` | `localhost` / `6379` | Redis 地址 |
| `coupon.push.mode` | `http` | `http` 调 REST API；`kafka` 发到触达 topic |
| `coupon.endpoint` | `http://localhost:8080/api/coupons/push` | http 模式的优惠券服务地址 |
| `cep.within.minutes` | `30` | CEP 模式时间窗 |
| `cart.timeout.minutes` | `30` | 加购未支付定时器超时 |
| `job.parallelism` | `2` | 作业并行度 |
| `job.checkpoint.interval.ms` | `60000` | checkpoint 间隔 |

## 5. 上线前置检查

- [ ] 三个 topic 已创建（见 `reproduce.md` §2）：`user-behavior-events`、`user-coupon-push-commands`，开广播再加 `user-marketing-rules`
- [ ] Redis 可连通且持久化策略明确（画像数据可接受丢失重建）
- [ ] 优惠券服务可用，或确认使用 `coupon.push.mode=kafka`
- [ ] 首次启动消费组无历史位移：源以 `committedOffsets(EARLIEST)` 兜底；**升级重启时注意消费组已有 checkpoint 位移，不会重复消费**
- [ ] 若开广播规则：重启后规则源从 LATEST 消费，**需在启动后重发全量规则 JSON**（见 `operations.md` §3）

## 6. 部署形态

### 6.1 单作业（当前实现）

两条链路在同一个作业里，共享 checkpoint。适合初期与中小流量。

### 6.2 双作业拆分（演进）

链路 A（检测+触达）与链路 B（画像聚合）可各自打一个 main（或用参数开关选择链路）独立提交：

- 互不影响，按流量独立扩缩容
- 链路 B 无状态依赖 Kafka 位移，可随时重启；链路 A 有序列/定时器状态，建议 savepoint 升级
- 拆分时 `job.parallelism` 可分别设置（链路 B 通常更吃并行度）

## 7. 回滚

- 代码回滚：重新部署上一个版本的 fat-jar；带 savepoint 时注意 POJO 结构变更需保证状态兼容（本项目状态类均为简单 POJO + `Types.POJO` 显式声明）
- 配置回滚：用旧的命令行参数重启即可，无额外操作
- 规则回滚：向 `user-marketing-rules` 发一条 `enabled=false` 的规则 JSON 即可下线（广播模式），或改回 `DefaultRules` 重启（静态模式）
- Redis 数据回滚：`user:behavior:profile:*` 与 `user:highvalue:rank` 可直接删除，由窗口重算恢复；幂等/冷却键不建议批量清理（避免重复触达）
