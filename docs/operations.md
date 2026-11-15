# 运维 SOP

> 面向日常运维与值班。所有 Kafka 命令基于 Windows 安装目录 `D:\develops\kafka\kafka_2.13-4.4.0`（KRaft 模式，下文记为 `%KAFKA%`）。**该 broker 由多个工具共用，禁止重启 broker 或执行 storage format，仅做 topic / 消费组级别的操作。**

```bat
set KAFKA=D:\develops\kafka\kafka_2.13-4.4.0
set BS=localhost:9092
```

## 1. 日常巡检（每日）

| 检查项 | 命令/位置 | 健康标准 |
|--------|----------|---------|
| Flink 作业状态 | Flink Web UI 或 `flink list` | RUNNING，无 restart 堆积 |
| checkpoint 成功率 | Flink UI → Checkpoints | 最近 10 次全部 Completed |
| 消费组积压 | `%KAFKA%\bin\windows\kafka-consumer-groups.bat --bootstrap-server %BS% --describe --group user-behavior-marketing` | LAG≈0；持续增长需处理 |
| 脏数据量 | 作业控制台 `dirty-behavior` 输出 / 日志 | 偶发即可；突增说明埋点协议变更 |
| Redis 画像新鲜度 | `redis-cli HGET user:behavior:profile:<uid> updatedAt` | 距今 < 窗口滑动步长(5min)×2 |
| 触达 topic 消费 | 触达系统侧确认 `user-coupon-push-commands` 消费正常 | LAG≈0 |

积压查看示例输出关注 `CURRENT-OFFSET / LOG-END-OFFSET / LAG` 三列。

## 2. 消费组与 topic 管理

```bat
:: 查看/重置位移（谨慎：重置会造成重复或丢失消费）
%KAFKA%\bin\windows\kafka-consumer-groups.bat --bootstrap-server %BS% --list
%KAFKA%\bin\windows\kafka-consumer-groups.bat --bootstrap-server %BS% --describe --group user-behavior-marketing

:: 重置到最早（必须先停作业）
%KAFKA%\bin\windows\kafka-consumer-groups.bat --bootstrap-server %BS% \
  --group user-behavior-marketing --reset-offsets --to-earliest --topic user-behavior-events --execute

:: topic 消息量 / 最新位移
%KAFKA%\bin\windows\kafka-get-offsets.bat --bootstrap-server %BS% --topic user-behavior-events
```

## 3. 规则热更新（`rules.broadcast.enabled=true` 时）

向 `user-marketing-rules` 发一条 `MarketingRule` JSON 即生效，**无需重启作业**。JSON 字段与 `MarketingRule` POJO 一致：

```bat
%KAFKA%\bin\windows\kafka-console-producer.bat --bootstrap-server %BS% --topic user-marketing-rules
```

上线规则（新增/修改都发同 ID 全量 JSON）：

```json
{"id":"rule-cart-abandon","name":"购物车未支付挽回","patternName":"CART_NO_PAY_TIMEOUT","couponTemplateId":"CPN_CART_10","couponType":"CASH","cooldownSeconds":3600,"audienceLevels":[],"enabled":true,"priority":100}
```

下线规则：**同 ID 发 `enabled:false`**（广播函数收到即从状态中删除）：

```json
{"id":"rule-cart-abandon","enabled":false}
```

注意：
- 广播函数按 `id` 覆盖/删除；`id` 为空的 JSON 会被忽略
- 规则源从 LATEST 消费，**作业重启后广播状态为空**，需重发全量启用中的规则
- `audienceLevels` 取值：`NORMAL/SILVER/GOLD/PLATINUM`，空数组 = 全人群
- 验证规则已生效：观察命中后触达 topic 的 `ruleId`/`couponId` 字段

## 4. 常见故障处理

### 4.1 消费积压

1. 确认作业 RUNNING、checkpoint 正常
2. 查 topic 分区数与作业并行度：并行度 ≤ 分区数，否则扩分区或调 `job.parallelism` 重启
3. 短时流量尖峰可等待自愈；持续积压按上述扩容后用 `--reset-offsets` 不需要——Flink 从 checkpoint 位移继续
4. 仍积压：dump 线程（Flink UI → Task Manager → Thread Dump）定位是否卡在 sink（常见为 Redis/HTTP 慢）

### 4.2 Redis 不可用

各组件的降级行为（均为设计预期，不是故障放大）：

| 组件 | 行为 |
|------|------|
| `RedisCooldownChecker` | 记 WARN，放行推送（由优惠券服务幂等兜底） |
| `CouponPushSink` 幂等 | 记 WARN，跳过去重直接推送 |
| `DedupSink` | 本地缓冲 + 放行；恢复后 `drainLocalBuffer` 走重试/死信 |
| `RedisProfileSink` | 记 ERROR，该条画像丢弃，下一窗口自动重写，无数据损坏 |

处理：恢复 Redis 后无需重启作业；若画像需立即补齐，重放对应时间段事件（见 §5）。

### 4.3 优惠券服务 5xx / 超时

- `CouponPushSink` 会**释放幂等键**，Flink checkpoint 重启后该命令会重推（at-least-once）
- 观察日志 `Coupon service returned <code>`；若大面积失败先熔断上游（把 `coupon.push.mode` 切 `kafka` 重启，命令转 topic 由触达系统削峰）

### 4.4 checkpoint 失败

1. Flink UI 看 failure cause：常见为 state 尺寸激增（广播规则量过大）或 TaskManager 内存不足
2. 广播状态只应保存启用中的规则，检查是否误发大量历史规则
3. 必要时调大 TM 内存或 `job.checkpoint.interval.ms`

### 4.5 坏消息不断流

- 反序列化失败：`UserBehaviorEventDeserializationSchema` 记 WARN 并跳过，属预期
- 字段级脏数据：进 side output `dirty-behavior` 打印；突增时优先排查埋点发布新版本

## 5. 数据重放（修复用）

画像或券漏发时的重放流程：

```bat
:: 1. 停作业（保留 checkpoint）
flink stop <jobId>

:: 2. 把位移重置到目标时间（示例：重置到最早）
%KAFKA%\bin\windows\kafka-consumer-groups.bat --bootstrap-server %BS% \
  --group user-behavior-marketing --reset-offsets --to-earliest --topic user-behavior-events --execute

:: 3. 重启作业，观察 Redis 画像 / 触达 topic 是否补齐
```

窗口聚合基于 event time + 水位线，重放旧事件可正确重算窗口；冷却/幂等 SETNX 会拦截重复触达。

## 6. Redis 数据维护

```bash
# 查看某用户画像
redis-cli HGETALL user:behavior:profile:u1

# 高价值排名（分数=24h 消费额）
redis-cli ZREVRANGE user:highvalue:rank 0 9 WITHSCORES

# 清理画像（重建用，不影响幂等/冷却）
redis-cli --scan --pattern "user:behavior:profile:*" | xargs -n1 redis-cli DEL
redis-cli DEL user:highvalue:rank
```

## 7. 发布/回滚清单

发布前：topic 就绪 → Redis 连通 → 优惠券服务可达（或切 kafka 模式）→ 确认消费组位移策略。
发布中：`flink run` 后观察 UI 10 分钟，重点 checkpoint 与 dirty 输出。
回滚：见 `deployment.md` §7；规则回滚优先用广播下线，不重启作业。
