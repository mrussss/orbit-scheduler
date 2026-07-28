# Orbit Scheduler 完整 Phase 开发文档（Go 后端求职增强版）

> 项目：Go、PostgreSQL 与 Kafka 的可靠分布式任务执行系统，并附带独立 MySQL 8 工程实验模块  
> 开发模式：Vibe Coding 完成参考实现 → AI 逐层讲解 → 从 Git 骨架检查点回滚 → 亲手复刻核心链路  
> 最终范围：本文所列功能全部属于最终范围，不以“最小 MVP”为目标；Phase 仅表示开发、验证和学习顺序。  
> 数据库边界：PostgreSQL 始终是 Orbit 生产主链路的唯一权威数据库；MySQL 8 仅用于独立工程实验、对比验证与求职能力证明，不参与生产双写。

---

## 0. 文档目的与使用方式

本文把 Orbit Scheduler 的完整设计拆成可执行的开发 Phase，并同时解决三个目标：

1. **工程目标**：尽快借助 AI 完成一套能够运行、测试、演示和写入简历的完整系统；
2. **学习目标**：不把项目变成“AI 写完但自己不会”，而是通过 Git 检查点，亲手复刻调度核心、Worker 生命周期、Outbox 和消费幂等等关键链路；
3. **求职目标**：让 Orbit 同时证明“主流 Go 业务后端能力”和“分布式可靠性能力”，并通过独立 MySQL 8 实验补齐高频数据库关键词，而不是再新建第三个大型商城或订单项目。

每一个包含核心链路的 Phase 都按以下固定循环推进：

```text
明确接口和验收标准
→ AI 生成非核心骨架
→ 提交并 Push“核心实现前”检查点
→ AI 完成参考实现
→ 测试通过并 Push“参考实现”检查点
→ AI 按调用链、事务边界、并发竞态进行讲解
→ 从“核心实现前”Tag 拉学习分支
→ 用户亲手实现核心链路
→ 运行同一套测试
→ 与 AI 参考实现进行 diff 和复盘
→ 合并或保留学习分支
```

### 0.1 三类代码的处理方式

#### A. 可以直接 Vibe Coding 的代码

包括：

- 配置加载；
- DTO、参数绑定和校验；
- GORM 普通 CRUD；
- Gin 路由注册；
- 日志、错误码、指标样板；
- Docker Compose；
- CI 配置；
- 测试辅助函数；
- Mock 数据；
- README 排版；
- Protobuf 生成代码；
- 非关键 Repository 包装代码。

这些内容需要理解用途和边界，但没有必要逐行手写。

#### B. 必须亲手复刻的核心链路

至少包括：

1. PostgreSQL 原子领取任务：`FetchTasks`；
2. Lease 续租与 Fencing 校验：`RenewLease`；
3. 结果提交、重复提交和旧 Attempt 拒绝：`ReportResult`；
4. Lease Reaper 与 Complete 的竞态；
5. 创建幂等：Idempotency Key + Request Hash；
6. Worker 的 Fetch → Execute → Renew → Report 主链路；
7. Worker Draining 与 Graceful Shutdown；
8. Transactional Outbox 的“业务状态 + 事件”同事务写入；
9. Outbox Relay 的 Claim → Publish → Mark Published；
10. Audit Consumer 的数据库幂等写入 → Offset 提交；
11. Kafka 重复投递和 Poison Message 处理；
12. 至少三组 PostgreSQL 并发竞态测试；
13. MySQL 8 的 `FOR UPDATE SKIP LOCKED` 简化任务领取；
14. MySQL 死锁复现、错误识别与有限重试；
15. MySQL `EXPLAIN ANALYZE`、联合索引与游标分页实验。

#### C. 建议亲手写一次、但不必重复造轮子的代码

包括：

- Gin 鉴权中间件；
- Cursor Pagination；
- Retry Backoff；
- Semaphore 有界并发；
- Context 取消传播；
- Prometheus 自定义指标；
- Testcontainers 启动 PostgreSQL/Kafka/MySQL；
- MySQL 与 PostgreSQL 的隔离级别、锁行为和查询计划对比实验。

---

## 1. 项目最终定位

Orbit Scheduler 是一个使用 Go 实现的可靠分布式任务执行系统，同时也是一套面向 Go 后端求职的完整工程能力证明。

项目需要同时证明两层能力：

```text
第一层：主流 Go 业务后端
Gin / GORM / REST API / Token 鉴权 / 多租户隔离
分页筛选 / 参数校验 / Migration / 连接池 / SQL 与索引

第二层：分布式平台后端
pgx / PostgreSQL 事务 / gRPC / Worker / Lease / Fencing
Transactional Outbox / Kafka / 幂等 / 故障恢复 / 可观测性
```

生产主系统采用：

```text
单逻辑 Scheduler
+
多个主动拉取任务的 Worker
+
PostgreSQL 权威状态
+
gRPC Worker 通信
+
Lease / Attempt No / Fencing
+
Transactional Outbox
+
Kafka 生命周期事件
+
Audit Consumer
```

仓库另外提供一个完全独立的 MySQL 8 工程实验模块：

```text
MySQL 8 Lab
├── GORM 与 database/sql 基础访问
├── Migration 与表结构约束
├── 联合索引、覆盖索引和游标分页
├── EXPLAIN ANALYZE 与慢查询优化
├── READ COMMITTED / REPEATABLE READ
├── 行锁、Gap Lock、Next-Key Lock
├── 死锁复现与有限重试
├── FOR UPDATE SKIP LOCKED 简化领取
└── PostgreSQL / MySQL 对比报告
```

MySQL Lab 的目的不是把 Orbit 改成“双数据库生产系统”，而是形成真实、可测试、可解释的 MySQL 使用证据。

系统需要提供：

- Project、API Token、Job、Task、Attempt、Worker 的完整管理能力；
- 清晰的 HTTP API Contract、统一错误结构和多租户隔离；
- Cursor Pagination、批量创建、组合过滤与稳定排序；
- PostgreSQL Migration、索引设计、连接池配置和查询计划验证；
- 多 Worker 原子领取；
- Worker 宕机恢复；
- Lease 续租；
- 过期 Worker 结果拒绝；
- 临时失败重试；
- 永久失败；
- 任务取消；
- 执行超时；
- 创建幂等；
- 结果提交幂等；
- Transactional Outbox；
- Kafka At-least-once 发布；
- Audit Consumer 幂等消费；
- DLQ；
- Prometheus；
- PostgreSQL/Kafka/MySQL Testcontainers；
- Race Detector；
- 故障注入；
- 性能测试；
- 可复现性能报告；
- MySQL 与 PostgreSQL 的数据库工程对比报告。

项目最终简历定位固定为：

> **Go 任务调度与执行平台：兼具完整业务 API、数据库工程和分布式可靠性。**

---

## 2. 固定执行语义与项目边界

### 2.1 系统提供的语义

Orbit 明确提供：

> At-least-once execution。

保证：

- 已持久化任务不会因为 Worker 临时宕机而永久丢失；
- 同一时刻只有当前有效的 Worker Instance 和 Attempt No 可以修改任务权威状态；
- 任务可能被重复执行，但过期执行者不能覆盖新执行结果；
- 相同结果提交可以安全重试；
- Outbox 事件至少一次发布，允许重复；
- Audit Consumer 可以识别并忽略重复事件；
- Kafka 暂时不可用时，核心任务调度仍能继续运行。

### 2.2 系统不承诺

- Exactly-once execution；
- 外部副作用只发生一次；
- Kafka 消息绝不重复；
- 不同 Task 之间的全局事件顺序；
- 多 Scheduler 高可用或选主；
- PostgreSQL 长时间不可用时继续调度；
- 强制终止任意 Goroutine；
- PostgreSQL 和 Kafka 跨系统分布式事务；
- DAG、Cron、复杂工作流编排；
- Kubernetes Operator；
- 复杂 Web 前端；
- 完整 RBAC。

### 2.3 固定不加入的组件

生产主链路不加入：

```text
Redis
Kubernetes
OpenTelemetry
Nacos
etcd
Raft
多 Scheduler 选主
Kafka Task Queue
DAG
Cron 平台
复杂前端
自研消息队列
生产环境 PostgreSQL/MySQL 双写
用 MySQL 替换 PostgreSQL 调度内核
```

例外说明：

- MySQL 8 只存在于 `experiments/mysql8`、`migrations/mysql8` 和 `tests/mysql8`；
- MySQL Lab 不被 `orbit-server`、`orbit-worker`、`outbox-relay` 或 `audit-consumer` 依赖；
- 不设计跨 PostgreSQL/MySQL 的分布式事务；
- 不用 MySQL Lab 的结果伪装成 Orbit 生产链路的性能结果。

---

## 3. 总体架构

```text
                          Client / CLI
                               │
                           HTTP REST
                               ▼
┌──────────────────────────────────────────────────────────┐
│                       orbit-server                       │
│                                                          │
│  Gin API                                                 │
│  ├── Project / Token                                     │
│  ├── Job / Task                                          │
│  ├── 分页 / 过滤 / 取消                                  │
│  └── Attempt / Result / Worker / Audit                   │
│                                                          │
│  GORM Repositories                                       │
│  └── 普通业务 CRUD                                       │
│                                                          │
│  Scheduler Core / pgx                                    │
│  ├── Create Idempotently                                 │
│  ├── FetchTasks                                          │
│  ├── RenewLease                                          │
│  ├── ReportResult                                        │
│  ├── Cancel                                              │
│  ├── Lease Reaper                                        │
│  └── Transactional Outbox                                │
│                                                          │
│  Worker gRPC Service                                     │
└────────────────────────────┬─────────────────────────────┘
                             │
                             ▼
                        PostgreSQL
               Project / Job / Task / Attempt
                 Worker / Outbox / Audit
                             │
                             ▼
┌──────────────────────────────────────────────────────────┐
│                       outbox-relay                       │
│ Claim Events → Commit Claim → Publish Kafka → Mark Done │
└────────────────────────────┬─────────────────────────────┘
                             ▼
                           Kafka
                   orbit.task-events.v1
                             │
                             ▼
┌──────────────────────────────────────────────────────────┐
│                      audit-consumer                      │
│ Validate → Idempotent DB Write → Commit Kafka Offset    │
│ Invalid Permanent Message → DLQ                         │
└──────────────────────────────────────────────────────────┘

                    gRPC
orbit-server ◄──────────────── orbit-worker A / B / C
                                   │
                              Executor Registry
                              ├── Mock Executor
                              └── HTTP Executor
```

独立数据库工程实验：

```text
开发者 / 测试命令
        │
        ▼
experiments/mysql8
├── CRUD 与事务示例
├── Query / Index Lab
├── Isolation / Lock Lab
├── Deadlock Retry Lab
└── SKIP LOCKED Lab
        │
        ▼
MySQL 8 Testcontainer

注意：该链路不连接 Orbit 生产进程，也不参与 PostgreSQL 数据复制或双写。
```

---

## 4. 进程与职责

仓库最终生成四个生产可执行程序，并提供一个独立实验入口：

```text
orbit-server
orbit-worker
outbox-relay
audit-consumer

# 独立实验命令，不属于生产进程
mysql8-lab
```

### 4.1 orbit-server

负责：

- Gin HTTP API；
- Project、Token、Job、Task 管理；
- GORM 普通查询和 CRUD；
- Worker gRPC 服务；
- pgx 调度事务；
- 原子领取；
- Lease 续租；
- 结果提交；
- 取消；
- Lease Reaper；
- Outbox 同事务写入；
- Server 侧 Prometheus 指标。

### 4.2 orbit-worker

负责：

- 生成新的 Worker Instance ID；
- 注册；
- Heartbeat；
- Fetch；
- 本地容量控制；
- Executor 执行；
- 每任务 Lease Renew；
- Context 取消；
- ReportResult 重试；
- Draining；
- Graceful Shutdown；
- Worker 侧 Prometheus 指标。

### 4.3 outbox-relay

负责：

- 扫描未发布 Outbox；
- 批量 Claim；
- 不持锁发布 Kafka；
- 成功标记；
- 失败退避；
- Claim 超时恢复；
- Outbox 清理；
- Relay 指标。

### 4.4 audit-consumer

负责：

- Kafka Consumer Group；
- Event Schema 校验；
- Event ID 幂等入库；
- 数据库成功后提交 Offset；
- 临时错误重试；
- 永久错误写入 DLQ；
- Consumer Lag、Duplicate、DLQ 指标。

### 4.5 mysql8-lab

只用于开发、测试和面试复盘，负责：

- 启动隔离的 MySQL 8 Testcontainer；
- 执行独立 MySQL Migration；
- 运行 CRUD、事务、索引和分页实验；
- 复现 READ COMMITTED 与 REPEATABLE READ 的差异；
- 复现行锁、Gap Lock、Next-Key Lock 和死锁；
- 实现可识别 MySQL 死锁错误的有限重试；
- 实现简化版 `FOR UPDATE SKIP LOCKED` 并发领取；
- 输出查询计划、实验日志和数据库对比报告；
- 不提供线上 API，不被生产二进制导入。

---

## 5. 技术选型与工程约束

### 5.1 语言和基础库

- Go；
- 标准库 `context`、`log/slog`、`net/http`、`crypto/rand`、`crypto/sha256`；
- Gin：HTTP API；
- GORM：普通业务 CRUD；
- pgx：关键事务与手写 SQL；
- gRPC + Protobuf：Server 与 Worker 通信；
- PostgreSQL：任务权威状态；
- Kafka 客户端：建议使用 franz-go；
- Prometheus Go Client；
- Testcontainers for Go；
- MySQL 8.x：仅用于独立工程实验模块；
- MySQL Driver：GORM MySQL Driver 与 `database/sql`；
- Migration：PostgreSQL 与 MySQL 分开维护版本，不使用 AutoMigrate 管理正式 Schema；
- Docker Compose：本地运行环境；
- k6 或自研 Go Client：HTTP 压测；
- ghz 或自研 Benchmark：gRPC 压测。

### 5.2 GORM 与 pgx 边界

GORM 负责：

- Project CRUD；
- API Token 管理；
- Job 普通查询；
- Task 普通查询；
- Attempt 查询；
- Worker 展示；
- Audit 查询；
- 管理类接口。

pgx 负责：

- 原子任务领取；
- Lease 续租；
- ReportResult；
- Lease Reaper；
- Cancel 与 Complete 竞态；
- 创建幂等关键事务；
- Result 幂等关键事务；
- Transactional Outbox；
- Outbox Claim；
- Consumer 幂等写入。

禁止：

```text
在同一个数据库事务中混用 GORM Transaction 和 pgx Transaction
```

MySQL Lab 边界：

- 允许使用 GORM 完成普通 CRUD 和 Schema 映射；
- 关键锁、隔离级别、死锁和 `SKIP LOCKED` 实验必须使用 `database/sql` 或原生 SQL；
- PostgreSQL Repository 和 MySQL Lab Repository 必须位于不同包；
- 生产 Service 层不得依赖 MySQL Lab 接口；
- 不抽象一个“万能跨数据库 Repository”掩盖数据库差异。

### 5.3 连接池约束

必须分别配置：

- GORM/`database/sql` 最大打开连接；
- GORM 最大空闲连接；
- pgxpool 最大连接；
- pgxpool 最小连接；
- 连接最大生命周期；
- 连接空闲时间。

必须保证：

```text
GORM 最大连接
+
pgx 最大连接
+
Migration/测试/管理预留
<
PostgreSQL max_connections
```

MySQL Lab 使用独立连接池和独立配置：

```text
MYSQL_LAB_MAX_OPEN_CONNS
+
MYSQL_LAB_MAX_IDLE_CONNS
<
MySQL max_connections
```

实验结束必须关闭连接池；测试不得依赖本机已有 MySQL 实例。

### 5.4 数据库选型结论

固定结论：

- PostgreSQL 继续承担任务权威状态和调度核心；
- MySQL Lab 只证明 MySQL 工程能力；
- 不因为 JD 关键词更换 Orbit 主数据库；
- 面试时必须能够分别解释两种数据库在隔离级别、锁、索引、`SKIP LOCKED` 和错误处理上的差异；
- 简历中必须写清“独立 MySQL 8 实验”，不得让人误以为生产系统同时依赖两个主库。

---

## 6. 仓库结构

```text
orbit-scheduler/
├── cmd/
│   ├── orbit-server/
│   │   └── main.go
│   ├── orbit-worker/
│   │   └── main.go
│   ├── outbox-relay/
│   │   └── main.go
│   └── audit-consumer/
│       └── main.go
│
├── internal/
│   ├── api/
│   │   ├── handler/
│   │   ├── dto/
│   │   ├── router.go
│   │   └── response.go
│   ├── middleware/
│   ├── auth/
│   ├── config/
│   ├── domain/
│   ├── gormrepo/
│   ├── pgstore/
│   ├── scheduler/
│   ├── grpcservice/
│   ├── worker/
│   ├── executor/
│   ├── outbox/
│   ├── kafka/
│   ├── audit/
│   ├── observability/
│   ├── clock/
│   └── testkit/
│
├── proto/
│   ├── orbit/worker/v1/worker.proto
│   └── buf.gen.yaml
├── migrations/
│   ├── postgres/
│   └── mysql8/
├── experiments/
│   └── mysql8/
│       ├── cmd/
│       │   └── mysql8-lab/
│       ├── internal/
│       │   ├── model/
│       │   ├── repository/
│       │   ├── querylab/
│       │   ├── locklab/
│       │   ├── retry/
│       │   └── testkit/
│       └── README.md
├── tests/
│   ├── integration/
│   ├── concurrency/
│   ├── fault/
│   ├── performance/
│   └── mysql8/
├── deploy/
│   ├── docker-compose.yml
│   ├── prometheus.yml
│   └── grafana/
├── scripts/
├── docs/
│   ├── architecture.md
│   ├── state-machine.md
│   ├── execution-semantics.md
│   ├── failure-cases.md
│   ├── performance-report.md
│   ├── business-api-and-querying.md
│   ├── mysql-vs-postgresql.md
│   ├── mysql-index-and-lock-lab.md
│   └── learning-notes/
├── Makefile
├── go.mod
├── go.sum
├── .env.example
├── .golangci.yml
├── .github/workflows/ci.yml
└── README.md
```

---

## 7. Git、Push 与学习分支规范

## 7.1 分支

```text
main                         稳定可运行参考实现
phase/<number>-<name>        当前 Phase 的 Vibe Coding 分支
learn/<number>-<core-name>   用户手写复刻分支
fix/<name>                   缺陷修复
perf/<name>                  性能实验
lab/mysql8-<name>            MySQL 8 独立实验
```

### 7.2 核心检查点

每条核心链路至少保留两个 Tag：

```text
<phase>-<core>-start       只有接口、测试和 TODO，没有核心实现
<phase>-<core>-reference   AI 参考实现完成并通过测试
```

例如：

```text
p2-fetch-start
p2-fetch-reference
p2-lease-start
p2-lease-reference
p4-worker-loop-start
p4-worker-loop-reference
p6-outbox-relay-start
p6-outbox-relay-reference
p8b-mysql-skip-locked-start
p8b-mysql-skip-locked-reference
p8b-mysql-deadlock-start
p8b-mysql-deadlock-reference
```

### 7.3 固定操作

核心实现前：

```bash
git add .
git commit -m "chore(p2): scaffold atomic task fetch"
git tag p2-fetch-start
git push origin phase/2-scheduler-core
git push origin p2-fetch-start
```

AI 参考实现通过后：

```bash
git add .
git commit -m "feat(scheduler): implement atomic task fetch"
git tag p2-fetch-reference
git push origin phase/2-scheduler-core
git push origin p2-fetch-reference
```

亲手复刻：

```bash
git switch -c learn/2-atomic-fetch p2-fetch-start
# 自己实现
go test ./...
git add .
git commit -m "learn(scheduler): reimplement atomic task fetch"
git push -u origin learn/2-atomic-fetch
```

对比参考实现：

```bash
git diff p2-fetch-reference...learn/2-atomic-fetch -- internal/pgstore internal/scheduler tests
```

### 7.4 为什么必须 Push

本项目不只依赖本地 commit。以下节点必须 Push 到远端：

- 每个 Phase 的启动骨架；
- 每条核心链路的 `start` Tag；
- 每条核心链路的 `reference` Tag；
- 每个 Phase 的最终验收提交；
- 所有学习分支完成提交；
- 故障注入和性能报告提交；
- MySQL Lab 的核心实验、报告和 Tag；
- `v1.0.0` Release Tag。

这样即使本地误操作，也可以从远端 Tag 精确恢复。

---

## 8. Commit 规范

建议使用：

```text
chore:  工程配置、骨架、依赖
feat:   功能实现
fix:    缺陷修复
test:   测试
refactor: 不改变行为的重构
docs:   文档
perf:   性能优化
learn:  手写复刻与学习记录
```

示例：

```text
chore(p0): initialize repository and local dependencies
feat(schema): add task attempt and worker migrations
test(scheduler): add concurrent fetch integration cases
feat(scheduler): implement lease renewal with fencing
feat(worker): implement bounded execution loop
feat(outbox): publish claimed events to kafka
fix(reaper): prevent terminal state regression
perf(fetch): batch attempt inserts and reduce round trips
docs: add failure recovery walkthrough
feat(mysql-lab): add indexed cursor query experiment
learn(mysql-lab): reimplement deadlock retry
```

一个 commit 应满足：

- 能解释一个明确变化；
- 尽量能独立编译；
- 核心参考实现与测试不要混成无法阅读的巨型提交；
- 不要把格式化、依赖升级和核心逻辑同时塞进一个 commit。

---

# 9. Phase 总览

| Phase    | 主题                  | 主要成果                                      | 必须手写复刻               |
| -------- | --------------------- | --------------------------------------------- | -------------------------- |
| Phase 0  | 工程初始化            | 仓库、配置、Docker、CI 骨架                   | 否                         |
| Phase 1  | 领域模型与数据库      | Migration、Schema、Repository 边界            | 部分 SQL 与约束            |
| Phase 2  | PostgreSQL 调度内核   | Fetch、Renew、Report、Reaper、竞态            | 是，最高优先级             |
| Phase 3  | Gin 业务 API          | Project、Token、Job、Task、分页、取消、幂等   | 创建幂等建议手写           |
| Phase 4  | gRPC 与 Worker 主链路 | Register、Heartbeat、Fetch、Renew、Report     | 是                         |
| Phase 5  | Executor 与退出       | Mock、HTTP、SSRF、Timeout、Draining           | Graceful Shutdown 必须手写 |
| Phase 6  | Outbox 与 Kafka       | Relay、Consumer、幂等、DLQ                    | 是                         |
| Phase 7  | 可观测性与集成测试    | Prometheus、Testcontainers、Race、CI          | 部分                       |
| Phase 8  | 故障注入与性能        | 故障矩阵、压测、瓶颈分析                      | 亲手做实验                 |
| Phase 8B | MySQL 8 工程实验      | 索引、MVCC、锁、死锁、SKIP LOCKED、数据库对比 | 是                         |
| Phase 9  | 文档、演示与发布      | README、架构图、报告、v1.0.0                  | 亲自讲解                   |

## 9.1 两个交付里程碑

### Milestone A：Orbit Job-ready

达到以下条件即可开始以 Orbit 投递 Go 后端实习，不必等待全部高级模块结束：

- Phase 0—3 完成；
- Phase 2 的 Fetch、Renew、Report、Reaper 核心正确；
- Phase 4 Worker 主链路完成；
- Phase 5 Mock Executor、超时、取消与 Graceful Shutdown 完成；
- 提前完成 Phase 7 中最基础的 Prometheus、PostgreSQL Testcontainers、Race Detector 和 CI；
- 至少完成 MySQL Lab 的 CRUD、索引、`EXPLAIN ANALYZE` 和事务隔离基础实验；
- README 可以完成 Project → Token → Task → Worker → Result 的完整演示；
- 核心链路能够独立讲解。

该里程碑的目的：

> 先形成一份可信、可运行、可投递的 Go 后端项目，再边投递边补 Kafka、Outbox、故障矩阵和完整 MySQL Lab。

### Milestone B：Orbit Complete

完成全部 Phase，包括：

- Transactional Outbox；
- Kafka Relay 与 Audit Consumer；
- DLQ；
- 完整可观测性；
- 故障注入；
- 性能报告；
- MySQL 8 全部实验与 PostgreSQL 对比；
- v1.0.0 Release。

---

# Phase 0：工程初始化与运行基线

## 10. Phase 0 目标

建立能够持续演进的工程骨架。此阶段不实现调度逻辑，但必须做到：

- 四个程序能够编译；
- PostgreSQL 和 Kafka 能通过 Docker Compose 启动；
- 配置可加载；
- 日志可用；
- 健康检查可用；
- Migration 命令可执行；
- 单元测试和静态检查可运行；
- CI 骨架可运行。

## 10.1 实现内容

### 工程初始化

- `go mod init`；
- 创建目录结构；
- 添加 Makefile；
- 添加 `.gitignore`；
- 添加 `.env.example`；
- 添加 License；
- 添加基础 README；
- 创建四个 `cmd` 入口；
- 为每个进程实现统一的启动和退出日志。

### 本地依赖

Docker Compose 默认包含：

- PostgreSQL；
- Kafka；
- 可选 Kafka 管理 UI，仅用于本地观察；
- Prometheus；
- 可选 Grafana。

另外提供独立 Profile：

```text
mysql-lab
```

该 Profile 只启动 MySQL 8 实验环境，不被默认生产栈依赖。正式实验与 CI 仍优先使用 Testcontainers，避免依赖开发者本机状态。

### 基础配置

至少支持：

```text
APP_ENV
LOG_LEVEL
HTTP_ADDR
GRPC_ADDR
METRICS_ADDR
DATABASE_URL
GORM_MAX_OPEN_CONNS
GORM_MAX_IDLE_CONNS
PGX_MAX_CONNS
PGX_MIN_CONNS
KAFKA_BROKERS
KAFKA_TASK_EVENTS_TOPIC
KAFKA_TASK_EVENTS_DLQ_TOPIC
TOKEN_PEPPER

# 仅 MySQL Lab 使用
MYSQL_LAB_DSN
MYSQL_LAB_MAX_OPEN_CONNS
MYSQL_LAB_MAX_IDLE_CONNS
```

### 基础组件

- `internal/config`；
- `internal/observability/logging.go`；
- `internal/clock`，生产使用真实时钟，测试可注入；
- `internal/api/response.go`；
- `/health/live`；
- `/health/ready`；
- `/metrics` 端点骨架。

## 10.2 AI Vibe Coding 任务

AI 可以一次性生成：

- 目录结构；
- 配置结构体；
- 四个 main 入口；
- Docker Compose；
- Makefile；
- CI 初始文件；
- 健康检查；
- 日志初始化；
- 基础测试。

但必须要求 AI：

- 不实现虚假的内存 Scheduler；
- 不使用 SQLite；
- 不提前加入 Redis、etcd 或 Kubernetes；
- 不在 main 中堆业务逻辑；
- 不让生产进程导入 `experiments/mysql8`；
- 不提前实现 PostgreSQL/MySQL 双写；
- 所有服务构造函数返回 error；
- 所有后台循环接受 Context。

## 10.3 推荐提交

```text
chore(p0): initialize go module and repository layout
chore(dev): add postgres kafka and prometheus compose stack
feat(config): add validated process configuration
feat(platform): add logging health checks and shutdown skeleton
test(platform): add configuration and health endpoint tests
```

Phase 完成：

```bash
git tag p0-bootstrap
git push origin main
git push origin p0-bootstrap
```

## 10.4 验收

```bash
make lint
make test
make compose-up
make migrate-up
go build ./cmd/...
curl localhost:<port>/health/live
curl localhost:<port>/health/ready
```

验收标准：

- 四个程序编译成功；
- PostgreSQL 和 Kafka 健康；
- 环境变量缺失时明确报错；
- SIGTERM 能让空服务正常退出；
- CI 能运行 `go test ./...`。

---

# Phase 1：领域模型、Migration 与数据访问边界

## 11. Phase 1 目标

完成所有最终数据表、约束、索引、领域类型和 Repository 接口，为后续并发核心打下稳定基础。

此阶段不追求先少建表再补，而是一次规划完整最终 Schema；Migration 仍可以按依赖顺序拆分。

## 11.1 Migration 顺序

建议：

```text
000001_enable_extensions
000002_create_projects_and_tokens
000003_create_jobs_tasks_attempts
000004_create_worker_instances
000005_create_outbox_events
000006_create_audit_events
000007_add_constraints_and_indexes
000008_add_housekeeping_indexes
```

UUID 可由应用生成，数据库不必依赖隐式随机行为；所有时间字段使用 `timestamptz`。

## 11.2 表结构

### projects

核心字段：

```text
id uuid primary key
name text not null
status text not null
 task_quota bigint not null
max_concurrent_tasks integer not null
created_at timestamptz not null
updated_at timestamptz not null
```

约束：

- `status` 只能为 ACTIVE、DISABLED；
- quota 非负；
- max concurrent tasks 大于 0；
- name 在合理长度内。

### api_tokens

```text
id uuid primary key
project_id uuid not null references projects(id)
token_prefix text not null
token_hash bytea not null
name text not null
scopes text[] not null
disabled boolean not null
expires_at timestamptz null
last_used_at timestamptz null
created_at timestamptz not null
updated_at timestamptz not null
```

要求：

- 明文 Token 只在创建时返回；
- 保存 Hash，不保存明文；
- Token 可使用 32 字节安全随机数；
- Hash 计算包含服务端 Pepper；
- 日志禁止输出 Token 明文。

### jobs

```text
id uuid primary key
project_id uuid not null references projects(id)
name text not null
cancel_requested_at timestamptz null
metadata jsonb not null default '{}'
created_at timestamptz not null
updated_at timestamptz not null
```

Job 状态从 Task 聚合计算，不维护可漂移的计数作为事实源。

### tasks

```text
id uuid primary key
project_id uuid not null references projects(id)
job_id uuid null references jobs(id)
task_type text not null
payload jsonb not null
payload_hash bytea not null
status text not null
priority integer not null
available_at timestamptz not null
execution_timeout interval not null
overall_deadline timestamptz null
max_attempts integer not null
attempt_no integer not null default 0
lease_owner_instance_id uuid null
lease_expires_at timestamptz null
cancel_requested_at timestamptz null
idempotency_key text null
creation_request_hash bytea null
result jsonb null
result_hash bytea null
final_error_type text null
final_error_message text null
completed_by_worker_instance_id uuid null
completed_attempt_no integer null
created_at timestamptz not null
updated_at timestamptz not null
completed_at timestamptz null
```

状态：

```text
PENDING
RUNNING
SUCCEEDED
FAILED
CANCELED
```

延迟重试：

```text
status = PENDING
available_at = 退避后的下一次时间
```

不增加 RETRY_WAIT。

重要约束：

- `attempt_no >= 0`；
- `max_attempts > 0`；
- PENDING 时 Lease 字段应为空；
- RUNNING 时 Lease Owner 和 Lease Expires 应存在；
- 终态必须有 `completed_at`；
- Project 范围创建幂等唯一约束：`UNIQUE(project_id, idempotency_key)`，允许 Key 为空；
- Result 大小在应用层和数据库写入前限制。

### task_attempts

```text
task_id uuid not null references tasks(id)
attempt_no integer not null
worker_name text not null
worker_instance_id uuid not null
started_at timestamptz not null
finished_at timestamptz null
outcome text null
error_type text null
error_message text null
execution_duration_ms bigint null
lease_expired boolean not null default false
result_hash bytea null
created_at timestamptz not null
updated_at timestamptz not null
primary key(task_id, attempt_no)
```

### worker_instances

```text
id uuid primary key
worker_name text not null
hostname text not null
capacity integer not null
supported_task_types text[] not null
running_tasks integer not null
draining boolean not null
last_heartbeat_at timestamptz not null
started_at timestamptz not null
process_version text not null
metadata jsonb not null default '{}'
created_at timestamptz not null
updated_at timestamptz not null
```

每次进程启动必须生成新 UUID。逻辑 Worker Name 不作为租约唯一身份。

### outbox_events

```text
event_id uuid primary key
aggregate_type text not null
aggregate_id uuid not null
event_type text not null
event_version integer not null
event_key text not null
payload jsonb not null
trace_id text null
created_at timestamptz not null
published_at timestamptz null
publish_attempts integer not null default 0
next_attempt_at timestamptz not null
last_error text null
claimed_by text null
claim_expires_at timestamptz null
```

### audit_events

```text
event_id uuid primary key
aggregate_type text not null
aggregate_id uuid not null
event_type text not null
event_version integer not null
payload jsonb not null
kafka_topic text not null
kafka_partition integer not null
kafka_offset bigint not null
occurred_at timestamptz not null
consumed_at timestamptz not null
```

`event_id` 唯一约束承担 Consumer 幂等。

## 11.3 核心索引

至少包括：

```text
tasks(status, available_at, priority desc, id)
tasks(project_id, status, created_at desc, id desc)
tasks(job_id, created_at desc, id desc)
tasks(task_type, status, available_at)
tasks(lease_expires_at) where status = 'RUNNING'
task_attempts(task_id, attempt_no desc)
worker_instances(last_heartbeat_at)
outbox_events(next_attempt_at, created_at) where published_at is null
outbox_events(claim_expires_at) where published_at is null
audit_events(aggregate_id, occurred_at desc)
```

索引必须通过真实查询计划验证，不能只因为“看起来会用到”就无限添加。

## 11.4 领域类型

在 `internal/domain` 定义：

- `TaskStatus`；
- `TaskOutcome`；
- `ErrorType`；
- `JobDerivedStatus`；
- `WorkerInstance`；
- `Task`；
- `TaskAttempt`；
- `OutboxEvent`；
- `AuditEvent`；
- 状态转换校验函数；
- 终态判断；
- Retry Backoff 纯函数；
- Payload/Result/Creation Request Hash 函数。

## 11.5 Repository 接口

接口按调用者需要定义，不做一个巨大的通用 Repository。

示例：

```text
ProjectRepository
TokenRepository
JobQueryRepository
TaskQueryRepository
SchedulerStore
OutboxStore
AuditStore
WorkerRegistryStore
```

`SchedulerStore` 只暴露调度语义方法，不把 pgx 事务对象泄漏到 Service 层。

## 11.6 学习检查点

虽然这一阶段没有复杂并发链路，但必须亲手完成：

- tasks 的 CHECK/UNIQUE 约束设计；
- 三个核心索引的理由；
- `TaskStatus` 和终态判断；
- Retry Backoff 纯函数；
- Hash 的规范化策略。

## 11.7 提交与 Push

```text
feat(schema): add project token and job migrations
feat(schema): add task attempt and worker migrations
feat(schema): add outbox audit constraints and indexes
feat(domain): define task state and retry semantics
feat(store): define gorm and pgx repository boundaries
test(domain): cover state retry and hashing rules
```

Tag：

```bash
git tag p1-schema-domain
git push origin main
git push origin p1-schema-domain
```

## 11.8 验收

- 全部 Migration 可从空库升级；
- 全部 Migration 可在测试容器执行；
- 不使用正式 AutoMigrate；
- 非法状态和非法数值被约束拒绝；
- Idempotency 唯一约束存在；
- Task/Attempt/Outbox/Audit 字段完整；
- 领域单元测试通过。

---

# Phase 2：PostgreSQL 调度内核

这是整个项目最重要的 Phase。必须拆成多个可回滚的核心检查点。

# Phase 2A：原子任务领取 FetchTasks

## 12. Phase 2A 目标

实现多个 Worker 并发 Fetch 时：

- 同一 Task 不会被同时领取；
- 按优先级、可用时间和 ID 稳定排序；
- Attempt No 单调递增；
- Task、Attempt、Outbox 在同一事务内写入；
- 不匹配的任务类型不会被领取；
- 已过 Overall Deadline 的任务不会被领取；
- 返回的任务已经处于 RUNNING 且拥有有效 Lease。

## 12.1 事务流程

```text
BEGIN
→ 选择 status=PENDING 且 available_at<=数据库当前时间
→ 排除已过 overall_deadline 的任务
→ 匹配 supported_task_types
→ ORDER BY priority DESC, available_at ASC, id ASC
→ FOR UPDATE SKIP LOCKED
→ LIMIT batch
→ UPDATE 为 RUNNING
→ attempt_no = attempt_no + 1
→ 写 lease_owner_instance_id
→ 写 lease_expires_at
→ 创建 Task Attempt
→ 创建 TASK_STARTED Outbox Event
→ COMMIT
→ 返回领取结果
```

数据库时间统一使用 `statement_timestamp()` 或事务内明确选择的数据库时间，避免 Worker 本地时钟作为权威。

## 12.2 服务端限制

实际领取数量：

```text
min(
  requested_batch_size,
  registered_capacity - server_known_running,
  server_max_fetch_batch
)
```

不信任 Worker 任意上报的容量。

## 12.3 核心实现前检查点

AI 先完成：

- `SchedulerStore.FetchTasks` 接口；
- 输入输出类型；
- SQL 文件或常量位置；
- 事务辅助函数；
- PostgreSQL Testcontainers 测试骨架；
- 并发测试：20 个 Goroutine 领取 100 个 Task；
- TODO 和预期断言；
- 不填写核心 SQL 和事务实现。

提交：

```bash
git commit -am "chore(p2): scaffold atomic task fetch"
git tag p2-fetch-start
git push origin phase/2-scheduler-core
git push origin p2-fetch-start
```

## 12.4 AI 参考实现要求

AI 完成后必须解释：

1. 为什么使用 `FOR UPDATE SKIP LOCKED`；
2. 行锁在何时获取和释放；
3. 为什么不能先 SELECT 再在 Go 中判断后无条件 UPDATE；
4. CTE UPDATE 的每一步；
5. Attempt 写入为何必须与 Task 更新同事务；
6. Outbox 写入为何必须同事务；
7. Claim 响应丢失时系统如何恢复；
8. Batch Size 对锁和吞吐的影响；
9. PostgreSQL 默认隔离级别下这一实现为何成立；
10. 空结果、超时和事务冲突分别如何返回。

参考实现通过：

```bash
git commit -am "feat(scheduler): implement atomic task fetch"
git tag p2-fetch-reference
git push origin phase/2-scheduler-core
git push origin p2-fetch-reference
```

## 12.5 手写复刻任务

从 `p2-fetch-start` 创建学习分支，亲手完成：

- 候选 Task SQL；
- CTE UPDATE；
- Attempt 批量插入；
- Outbox 批量插入；
- 事务提交；
- RowsAffected/Returning 校验；
- 错误包装；
- 并发测试修复。

不得直接复制参考实现。可以查看：

- 接口；
- 测试；
- PostgreSQL 文档；
- AI 对 SQL 语义的解释。

完成后与参考分支进行 diff，记录：

- SQL 结构差异；
- 往返次数差异；
- 错误处理差异；
- 是否存在 N+1 插入；
- 是否会遗漏 Outbox。

## 12.6 验收测试

- 100 个 Task 被 10 个 Worker 并发领取，每个 Task 只出现一次；
- Attempt No 从 0 变 1；
- 第二次执行变 2；
- 不支持类型不领取；
- Future `available_at` 不领取；
- Deadline 已过不领取；
- 事务中故意让 Attempt 插入失败，Task 不得变 RUNNING；
- 事务中故意让 Outbox 插入失败，Task 不得变 RUNNING；
- Claim 提交后模拟响应丢失，Lease 到期可被回收。

---

# Phase 2B：Lease Renew、Fencing 与 ReportResult

## 13. Phase 2B 目标

实现：

- 只有当前 Worker Instance + Attempt No 能续租；
- 过期 Lease 不能续租；
- 旧 Worker 结果被拒绝；
- 相同结果重复提交幂等成功；
- 相同 Attempt 不同结果冲突；
- 临时失败进入延迟重试；
- 永久失败进入 FAILED；
- 成功进入 SUCCEEDED；
- 取消和超时按语义处理；
- Attempt 和 Outbox 同事务更新。

## 13.1 RenewLease 条件

必须同时校验：

```text
Task ID 匹配
status = RUNNING
lease_owner_instance_id 匹配
attempt_no 匹配
lease_expires_at > 数据库当前时间
```

使用：

```sql
UPDATE ...
SET lease_expires_at = statement_timestamp() + interval
WHERE ...全部前置条件...
RETURNING ...
```

返回需要区分：

- Renewed；
- Stale Lease；
- Task Canceled/Cancel Requested；
- Task 已进入终态；
- 数据库暂时失败；
- 网络不确定失败。

## 13.2 ReportResult 输入

```text
task_id
worker_instance_id
attempt_no
outcome
result
result_hash
error_type
error_message
execution_started_at
execution_finished_at
```

Outcome：

```text
SUCCEEDED
RETRYABLE_FAILURE
PERMANENT_FAILURE
TIMEOUT
CANCELED
```

## 13.3 结果状态转换

### SUCCEEDED

- 条件匹配当前 RUNNING Lease；
- 写 result、result_hash；
- 写 completed worker/attempt；
- 清空 Lease；
- 更新 Attempt；
- 写 `TASK_SUCCEEDED` Outbox。

### RETRYABLE_FAILURE

如果：

- 未达到 Max Attempts；
- 未超过 Overall Deadline；
- Job/Task 未取消；

则：

```text
status = PENDING
available_at = next retry time
清空 lease
更新 Attempt outcome
写 TASK_RETRY_SCHEDULED
```

否则进入 FAILED 或 CANCELED。

### PERMANENT_FAILURE

进入 FAILED，记录最终错误和 `TASK_FAILED`。

### TIMEOUT

根据策略作为可重试失败处理；达到次数或 Deadline 后 FAILED。

### CANCELED

只有在任务已请求取消、执行 Context 被取消等符合条件时进入 CANCELED。不能让任意 Worker 随意取消一个未请求取消的任务。

## 13.4 结果提交幂等

Task 永久保存：

```text
completed_by_worker_instance_id
completed_attempt_no
result_hash
final_status
```

处理顺序：

1. 尝试按当前 RUNNING + Lease + Attempt 条件完成；
2. 如果更新成功，返回成功；
3. 如果更新失败，读取最小必要的当前终态信息；
4. 若已由相同 Worker、相同 Attempt、相同 Outcome/Result Hash 完成，返回幂等成功；
5. 若同 Attempt 但 Hash 不同，返回 Conflict；
6. 若 Attempt 已旧，返回 Stale Lease；
7. 若任务由其他 Attempt 完成，返回 Stale/Already Finalized。

## 13.5 Retry Backoff

```text
delay = min(base × 2^(attempt_no - 1), max_delay) + jitter
```

要求：

- Jitter 可注入随机源；
- 单元测试使用固定随机种子或假随机源；
- 结果不能超过最大延迟；
- 不能溢出；
- Overall Deadline 是最终上限。

## 13.6 核心检查点

AI 先生成：

- RenewLease/ReportResult 接口；
- 领域错误；
- 测试表格；
- 幂等测试；
- Stale 测试；
- 事务骨架；
- 核心 SQL 留 TODO。

```bash
git commit -am "chore(p2): scaffold lease and result transactions"
git tag p2-lease-result-start
git push origin phase/2-scheduler-core
git push origin p2-lease-result-start
```

AI 完成参考实现后：

```bash
git commit -am "feat(scheduler): implement lease renewal and idempotent result reporting"
git tag p2-lease-result-reference
git push origin phase/2-scheduler-core
git push origin p2-lease-result-reference
```

## 13.7 AI 必须讲解

- Attempt No 为什么既是尝试次数又是 Fencing Token；
- Worker Name 为什么不能作为 Lease Owner；
- 为什么 Lease 过期后旧 Worker 即使真的完成也必须被拒绝；
- 网络超时为什么不能直接解释成 Renew 失败；
- Result 事务成功但响应丢失时如何恢复；
- 为何重复结果需要保存 Result Hash；
- 为什么终态不能回退；
- Retry、Timeout、Cancel 的优先级；
- SQL 条件更新如何解决并发；
- 错误码如何映射 gRPC/HTTP。

## 13.8 手写复刻

至少亲手实现：

- RenewLease 条件 SQL；
- `ReportResult(SUCCEEDED)`；
- 重复结果幂等判断；
- Stale Attempt 判断；
- Retryable Failure 状态转换；
- Result 与 Attempt、Outbox 同事务。

## 13.9 验收

- 有效 Worker 续租成功；
- 错 Worker、错 Attempt、过期 Lease 均失败；
- Attempt 1 过期，Attempt 2 成功后，Attempt 1 提交被拒绝；
- Result 事务成功但响应丢失，重复提交返回成功；
- 相同 Attempt 不同 Result Hash 返回 Conflict；
- Retry 按 Backoff 进入 PENDING；
- 达到 Max Attempts 进入 FAILED；
- Deadline 已过不再重试；
- 任何终态不能被后续更新回 RUNNING/PENDING。

---

# Phase 2C：Lease Reaper、取消和竞态

## 14. Phase 2C 目标

实现后台 Reaper，并验证它与 ReportResult、Cancel 的竞态正确性。

## 14.1 Reaper 扫描条件

```text
status = RUNNING
lease_expires_at <= statement_timestamp()
```

批量处理时建议：

- 限制 Batch；
- `FOR UPDATE SKIP LOCKED`；
- 条件更新；
- 不一次锁住全部过期任务；
- 循环接受 Context；
- 批次之间可短暂等待；
- 记录处理数量和耗时。

## 14.2 Reaper 决策

Lease 过期后：

- 如果已请求取消：CANCELED；
- 如果 Attempt 已达到 Max Attempts：FAILED；
- 如果 Overall Deadline 已过：FAILED 或 CANCELED，按明确策略；
- 否则：回到 PENDING，并计算下一次 Available At；
- 更新当前 Attempt：`lease_expired=true`；
- 创建 `TASK_LEASE_EXPIRED`；
- 如果重新排队，再创建 `TASK_RETRY_SCHEDULED`，或在事件 Payload 表示原因；
- 清空 Lease 字段。

## 14.3 取消

PENDING：

```text
条件更新为 CANCELED
写 completed_at
写 TASK_CANCELED
```

RUNNING：

```text
只设置 cancel_requested_at
写取消请求相关事件或审计信息
Worker 通过 RenewLease 响应获得 cancel_requested=true
```

Worker 仍有有效 Lease 且在取消条件更新前完成时，可以 SUCCEEDED。取消是协作式请求，不是时间倒流。

Job Cancel：

- 设置 Job `cancel_requested_at`；
- 批量取消 PENDING Task；
- 对 RUNNING Task 设置 cancel_requested_at；
- 写 `JOB_CANCEL_REQUESTED`；
- 过程需要可重试和幂等。

## 14.4 核心检查点

```bash
git commit -am "chore(p2): scaffold lease reaper and cancellation races"
git tag p2-reaper-start
git push origin phase/2-scheduler-core
git push origin p2-reaper-start
```

参考实现：

```bash
git commit -am "feat(scheduler): implement lease reaper and conditional cancellation"
git tag p2-reaper-reference
git push origin phase/2-scheduler-core
git push origin p2-reaper-reference
```

## 14.5 必须手写复刻

- Reaper Candidate SQL；
- Reaper 条件更新；
- Attempt lease_expired 更新；
- Reaper 与 Complete 并发测试；
- Cancel 与 Complete 并发测试；
- Job Cancel 幂等重试。

## 14.6 必须通过的竞态

### Reaper 与 Complete

```text
两者同时对同一 Task 条件更新
→ 只有一方成功
→ 如果 Complete 先成功，Reaper 更新 0 行
→ 如果 Reaper 先成功，旧 Worker Complete 变 Stale
→ 终态绝不回退
```

### Cancel 与 Complete

```text
PENDING Cancel 和 Fetch 竞争
RUNNING Cancel Request 和 Complete 竞争
→ 都依赖条件状态
→ 不允许无条件覆盖
```

### 两个 Reaper

```text
多个 Reaper 实例同时运行
→ SKIP LOCKED 或 Claim 机制避免重复处理同一批
→ 即使重复扫描，条件更新仍保证正确
```

## 14.7 Phase 2 总验收

- 原子领取正确；
- Lease/Fencing 正确；
- 结果幂等正确；
- Reaper 正确；
- Cancel 正确；
- 所有关键状态变更写 Attempt/Outbox；
- PostgreSQL 并发测试稳定重复通过；
- `go test -race ./...` 不报数据竞争；
- 可以用命令行直接模拟完整 Task 生命周期。

Phase Tag：

```bash
git tag p2-scheduler-core
git push origin main
git push origin p2-scheduler-core
```

---

# Phase 3：Gin 业务 API、数据库工程与创建幂等

## 15. Phase 3 目标

构建完整业务管理 API，让 Orbit 不只是“调度算法项目”，而是一套真正具备业务后端工程能力的服务。

必须严格保持：

```text
普通 CRUD、查询展示走 GORM
关键状态、幂等与调度事务走 pgx
生产主库始终为 PostgreSQL
```

本 Phase 不允许只生成 Handler 和简单增删改查后就结束。最终必须证明：

- 会设计 API Contract；
- 会做多租户数据隔离；
- 会设计 Migration、约束和索引；
- 会处理批量创建、分页、过滤与稳定排序；
- 会分析查询计划；
- 会配置连接池和请求超时；
- 会在数据库唯一约束、事务与应用校验之间划分职责。

## 15.1 Gin 中间件

必须实现：

- Request ID；
- Trace ID；
- `slog` 结构化访问日志；
- Recovery；
- API Token 鉴权；
- Scope 校验；
- Body 大小限制；
- 请求超时；
- CORS 配置；
- Prometheus HTTP 指标；
- 统一错误映射。

统一错误格式：

```json
{
  "request_id": "req-xxx",
  "code": "TASK_NOT_FOUND",
  "message": "task does not exist",
  "details": {}
}
```

不得把内部 SQL、连接串、Token、堆栈直接返回给客户端。

## 15.2 业务后端工程标准

### 分层

固定调用关系：

```text
Gin Handler
→ Application Service
→ Query Repository / Command Store
→ PostgreSQL
```

要求：

- Handler 只做协议转换、校验和错误映射；
- Service 负责业务规则、租户边界和事务编排；
- Repository 只暴露调用方真正需要的方法；
- 不允许 Handler 直接拼 SQL；
- 不允许为了“分层”创建无意义的一层层转发；
- 领域错误和基础设施错误分开定义。

### API Contract

所有列表接口必须明确：

- 默认排序；
- 支持过滤项；
- Page Size 上限；
- Cursor 语义；
- 空结果格式；
- 非法参数错误；
- 资源不存在与无权访问的返回差异；
- 时间字段全部使用 RFC3339 和 UTC。

### 多租户隔离

- Project ID 从已验证 Token Context 中获得；
- 普通调用方不得从 Body 任意指定其他 Project；
- Repository 查询必须包含 Project 条件；
- 所有跨租户访问测试必须存在；
- 管理接口如需跨 Project，必须使用独立 Admin Scope。

## 15.3 Project API

```text
POST   /api/v1/projects
GET    /api/v1/projects
GET    /api/v1/projects/:project_id
PATCH  /api/v1/projects/:project_id
POST   /api/v1/projects/:project_id/tokens
GET    /api/v1/projects/:project_id/tokens
DELETE /api/v1/projects/:project_id/tokens/:token_id
```

Project 字段：

- Name；
- Status；
- Task Quota；
- Max Concurrent Tasks。

Token Scopes：

```text
task:read
task:write
job:read
job:write
project:admin
```

不实现复杂 RBAC。

Project 更新要求：

- 使用允许字段白名单；
- 不允许 Patch 任意覆盖系统字段；
- 状态从 ACTIVE → DISABLED 后禁止创建新任务；
- 已存在的运行中任务是否继续执行必须写入 Contract；
- Quota 和 Max Concurrent Tasks 必须有数据库约束和应用校验。

## 15.4 Token 流程

创建时：

1. 生成安全随机 Token；
2. 计算 Prefix 用于展示和定位；
3. 使用 Pepper 计算 Hash；
4. 只保存 Hash；
5. 明文仅在创建响应返回一次。

鉴权时：

1. 解析 Bearer Token；
2. 通过 Prefix 缩小候选；
3. 计算 Hash；
4. 常量时间比较；
5. 校验 Disabled、Expires At、Scope；
6. 将 Project ID 注入 Context；
7. 异步或受控更新 Last Used At，不能让其拖慢关键请求。

必须补充：

- Token 轮换；
- 禁用后的立即拒绝；
- 不在日志打印完整 Token；
- Prefix 冲突处理；
- Last Used At 更新失败不能阻断正常鉴权；
- Token 列表只返回元数据。

## 15.5 Job API

```text
POST /api/v1/jobs
GET  /api/v1/jobs
GET  /api/v1/jobs/:job_id
POST /api/v1/jobs/:job_id/cancel
GET  /api/v1/jobs/:job_id/tasks
```

支持：

- 批量创建 Task；
- Idempotency Key；
- Project 过滤；
- 创建时间过滤；
- Derived Status 过滤；
- Cursor Pagination；
- Job Cancel。

批量创建约束：

- 固定最大 Task 数；
- 固定总 Payload 大小；
- 整批语义是“全部成功或全部失败”；
- Job、Task、Outbox 同事务；
- 不允许逐条提交形成半成功；
- 超大批次返回明确错误，不在 Handler 中无限占用内存。

Job 聚合返回：

```text
total
pending
running
succeeded
failed
canceled
derived_status
```

Derived Status：

```text
PENDING
RUNNING
SUCCEEDED
FAILED
CANCELED
PARTIAL
```

聚合规则需要形成纯函数并进行表驱动测试。

## 15.6 Task API

```text
POST /api/v1/tasks
GET  /api/v1/tasks
GET  /api/v1/tasks/:task_id
POST /api/v1/tasks/:task_id/cancel
GET  /api/v1/tasks/:task_id/attempts
GET  /api/v1/tasks/:task_id/result
```

过滤：

- Project；
- Job；
- Status；
- Task Type；
- Priority；
- Created Time；
- Available Time。

稳定排序：

```text
created_at DESC
task_id DESC
```

Cursor 至少编码：

```text
created_at
task_id
```

Cursor 必须签名或进行严格格式校验，非法 Cursor 返回明确错误。

列表响应要求：

- 默认不返回超大 Payload 和 Result；
- 通过详情接口读取完整内容；
- 明确 `next_cursor`；
- Page Size 有上限；
- 相同时间戳下使用 Task ID 保证稳定顺序；
- 新数据插入时游标分页不能出现重复项；
- 查询被 Context 取消后及时终止。

## 15.7 查询、索引与分页工程

这一节必须亲手完成，不能只让 ORM 自动生成查询。

### 必须验证的 PostgreSQL 查询

1. Project 下按状态分页查询 Task；
2. Job 下按创建时间分页查询 Task；
3. 按 Task Type、Status 和 Available At 查找候选任务；
4. 查询运行中且 Lease 已过期的任务；
5. 查询未发布 Outbox；
6. 查询某 Task 的 Attempt 历史。

每个核心查询必须记录：

- SQL；
- 预期索引；
- `EXPLAIN (ANALYZE, BUFFERS)`；
- 是否发生 Seq Scan；
- 返回行数估计是否合理；
- 索引前后差异；
- 数据规模和机器环境。

### Cursor Pagination

必须与 Offset Pagination 做对比：

- 小页码下二者差异；
- 大 Offset 的扫描成本；
- 新数据插入时重复和漏项风险；
- 组合 Cursor 的边界条件。

禁止在简历中只写“使用游标分页优化性能”而没有实验记录。

### 索引纪律

- 每个索引必须对应真实查询；
- 不为所有字段单独建索引；
- 解释联合索引字段顺序；
- 关注写放大与存储成本；
- Migration 中明确创建和回滚；
- 在测试数据量足够时验证执行计划。

## 15.8 连接池、超时与资源保护

必须配置并验证：

- GORM 最大连接数；
- GORM 最大空闲连接数；
- pgxpool 最大/最小连接数；
- Max Lifetime；
- Max Idle Time；
- HTTP Request Timeout；
- 数据库 Statement Timeout；
- 批量创建上限；
- List Page Size 上限；
- Payload 和 Result 大小上限。

必须做两个实验：

1. 人为降低连接池容量并压测，观察等待时间和吞吐；
2. 制造慢查询，验证请求 Context 和数据库超时能够终止查询。

报告必须说明：

- 连接池不是越大越好；
- GORM 和 pgxpool 的总连接预算；
- 请求超时、事务超时和 Worker Lease 时间不能互相矛盾。

## 15.9 创建幂等

唯一范围：

```text
(project_id, idempotency_key)
```

同时保存：

```text
creation_request_hash
```

语义：

- 同 Key + 同 Hash：返回原 Task/Job；
- 同 Key + 不同 Hash：409 Conflict；
- 新 Key：创建资源；
- Task/Job 创建与 `TASK_CREATED` Outbox 同事务。

Hash 必须基于规范化后的请求语义，不能直接依赖 JSON 字段顺序。

优先使用数据库唯一约束作为最终并发防线，不使用仅存在于单进程内存中的锁保证幂等。

## 15.10 创建幂等核心检查点

AI 先生成：

- DTO；
- Handler；
- Service 接口；
- Hash 纯函数；
- pgx 事务测试；
- 并发创建测试；
- 核心事务 TODO。

```bash
git commit -am "chore(p3): scaffold idempotent task and job creation"
git tag p3-create-idempotency-start
git push origin phase/3-business-api
git push origin p3-create-idempotency-start
```

参考实现：

```bash
git commit -am "feat(api): implement idempotent task and job creation"
git tag p3-create-idempotency-reference
git push origin phase/3-business-api
git push origin p3-create-idempotency-reference
```

## 15.11 必须手写复刻

- Creation Request 规范化；
- Hash；
- `INSERT ... ON CONFLICT` 或显式冲突处理；
- 同 Key 同 Hash返回原资源；
- 同 Key 不同 Hash冲突；
- 资源与 Outbox 同事务；
- 两个并发相同 Key 请求只创建一个资源；
- Cursor 编解码和稳定分页；
- 至少一个核心列表查询及其 `EXPLAIN` 分析；
- 跨 Project 访问测试；
- 连接池和查询超时实验。

## 15.12 API 验收

- Project/Token/Job/Task API 完整；
- Handler、Service、Repository 职责清晰；
- Token 明文不入库；
- Scope 生效；
- 跨 Project 访问被拒绝；
- 分页稳定，无重复和漏项；
- 过滤条件正确；
- Page Size、Batch Size 和 Body Size 均有上限；
- 列表接口默认不返回超大字段；
- Body 超限被拒绝；
- 请求和数据库超时生效；
- 创建幂等并发正确；
- Job 聚合状态正确；
- PENDING/RUNNING 取消语义正确；
- 错误格式统一；
- 核心查询存在可复现 `EXPLAIN (ANALYZE, BUFFERS)` 记录；
- Migration、约束与索引有对应测试；
- GORM 和 pgxpool 连接预算不超过 PostgreSQL 限制。

Phase Tag：

```bash
git tag p3-business-api
git push origin main
git push origin p3-business-api
```

---

# Phase 4：Worker gRPC 与执行主链路

## 16. Phase 4 目标

建立 orbit-server 与 orbit-worker 的完整 gRPC 协议，以及 Worker 的有界并发执行主循环。

## 16.1 gRPC 方法

```text
RegisterWorker
Heartbeat
FetchTasks
RenewLease
ReportResult
SetDraining
```

所有 RPC 必须：

- 设置 Deadline；
- 支持 Context 取消；
- 映射稳定业务错误码；
- 验证消息大小；
- 记录延迟和错误指标；
- 传播 Trace/Request 信息；
- 不把数据库错误直接暴露给 Worker。

## 16.2 RegisterWorker

输入：

```text
worker_name
worker_instance_id
hostname
capacity
supported_task_types
process_version
metadata
```

校验：

- UUID 格式；
- Capacity > 0 且不超过上限；
- Task Type 在允许范围；
- 同一 Instance ID 不允许冲突注册；
- 每次进程启动使用新 Instance ID。

## 16.3 Heartbeat

上报：

```text
worker_instance_id
running_tasks
available_capacity
draining
process_uptime
```

Server：

- 更新 Last Heartbeat；
- 不把 Heartbeat 作为 Lease 唯一依据；
- 可以用于展示 Worker 在线状态；
- 过期 Worker 记录不等于立刻强制终止其任务，Task Lease 才是权威。

## 16.4 FetchTasks

Worker 只请求当前空闲槽位数量。Server 再次限制 Batch。

返回至少包含：

```text
task_id
project_id
job_id
task_type
payload
attempt_no
lease_expires_at
execution_timeout
overall_deadline
trace_id
```

## 16.5 Worker 并发模型

使用：

- Root Context；
- Semaphore；
- WaitGroup；
- 有界任务槽位；
- 本地 Task Registry；
- 每任务执行 Context；
- 每任务续租协程；
- Atomic/Mutex；
- 统一错误分类。

主流程：

```text
启动
→ 生成 Worker Instance ID
→ Register
→ Heartbeat Loop
→ Fetch Loop
→ 按可用槽位 Fetch
→ 为每个 Task 注册本地执行状态
→ 占用 Semaphore
→ 启动 Executor
→ 启动 Lease Renew Loop
→ Executor 完成、超时、取消或 Lost Lease
→ 停止 Renew
→ ReportResult，必要时重试
→ 从 Registry 移除
→ 释放 Semaphore
```

## 16.6 必须避免

- 无界 Goroutine；
- 同 Task 启动两个本地执行协程；
- Channel 重复关闭；
- Executor 完成后仍续租；
- Lost Lease 后仍把结果当作成功提交；
- Worker Draining 后继续 Fetch；
- ReportResult 无限重试；
- Shutdown 期间新增任务；
- Heartbeat 和 Registry 数据竞争；
- Fetch 空循环疯狂打数据库。

## 16.7 核心检查点

AI 先生成：

- proto；
- 生成代码；
- gRPC Server Adapter；
- gRPC Client；
- Worker Runtime 结构体；
- Executor 接口；
- Semaphore、Registry 和 Loop 测试骨架；
- `runTask` 留 TODO。

```bash
git commit -am "chore(p4): scaffold grpc worker runtime"
git tag p4-worker-loop-start
git push origin phase/4-worker-runtime
git push origin p4-worker-loop-start
```

参考实现：

```bash
git commit -am "feat(worker): implement bounded fetch execute renew report loop"
git tag p4-worker-loop-reference
git push origin phase/4-worker-runtime
git push origin p4-worker-loop-reference
```

## 16.8 AI 必须讲解

- Root Context、任务 Context、Renew Context 的父子关系；
- Semaphore 与 WaitGroup 各自解决什么问题；
- Registry 为什么需要；
- Lost Lease 时为什么要取消 Executor Context；
- 网络不确定错误与明确 Stale 的区别；
- Renew 周期如何根据 Lease Duration 设置；
- ReportResult 重试如何设置上限；
- Fetch 空结果如何退避；
- Draining 和 Shutdown 的状态转换；
- Goroutine 退出路径和 Channel 所有权。

## 16.9 手写复刻

亲手实现：

- Fetch Loop；
- `runTask`；
- Renew Loop；
- Result Report 重试；
- Local Registry；
- Lost Lease 取消传播；
- Semaphore 释放和 WaitGroup Done 的所有退出路径。

## 16.10 验收

- Capacity=4 时最多执行 4 个任务；
- Fetch Batch 不超过可用容量；
- Renew 正常；
- Cancel Requested 能传递到 Executor；
- Lost Lease 后取消本地执行；
- Report 响应丢失可安全重试；
- Worker 重启产生新 Instance ID；
- 同一 Task 本地不重复执行；
- 空队列时无高频忙轮询；
- Race Detector 通过。

Phase Tag：

```bash
git tag p4-worker-runtime
git push origin main
git push origin p4-worker-runtime
```

---

# Phase 5：Executor、取消、超时与 Graceful Shutdown

## 17. Phase 5 目标

完成可测试的 Mock Executor、受限 HTTP Executor，以及完整的 Worker 优雅退出和异常行为模拟。

## 17.1 Executor 接口

建议语义：

```text
Execute(ctx, task) -> ExecutionResult
```

`ExecutionResult` 包含：

- Outcome；
- Result；
- Result Hash；
- Error Type；
- Error Message；
- Started At；
- Finished At。

Executor 不直接访问 Scheduler Store，不自行修改 Task 状态。

## 17.2 Mock Executor

必须支持通过 Payload 控制：

- 立即成功；
- 延迟成功；
- 永久失败；
- 前 N 次失败后成功；
- 执行超时；
- 忽略 Context；
- 延迟结果上报；
- 执行完成后模拟 Worker 崩溃；
- 超大结果；
- 固定随机种子失败。

Mock Executor 是后续故障注入的主要工具，不是随便返回成功的占位代码。

## 17.3 HTTP Executor

支持：

- GET；
- POST；
- Context；
- 请求超时；
- 429；
- 5xx；
- Idempotency Key Header；
- 请求体大小限制；
- 响应体大小限制；
- 可配置成功码范围；
- 结果存储；
- 安全重定向策略。

### SSRF 防护

至少实现：

- Host 白名单或允许域名列表；
- 拒绝 localhost；
- 拒绝环回、链路本地、私网、组播和保留地址；
- DNS 解析后再次校验 IP；
- 重定向目标重新校验；
- 限制重定向次数；
- 禁止敏感 Header；
- 限制 URL Scheme 为 HTTP/HTTPS；
- 设置独立 Transport 和连接超时；
- 不复用用户提供的任意 Proxy 配置。

需要在文档中明确：应用层防护无法替代网络层隔离，但本项目仍实现合理安全边界。

## 17.4 执行超时

每个任务：

```text
execution_ctx = context.WithTimeout(task_ctx, execution_timeout)
```

Overall Deadline 早于 Execution Timeout 时，应使用更早的 Deadline。

超时后：

- 取消 Executor Context；
- 停止续租；
- Report TIMEOUT；
- Scheduler 决定是否重试。

## 17.5 Graceful Shutdown

收到 SIGTERM：

1. 进程进入 DRAINING；
2. 调用 `SetDraining`；
3. 停止 Fetch；
4. Heartbeat 继续上报 Draining；
5. 已执行任务继续续租；
6. 在 Grace Period 内等待完成；
7. Grace Period 到期后取消 Root/Task Context；
8. 尝试上报 CANCELED/TIMEOUT；
9. 停止 Heartbeat；
10. 停止所有 Renew；
11. WaitGroup 等待；
12. 关闭 gRPC 连接和 Metrics Server；
13. 退出；
14. 未完成任务由 Lease Reaper 回收。

## 17.6 核心检查点

AI 先生成 Graceful Shutdown 状态机、测试时钟和测试骨架，但不实现关键退出顺序。

```bash
git commit -am "chore(p5): scaffold executor shutdown lifecycle"
git tag p5-shutdown-start
git push origin phase/5-executor-shutdown
git push origin p5-shutdown-start
```

参考实现：

```bash
git commit -am "feat(worker): implement executor timeout and graceful shutdown"
git tag p5-shutdown-reference
git push origin phase/5-executor-shutdown
git push origin p5-shutdown-reference
```

## 17.7 手写复刻

- Draining 状态转换；
- 停止 Fetch；
- 等待正在运行任务；
- Grace Period 超时取消；
- Renew Loop 退出；
- WaitGroup 关闭顺序；
- 忽略 Context 的 Executor 如何被隔离；
- Shutdown 测试。

## 17.8 验收

- Mock Executor 所有模式可控；
- HTTP Executor 限制有效；
- 私网和重定向 SSRF 被拒绝；
- Execution Timeout 生效；
- Draining 后不再 Fetch；
- Grace Period 内任务可正常完成；
- 超时后 Context 被取消；
- 忽略 Context 的 Executor 不阻塞进程无限期退出；
- 未完成任务最终被 Reaper 回收；
- 无 Goroutine 泄漏。

Phase Tag：

```bash
git tag p5-executor-shutdown
git push origin main
git push origin p5-executor-shutdown
```

---

# Phase 6：Transactional Outbox、Kafka 与 Audit Consumer

# Phase 6A：事件模型与业务事务写 Outbox

## 18. Phase 6A 目标

所有重要 Task 状态转换与 Outbox Event 必须写入同一个 PostgreSQL 事务。

## 18.1 固定事件

```text
TASK_CREATED
TASK_STARTED
TASK_RETRY_SCHEDULED
TASK_SUCCEEDED
TASK_FAILED
TASK_CANCELED
TASK_LEASE_EXPIRED
JOB_CANCEL_REQUESTED
```

## 18.2 事件格式

```json
{
  "event_id": "uuid",
  "event_type": "TASK_SUCCEEDED",
  "event_version": 1,
  "aggregate_type": "TASK",
  "aggregate_id": "task-id",
  "project_id": "project-id",
  "job_id": "job-id",
  "task_id": "task-id",
  "attempt_no": 3,
  "occurred_at": "timestamp",
  "trace_id": "trace-id",
  "payload": {}
}
```

要求：

- Event ID 由应用生成 UUID；
- Event Version 固定从 1 开始；
- Event Payload 有显式结构和版本；
- Trace ID 从 HTTP/gRPC 上下文传入；
- Event Key 使用 Task ID；
- 事件创建失败必须导致业务事务回滚。

## 18.3 手写复刻

选择至少三个状态转换亲手实现 Outbox 同事务：

- TASK_CREATED；
- TASK_STARTED；
- TASK_SUCCEEDED 或 TASK_RETRY_SCHEDULED。

必须能解释两种错误方案：

```text
Commit DB → Publish Kafka
Publish Kafka → Commit DB
```

以及它们各自为何会丢事件或制造幽灵事件。

---

# Phase 6B：Outbox Relay

## 19. Phase 6B 目标

实现多个 Relay 实例可以安全并发领取 Outbox，并以 At-least-once 语义发布到 Kafka。

## 19.1 Claim 条件

```text
published_at IS NULL
next_attempt_at <= now
并且：
claimed_by IS NULL
或 claim_expires_at <= now
```

Claim 事务：

```text
BEGIN
→ 选择候选事件
→ FOR UPDATE SKIP LOCKED
→ 设置 claimed_by 和 claim_expires_at
→ COMMIT
```

然后在事务外发布 Kafka。

禁止在持有数据库行锁时执行 Kafka 网络调用。

## 19.2 Publish 流程

```text
Claim Batch
→ Commit Claim
→ Publish Kafka
→ 成功：条件标记 published_at，清除 claim
→ 失败：publish_attempts + 1，计算 next_attempt_at，记录 last_error，清除或延长 claim
```

成功标记必须带条件：

- Event ID；
- Claimed By；
- Published At 仍为空。

## 19.3 At-least-once 场景

```text
Kafka 发布成功
→ Relay 在标记 Published 前崩溃
→ Claim 过期
→ 事件被再次发布
```

这是预期语义，不应通过危险的“猜测 Kafka 是否成功”来伪装 Exactly-once。

## 19.4 Topic

```text
orbit.task-events.v1
orbit.task-events.dlq.v1
```

Key：

```text
task_id
```

保证同一 Task 尽量进入同一 Partition，维持单 Task 内顺序。不承诺跨 Task 全局顺序。

## 19.5 核心检查点

```bash
git commit -am "chore(p6): scaffold outbox claiming and kafka publishing"
git tag p6-outbox-relay-start
git push origin phase/6-outbox-kafka
git push origin p6-outbox-relay-start
```

参考实现：

```bash
git commit -am "feat(outbox): implement concurrent claim publish and retry"
git tag p6-outbox-relay-reference
git push origin phase/6-outbox-kafka
git push origin p6-outbox-relay-reference
```

## 19.6 手写复刻

- Claim SQL；
- Claim Lease；
- Kafka 发布与数据库锁分离；
- Mark Published 条件更新；
- Publish Backoff；
- 发布成功后崩溃的重复发布测试；
- 两个 Relay 并发 Claim 测试。

## 19.7 验收

- 两个 Relay 不会同时持有同一事件；
- Claim 进程崩溃后事件可恢复；
- Kafka 不可用时 Outbox 积压且主调度继续；
- Publish Attempts 和 Last Error 正确；
- Kafka 恢复后能够追平；
- 发布成功、标记前崩溃会产生重复消息；
- 不存在持锁等待 Kafka 的行为。

---

# Phase 6C：Audit Consumer、消费幂等与 DLQ

## 20. Phase 6C 目标

实现 Consumer Group：

```text
orbit-audit-consumer
```

处理顺序：

```text
拉取消息
→ 解析和 Schema 校验
→ BEGIN PostgreSQL
→ INSERT audit_event ON CONFLICT(event_id) DO NOTHING
→ COMMIT PostgreSQL
→ 提交 Kafka Offset
```

关键原则：

> 先完成幂等数据库写入，再提交 Kafka Offset。

## 20.1 重复消息

如果 Event ID 已存在：

- 视为已处理；
- 增加 Duplicate 指标；
- 可以提交 Offset；
- 不重复写审计记录。

## 20.2 临时错误

包括：

- PostgreSQL 暂时不可用；
- Kafka 请求暂时失败；
- 网络超时。

处理：

- 不提交 Offset；
- 退避重试；
- 保留消息；
- 不立即写 DLQ。

## 20.3 永久错误

包括：

- JSON/Protobuf Schema 无法解析；
- 必要字段缺失；
- Event Version 不支持；
- Event Type 非法；
- Payload 永久不符合约束。

达到处理限制后：

1. 将原消息和错误元数据发布到 DLQ；
2. 记录原 Topic、Partition、Offset；
3. 确认 DLQ 发布成功；
4. 提交原消息 Offset；
5. 增加 DLQ 指标。

## 20.4 核心检查点

```bash
git commit -am "chore(p6): scaffold idempotent audit consumer"
git tag p6-consumer-start
git push origin phase/6-outbox-kafka
git push origin p6-consumer-start
```

参考实现：

```bash
git commit -am "feat(audit): implement idempotent consumption and dlq"
git tag p6-consumer-reference
git push origin phase/6-outbox-kafka
git push origin p6-consumer-reference
```

## 20.5 AI 必须讲解

- 为什么 Offset 必须在 DB Commit 后提交；
- DB 已成功但 Offset 未提交时为何会重投；
- Event ID 唯一约束如何消除重复审计记录；
- 为什么 Consumer 仍然是 At-least-once；
- Poison Message 为什么不能无限阻塞 Partition；
- DLQ 发布失败时为什么不能提交原 Offset；
- 同一 Task 的 Partition 顺序和数据库幂等分别解决什么问题；
- Consumer Rebalance 期间如何安全退出处理循环。

## 20.6 手写复刻

- Idempotent Insert；
- DB Commit 与 Offset Commit 顺序；
- 重复消息路径；
- DB Commit 后模拟崩溃；
- Poison Message 计数；
- DLQ 生产；
- Rebalance/Shutdown 中的 Context 退出。

## 20.7 Phase 6 验收

- 业务事务与 Outbox 原子；
- Relay At-least-once；
- Consumer 幂等；
- DLQ 正常；
- Kafka 不可用不影响调度；
- Outbox 积压可恢复；
- Consumer 重启可恢复；
- 重复消息只产生一条 Audit Event；
- Event 顺序测试通过；
- Outbox 定期清理机制存在。

Phase Tag：

```bash
git tag p6-outbox-kafka
git push origin main
git push origin p6-outbox-kafka
```

---

# Phase 7：Prometheus、Testcontainers、Race 与 CI

## 21. Phase 7 目标

使项目不只“能运行”，还能够被观察、被自动验证、被重复部署和被持续回归。

## 21.1 Orbit Server 指标

- HTTP 请求数；
- HTTP 延迟；
- HTTP 错误；
- Task 创建数；
- PENDING 当前数量；
- RUNNING 当前数量；
- Success/Failed/Canceled；
- Retry；
- Lease Expired；
- Stale Result；
- Idempotency Conflict；
- Fetch DB Latency；
- Renew Lease Latency；
- Report Result Latency；
- Reaper Duration；
- Reaper Processed；
- 在线 Worker；
- Draining Worker；
- pgx Pool 使用率；
- GORM 查询错误。

## 21.2 Worker 指标

- Running Tasks；
- Available Capacity；
- Fetch 次数；
- Fetch 空结果；
- Executor Duration；
- Executor Error；
- Renew Success/Failure；
- Lost Lease；
- Result Report Retry；
- Graceful Shutdown Duration。

## 21.3 Relay 指标

- Unpublished Outbox Count；
- Oldest Unpublished Age；
- Publish Success/Failure；
- Publish Attempts；
- Claim Batch Size；
- Kafka Publish Latency。

## 21.4 Consumer 指标

- Consumed Total；
- Duplicate Total；
- Processing Failure；
- DLQ Total；
- Consumer Lag；
- DB Write Latency。

禁止将以下作为 Prometheus Label：

```text
Task ID
Job ID
Project ID
Event ID
Worker Instance ID
```

避免高基数。

## 21.5 Testcontainers

集成测试必须使用真实：

- PostgreSQL；
- Kafka；
- MySQL 8（仅运行独立 Lab 测试，不启动生产进程）。

流程：

```text
启动容器
→ 等待健康
→ 执行 Migration
→ 创建 Topic
→ 运行测试
→ 输出必要日志
→ 清理容器
```

禁止：

- 使用 SQLite 模拟 PostgreSQL 锁和事务；
- 使用内存 Channel 代替 Kafka 集成测试；
- 把所有集成测试写成依赖本机固定端口；
- 让并发测试依赖 `time.Sleep` 猜测时序。

应使用：

- Barrier；
- WaitGroup；
- Channel；
- 可注入 Clock；
- Eventually 断言；
- 明确超时。

## 21.6 测试分类

```text
unit
integration
concurrency
fault
performance
```

通过 build tag 或 Makefile 命令控制耗时测试。

示例：

```bash
make test-unit
make test-integration
make test-race
make test-fault
make test-all
```

## 21.7 CI

至少包含：

1. 格式检查；
2. 静态检查；
3. 单元测试；
4. Race Detector；
5. PostgreSQL 集成测试；
6. Kafka 集成测试；
7. MySQL Lab 独立测试；
8. 构建四个生产二进制，并验证其不依赖 MySQL；
9. PostgreSQL 与 MySQL Migration 分别从零执行；
10. Docker 镜像构建；
11. 上传测试报告或保留失败日志。

性能测试不必每次 PR 全量运行，可做手动 Workflow 或夜间任务。

## 21.8 推荐提交

```text
feat(metrics): add server worker relay and consumer metrics
feat(testkit): add postgres and kafka testcontainers
refactor(tests): remove timing sleeps from concurrency tests
test(integration): cover scheduler outbox and consumer flows
ci: add lint race integration and image build jobs
```

Tag：

```bash
git tag p7-observability-verification
git push origin main
git push origin p7-observability-verification
```

## 21.9 验收

- Metrics 可抓取；
- 无高基数标签；
- PostgreSQL/Kafka 容器测试可独立运行；
- Race Detector 通过；
- CI 从干净环境通过；
- Integration Tests 不依赖本地已有数据库；
- 失败测试能输出足够诊断信息。

---

# Phase 8：故障注入与性能验证

## 22. Phase 8 目标

通过主动制造故障证明系统的语义，而不是只展示正常流程。

## 22.1 故障注入清单

必须覆盖：

1. Worker 执行中 `kill -9`；
2. Worker 暂停超过 Lease；
3. 旧 Worker 恢复后提交旧 Attempt；
4. PostgreSQL 短暂不可用；
5. Server 在 Claim 事务前退出；
6. Server 在 Claim 事务提交后、响应前退出；
7. Result 事务成功但响应丢失；
8. Reaper 与 Complete 同时运行；
9. Cancel 与 Complete 同时运行；
10. Kafka 不可用；
11. Relay 发布成功后崩溃；
12. Consumer 数据库提交后、Offset 提交前崩溃；
13. Consumer 收到重复事件；
14. Poison Message；
15. Worker 忽略 Context；
16. Graceful Shutdown 超时；
17. 两个 Relay 同时 Claim；
18. Consumer Rebalance；
19. Outbox 大量积压后 Kafka 恢复；
20. PostgreSQL 连接池耗尽。

## 22.2 每个故障实验的固定记录

在 `docs/failure-cases.md` 中记录：

```text
实验名称
前置状态
注入点
操作命令
预期行为
实际日志
数据库状态
Prometheus 指标变化
恢复过程
是否符合 At-least-once 语义
发现的问题
修复提交
```

## 22.3 核心验收场景

### 场景一：旧 Worker 被拒绝

```text
Worker A → Attempt 1
Lease 过期
Worker B → Attempt 2
Worker B 成功
Worker A 使用 Attempt 1 提交
Server 返回 STALE_LEASE
```

### 场景二：Claim 响应丢失

```text
数据库已设 RUNNING
Worker 未收到任务
Lease 到期
Reaper 回收
新 Worker 再次领取
```

### 场景三：Result 响应丢失

```text
数据库已经 SUCCEEDED
Worker 未收到响应
Worker 重复提交
Server 根据 Worker + Attempt + Result Hash 幂等返回成功
```

### 场景四：Reaper 与 Complete 竞态

```text
两者同时条件更新
只有一方成功
终态不回退
```

### 场景五：Outbox 重复发布

```text
Kafka 发布成功
Relay 标记前崩溃
事件再次发布
Audit Consumer 通过 Event ID 去重
```

### 场景六：Consumer Offset 未提交

```text
Audit DB 已写入
Offset 未提交
Kafka 重投
唯一约束识别重复
```

### 场景七：Kafka 长时间不可用

```text
任务创建、领取、执行继续
Outbox 积压
Kafka 恢复
Relay 追平
审计事件不丢失但可能重复投递
```

## 22.4 性能测试维度

### HTTP API

- Task 单条创建；
- Task 批量创建；
- Job 创建；
- Task 列表；
- Attempt 查询。

### gRPC

- Fetch；
- Renew；
- ReportResult；
- 不同 Batch Size；
- 不同 Worker 数量；
- 不同 Worker Capacity。

### Workload

- 短任务；
- 长任务；
- 高失败率任务；
- 大 Payload；
- 大 Result；
- Kafka 正常；
- Kafka 不可用；
- Consumer 落后。

## 22.5 记录指标

- Task 创建吞吐；
- Fetch P50/P95/P99；
- Queue Delay；
- Execution Duration；
- End-to-End Completion Latency；
- PostgreSQL CPU；
- PostgreSQL I/O；
- 锁等待；
- 活跃连接；
- pgx Pool Wait；
- GORM Pool Wait；
- Outbox 积压；
- Oldest Unpublished Age；
- Kafka Publish 吞吐；
- Audit Consumer 吞吐；
- Consumer Lag；
- 单 Scheduler 饱和点。

## 22.6 性能报告必须回答

1. Worker 增加到多少时吞吐仍线性或近线性增长；
2. 何时 PostgreSQL 成为瓶颈；
3. Fetch Batch Size 如何影响吞吐和公平性；
4. Lease Duration/Renew Interval 如何影响数据库写负载；
5. Outbox Relay 是否影响主库；
6. Kafka 不可用时 Outbox 每分钟增长多少；
7. Kafka 恢复后 Relay 多久追平；
8. Consumer 如何追平积压；
9. 哪些索引对性能最关键；
10. 两套数据库连接池应如何设置。

## 22.7 性能实验原则

- 固定硬件、容器配额和软件配置；
- 每组测试预热；
- 至少运行多轮；
- 输出原始数据；
- 不只截一张最好看的图；
- 区分数据库、Worker 和 Kafka 瓶颈；
- 对优化前后使用相同工作负载；
- 所有优化必须有基线对比。

## 22.8 推荐提交

```text
test(fault): add worker lease and stale result scenarios
test(fault): add relay and consumer crash recovery scenarios
perf(load): add reproducible http and grpc workloads
perf(report): document scheduler postgres and kafka bottlenecks
fix: resolve failures discovered by fault injection
```

Tag：

```bash
git tag p8-fault-performance
git push origin main
git push origin p8-fault-performance
```

---

# Phase 8B：MySQL 8 工程实验与 PostgreSQL 对比

## 22B. Phase 8B 目标

在不改变 Orbit 生产架构的前提下，建立一个独立、真实、可复现的 MySQL 8 工程实验模块。

本 Phase 解决的不是“给技术栈列表多写一个 MySQL”，而是回答以下问题：

- 是否真正会设计 MySQL 表和索引；
- 是否理解 InnoDB、聚簇索引和二级索引；
- 是否能使用 `EXPLAIN ANALYZE` 分析查询；
- 是否理解 MVCC 和隔离级别；
- 是否能解释行锁、Gap Lock 和 Next-Key Lock；
- 是否能复现死锁并实现安全的有限重试；
- 是否能在 MySQL 8 中正确使用 `FOR UPDATE SKIP LOCKED`；
- 是否能说明为什么 Orbit 生产主库仍选择 PostgreSQL。

固定原则：

```text
MySQL Lab 是必做求职增强模块
≠
Orbit 生产系统改成 MySQL
≠
PostgreSQL/MySQL 双写
```

## 22B.1 目录与依赖隔离

最终目录：

```text
experiments/mysql8/
├── cmd/mysql8-lab/
│   └── main.go
├── internal/
│   ├── model/
│   ├── repository/
│   ├── querylab/
│   ├── locklab/
│   ├── retry/
│   └── testkit/
└── README.md

migrations/mysql8/
tests/mysql8/
docs/mysql-vs-postgresql.md
docs/mysql-index-and-lock-lab.md
```

依赖规则：

- `experiments/mysql8` 可以依赖共享的纯领域类型，但不能依赖 `internal/pgstore`；
- 生产 `cmd/` 不得导入 MySQL Lab；
- MySQL Lab 不读取生产 `DATABASE_URL`；
- 测试默认启动 MySQL Testcontainer；
- 不要求开发者本机安装 MySQL；
- MySQL Migration 与 PostgreSQL Migration 分目录维护；
- 不建立“同一个 Migration 同时兼容两个数据库”的虚假抽象。

## 22B.2 MySQL 实验 Schema

使用简化但真实的表：

```text
lab_projects
lab_tasks
lab_task_attempts
lab_idempotency_records
```

### lab_projects

```text
id                 binary(16) primary key
name               varchar(128) not null
status             varchar(32) not null
created_at         datetime(6) not null
updated_at         datetime(6) not null
```

### lab_tasks

```text
id                 binary(16) primary key
project_id         binary(16) not null
idempotency_key    varchar(128) null
request_hash       binary(32) null
status             varchar(32) not null
priority           int not null
available_at       datetime(6) not null
attempt_no         int not null default 0
lease_owner        binary(16) null
lease_expires_at   datetime(6) null
payload            json not null
result             json null
created_at         datetime(6) not null
updated_at         datetime(6) not null
```

约束和索引至少包括：

```text
PRIMARY KEY(id)
UNIQUE(project_id, idempotency_key)
INDEX idx_task_list(project_id, status, created_at DESC, id DESC)
INDEX idx_task_fetch(status, available_at, priority DESC, id)
INDEX idx_task_lease(status, lease_expires_at)
```

注意：

- UUID 存储方式必须形成一份说明，可选 `BINARY(16)`，不能不加解释地使用超长字符串；
- 所有表使用 InnoDB；
- 字符集固定为 `utf8mb4`；
- 时间精度固定；
- JSON 字段的查询边界需要说明；
- MySQL 对 `CHECK` 的行为、版本要求和应用校验边界需要记录；
- 不使用 GORM AutoMigrate 代替正式 Migration。

## 22B.3 Push 8B.1：基础环境、Migration 与 CRUD

分支：

```text
lab/mysql8-foundation
```

实现：

- MySQL 8 Testcontainer；
- 独立配置；
- Migration Up/Down；
- GORM 普通 CRUD；
- `database/sql` 原生查询入口；
- 基础连接池；
- 测试数据生成器；
- 健康检查；
- 资源关闭。

必须测试：

- 从空库执行全部 Migration；
- Migration 重复执行行为明确；
- CRUD 能够处理 Context 取消；
- JSON、时间和 UUID 编解码正确；
- 唯一约束冲突能映射为稳定领域错误；
- 测试结束无连接泄漏。

推荐提交：

```text
chore(mysql-lab): initialize isolated mysql8 experiment
feat(mysql-lab): add versioned schema and repositories
test(mysql-lab): cover migration crud and constraints
```

Tag：

```bash
git tag p8b-mysql-foundation
git push origin lab/mysql8-foundation
git push origin p8b-mysql-foundation
```

## 22B.4 Push 8B.2：联合索引、覆盖索引与查询计划

建立可控数据集：

- 至少多个 Project；
- 多种 Status；
- 不同创建时间；
- 足以让优化器区分索引扫描与全表扫描的数据量；
- 数据生成过程可复现，固定随机种子。

实验查询：

```sql
SELECT id, status, priority, created_at
FROM lab_tasks
WHERE project_id = ?
  AND status = ?
  AND (created_at < ? OR (created_at = ? AND id < ?))
ORDER BY created_at DESC, id DESC
LIMIT ?;
```

必须对比：

1. 无合适索引；
2. 只有单列索引；
3. 联合索引；
4. 覆盖索引；
5. 错误字段顺序的联合索引。

使用：

```sql
EXPLAIN ANALYZE
```

报告记录：

- access type；
- possible keys；
- actual key；
- rows examined；
- filtered；
- extra；
- 实际耗时；
- 返回行数；
- 数据规模；
- 机器环境。

必须解释：

- 聚簇索引和二级索引；
- 回表；
- 覆盖索引；
- 最左前缀；
- 为什么查询字段、过滤字段和排序字段共同影响索引设计；
- 为什么索引越多写入越慢。

### Offset 与 Cursor 对比

使用同一数据集比较：

```sql
LIMIT 50 OFFSET 100000
```

与 Cursor Pagination。

必须记录：

- 延迟；
- 扫描行数；
- 新数据插入情况下的稳定性；
- 游标边界；
- 是否出现重复或漏项。

手写要求：

- Cursor 编解码；
- 查询 SQL；
- 索引 Migration；
- 对比报告；
- 不允许只复制结论。

推荐提交：

```text
feat(mysql-lab): add indexed cursor pagination experiment
perf(mysql-lab): compare offset cursor and covering indexes
docs(mysql-lab): record explain analyze evidence
```

## 22B.5 Push 8B.3：MVCC 与隔离级别

分别使用：

```text
READ COMMITTED
REPEATABLE READ
```

完成以下实验：

### 实验一：不可重复读

- 事务 A 第一次读取；
- 事务 B 更新并提交；
- 事务 A 第二次读取；
- 比较两个隔离级别下结果。

### 实验二：快照读与当前读

对比：

```sql
SELECT ...
SELECT ... FOR UPDATE
```

说明：

- 一致性读；
- 当前读；
- Read View；
- 为什么加锁读不等于普通快照读。

### 实验三：幻读相关行为

构造范围条件，在另一个事务插入区间内记录，观察：

- 普通快照读；
- `SELECT ... FOR UPDATE`；
- READ COMMITTED；
- REPEATABLE READ。

报告必须区分：

- SQL 标准概念；
- MySQL InnoDB 实际行为；
- 一致性读与锁定读；
- 不要用一句“MVCC 解决幻读”概括所有场景。

推荐提交：

```text
test(mysql-lab): demonstrate read committed and repeatable read
docs(mysql-lab): explain snapshot and locking reads
```

## 22B.6 Push 8B.4：行锁、Gap Lock 与 Next-Key Lock

必须通过两个独立连接和明确事务时序复现：

1. 主键等值更新产生记录锁；
2. 唯一索引等值查询的锁行为；
3. 非唯一索引范围查询；
4. 无合适索引时锁范围扩大；
5. Gap Lock；
6. Next-Key Lock；
7. 插入意向锁冲突；
8. READ COMMITTED 与 REPEATABLE READ 下的差异。

实验记录必须包含：

- 事务 A SQL；
- 事务 B SQL；
- 开始、阻塞、提交时间；
- 隔离级别；
- 使用的索引；
- 为什么阻塞；
- 如何通过查询或系统视图观察锁等待；
- 如何减小锁范围。

禁止：

- 只写理论笔记而不执行实验；
- 用一个进程内 Mutex 模拟数据库锁；
- 在没有索引说明的情况下讨论 Gap Lock。

## 22B.7 Push 8B.5：死锁复现与有限重试

### 固定死锁场景

事务 A：

```text
锁 Task 1
→ 等待
→ 尝试锁 Task 2
```

事务 B：

```text
锁 Task 2
→ 等待
→ 尝试锁 Task 1
```

要求：

- 稳定复现 MySQL 死锁；
- 识别 MySQL Deadlock 错误；
- 事务整体回滚；
- 使用有限次数重试；
- 指数退避和随机抖动；
- Context 取消立即停止；
- 非死锁错误不重试；
- 每次重试创建全新事务；
- 记录重试次数指标和日志。

不允许：

```text
for {
    retry()
}
```

核心实现前：

```bash
git commit -am "chore(mysql-lab): scaffold deadlock retry experiment"
git tag p8b-mysql-deadlock-start
git push origin lab/mysql8-deadlock
git push origin p8b-mysql-deadlock-start
```

参考实现后：

```bash
git commit -am "feat(mysql-lab): implement bounded deadlock retry"
git tag p8b-mysql-deadlock-reference
git push origin lab/mysql8-deadlock
git push origin p8b-mysql-deadlock-reference
```

手写复刻：

```bash
git switch -c learn/8b-mysql-deadlock p8b-mysql-deadlock-start
```

必须能够解释：

- 数据库为什么选择回滚其中一个事务；
- 为什么统一加锁顺序能够减少死锁；
- 为什么减少事务范围有帮助；
- 为什么重试必须有上限；
- 为什么业务幂等是重试安全的前提之一。

## 22B.8 Push 8B.6：MySQL `FOR UPDATE SKIP LOCKED`

实现一个简化版任务领取，不替换 Orbit 的 PostgreSQL 实现。

事务流程：

```text
BEGIN
→ SELECT PENDING TASKS
→ ORDER BY priority DESC, available_at ASC, id ASC
→ FOR UPDATE SKIP LOCKED
→ LIMIT batch
→ UPDATE RUNNING / attempt_no + 1 / lease owner
→ INSERT attempt
→ COMMIT
```

要求：

- 多个 Goroutine 使用独立连接并发领取；
- 同一任务不能被两个领取者同时领取；
- Attempt No 单调递增；
- 排序稳定；
- 空批次正确返回；
- 事务失败回滚；
- Context 超时；
- 连接池容量有上限；
- 不使用进程全局锁规避并发。

核心实现前：

```bash
git commit -am "chore(mysql-lab): scaffold skip locked fetch"
git tag p8b-mysql-skip-locked-start
git push origin lab/mysql8-skip-locked
git push origin p8b-mysql-skip-locked-start
```

参考实现后：

```bash
git commit -am "feat(mysql-lab): implement skip locked task fetch"
git tag p8b-mysql-skip-locked-reference
git push origin lab/mysql8-skip-locked
git push origin p8b-mysql-skip-locked-reference
```

手写复刻：

```bash
git switch -c learn/8b-mysql-skip-locked p8b-mysql-skip-locked-start
```

必须比较：

- MySQL 版本与 PostgreSQL 版本 SQL；
- 默认隔离级别；
- 锁行为；
- 时间函数；
- UUID 存储；
- `RETURNING` 能力差异及其对实现的影响；
- 错误码和重试策略；
- 查询计划和索引设计。

## 22B.9 Push 8B.7：唯一约束幂等实验

使用：

```text
(project_id, idempotency_key)
```

实现：

- 新 Key 创建；
- 同 Key + 同 Request Hash 返回原资源；
- 同 Key + 不同 Hash 返回冲突；
- 多 Goroutine 并发相同 Key；
- 唯一键冲突错误识别；
- 事务回滚；
- 不使用 Redis 锁；
- 不使用仅存在于单进程的 Mutex 作为最终防线。

与 PostgreSQL 版本对比：

- Upsert/Conflict 语法；
- 返回已有记录的方法；
- 错误码识别；
- 事务重试；
- 唯一索引与 NULL 行为。

## 22B.10 PostgreSQL 与 MySQL 对比文档

必须输出：

```text
docs/mysql-vs-postgresql.md
```

至少回答：

1. 为什么 Orbit 主系统选择 PostgreSQL；
2. MySQL 默认隔离级别与 PostgreSQL 默认隔离级别；
3. 两者 MVCC 的概念差异；
4. 行锁、Gap Lock、Next-Key Lock；
5. `FOR UPDATE SKIP LOCKED` 实现差异；
6. `RETURNING`、Upsert 和错误处理差异；
7. JSON 类型使用差异；
8. UUID 存储选择；
9. 索引组织和聚簇方式；
10. 查询计划工具；
11. 死锁识别与重试；
12. 连接池配置；
13. 哪些业务更常选择 MySQL；
14. 哪些系统能力使 PostgreSQL 更适合当前调度内核；
15. 为什么不做生产双写。

文档必须基于本项目实验，不得只复制通用八股。

## 22B.11 MySQL Lab 测试矩阵

至少包含：

- Migration Up/Down；
- GORM CRUD；
- 原生 SQL；
- UUID 编解码；
- JSON 字段；
- 唯一约束；
- 同 Key 幂等；
- Cursor Pagination；
- Offset 大页码；
- 联合索引；
- 覆盖索引；
- `EXPLAIN ANALYZE`；
- READ COMMITTED；
- REPEATABLE READ；
- 快照读；
- 当前读；
- 行锁；
- Gap Lock；
- Next-Key Lock；
- 死锁；
- 死锁有限重试；
- Context 取消；
- `SKIP LOCKED`；
- 多领取者并发；
- 连接池耗尽；
- 服务端超时；
- 容器重启。

命令：

```bash
make test-mysql-lab
make test-mysql-locks
make test-mysql-concurrency
make report-mysql-explain
```

## 22B.12 AI Vibe Coding 边界

AI 可以生成：

- 目录和配置骨架；
- Testcontainer 辅助代码；
- Migration Runner；
- GORM Model 和普通 CRUD；
- 数据生成器；
- 报告模板；
- 测试结构。

必须亲手复刻：

- 索引设计和核心查询；
- Cursor Pagination；
- 隔离级别实验；
- 锁实验；
- 死锁重试；
- `SKIP LOCKED`；
- 唯一约束幂等；
- PostgreSQL/MySQL 对比结论。

禁止 AI：

- 修改 Orbit 生产数据库配置；
- 把 MySQL 注入 orbit-server；
- 创建生产双写；
- 为了测试通过使用全局锁；
- 删除阻塞或并发测试；
- 用 SQLite 代替 MySQL；
- 编造 `EXPLAIN`、延迟和吞吐数据；
- 把理论结论写成已经实验验证的结果。

## 22B.13 Phase 8B 验收

- MySQL 8 Testcontainer 可独立启动；
- Migration 从空库执行成功；
- 生产四个二进制完全不依赖 MySQL；
- CRUD、事务和约束测试通过；
- Cursor 与 Offset 对比可复现；
- 联合索引和覆盖索引存在真实查询计划证据；
- 两种隔离级别实验通过；
- 行锁、Gap Lock、Next-Key Lock 实验有时序记录；
- 死锁可以稳定复现；
- 有限重试正确处理 Context 和非死锁错误；
- MySQL `SKIP LOCKED` 并发领取不重复；
- 幂等并发只创建一条记录；
- `docs/mysql-vs-postgresql.md` 完成；
- 所有核心 Tag 已 Push；
- 至少完成两个学习分支；
- 简历描述明确这是独立 MySQL 工程实验。

Phase Tag：

```bash
git tag p8b-mysql-engineering-lab
git push origin main
git push origin p8b-mysql-engineering-lab
```

---

# Phase 9：文档、演示、简历与 v1.0.0

## 23. Phase 9 目标

把“能跑的仓库”整理成别人可以理解、复现和面试追问的完整项目。

## 23.1 README 必须包含

- 一句话项目定位；
- 项目解决的问题；
- At-least-once 语义；
- 不保证什么；
- 架构图；
- 四个进程；
- 核心状态机；
- 快速启动；
- 创建 Project/Token/Task；
- 启动 Worker；
- 查看 Attempt；
- 查看 Kafka/Audit；
- Prometheus 指标；
- 运行测试；
- 故障演示；
- 性能结果；
- 项目边界；
- MySQL Lab 的独立定位和运行命令；
- PostgreSQL/MySQL 对比结论；
- 学习检查点说明。

## 23.2 必须独立完成的文档

```text
docs/architecture.md
docs/state-machine.md
docs/execution-semantics.md
docs/failure-cases.md
docs/performance-report.md
docs/database-and-locking.md
docs/outbox-and-kafka.md
docs/worker-lifecycle.md
docs/business-api-and-querying.md
docs/mysql-vs-postgresql.md
docs/mysql-index-and-lock-lab.md
docs/learning-notes/index.md
```

## 23.3 架构图

至少包含：

1. 总体组件图；
2. Task 状态机；
3. FetchTasks 事务时序；
4. Worker Execute/Renew/Report 时序；
5. Lease Expiry 与 Fencing 时序；
6. Outbox Relay 时序；
7. Consumer 幂等与 Offset 时序；
8. Graceful Shutdown 时序。

## 23.4 演示脚本

演示时按以下顺序：

1. `docker compose up`；
2. Migration；
3. 启动 Server；
4. 创建 Project 和 Token；
5. 创建 Job/Task；
6. 启动多个 Worker；
7. 展示 Task 被原子领取；
8. Kill Worker；
9. 展示 Lease Expire 和新 Attempt；
10. 恢复旧 Worker 并展示 Stale Result；
11. 展示 Outbox/Kafka/Audit；
12. 停止 Kafka，展示调度继续和 Outbox 积压；
13. 恢复 Kafka，展示追平；
14. 查看 Prometheus；
15. 运行关键测试；
16. 启动 MySQL Lab，展示索引查询计划；
17. 展示死锁重试和 `SKIP LOCKED` 并发领取；
18. 强调 MySQL 不参与 Orbit 生产运行。

## 23.5 面试必须能讲清的问题

- 为什么 Pull 而不是 Push；
- 为什么 PostgreSQL 是 Task 权威状态；
- `SKIP LOCKED` 如何避免重复领取；
- Lease 和 Heartbeat 的区别；
- Attempt No 为什么是 Fencing Token；
- 为什么仍是 At-least-once；
- 如何处理 Result 响应丢失；
- Reaper 与 Complete 如何避免状态回退；
- GORM 和 pgx 为什么分工；
- Outbox 为什么不是分布式事务；
- Kafka 重复投递如何处理；
- Consumer 为什么先写 DB 再提交 Offset；
- Kafka 不可用为何不影响核心调度；
- PostgreSQL 何时成为瓶颈；
- Worker Graceful Shutdown 如何保证安全；
- HTTP Executor 如何防 SSRF；
- 为什么生产主库选择 PostgreSQL，而不是因为 JD 改成 MySQL；
- MySQL InnoDB 聚簇索引和二级索引如何工作；
- MySQL READ COMMITTED 与 REPEATABLE READ 有什么差异；
- Gap Lock 和 Next-Key Lock 在什么场景出现；
- 如何识别并安全重试 MySQL 死锁；
- MySQL 与 PostgreSQL 的 `SKIP LOCKED` 实现有什么差异。

## 23.6 Release 前检查

```bash
gofmt -w .
go vet ./...
go test ./...
go test -race ./...
make test-integration
make test-fault
make build
make compose-smoke
make test-mysql-lab
make test-mysql-concurrency
```

检查：

- 无明文 Token；
- 无真实密钥；
- `.env` 未提交；
- README 命令可复现；
- Migration 从空库通过；
- Topic 自动创建或有初始化脚本；
- 所有核心 Tag 已推送；
- 性能报告包含环境；
- 故障实验可重复；
- MySQL Lab 与生产依赖隔离；
- MySQL 查询计划和锁实验可重复；
- CI 全绿。

## 23.7 发布

```bash
git tag -a v1.0.0 -m "Orbit Scheduler v1.0.0"
git push origin main
git push origin v1.0.0
```

Release 附件或说明包括：

- 架构摘要；
- 快速启动；
- 关键语义；
- 测试结果；
- 性能结论；
- 已知限制。

---

# 24. HTTP API 详细约定

## 24.1 通用请求头

```text
Authorization: Bearer <token>
X-Request-ID: optional
Idempotency-Key: creation endpoints
Content-Type: application/json
```

## 24.2 通用响应

成功响应建议统一：

```json
{
  "request_id": "req-xxx",
  "data": {},
  "meta": {}
}
```

错误响应：

```json
{
  "request_id": "req-xxx",
  "code": "IDEMPOTENCY_CONFLICT",
  "message": "the same idempotency key was used with a different request",
  "details": {}
}
```

## 24.3 主要错误码

```text
INVALID_ARGUMENT
UNAUTHENTICATED
PERMISSION_DENIED
PROJECT_NOT_FOUND
JOB_NOT_FOUND
TASK_NOT_FOUND
TOKEN_EXPIRED
TOKEN_DISABLED
SCOPE_REQUIRED
IDEMPOTENCY_CONFLICT
TASK_ALREADY_FINAL
TASK_NOT_CANCELABLE
PAYLOAD_TOO_LARGE
RESULT_TOO_LARGE
RATE_LIMITED
INTERNAL_ERROR
SERVICE_UNAVAILABLE
```

调度/gRPC 错误：

```text
WORKER_NOT_REGISTERED
WORKER_DRAINING
STALE_LEASE
LEASE_EXPIRED
RESULT_CONFLICT
TASK_CANCELED
TASK_ALREADY_FINAL
CAPACITY_EXCEEDED
UNSUPPORTED_TASK_TYPE
```

## 24.4 创建 Task 请求示例

```json
{
  "project_id": "uuid",
  "job_id": "uuid-or-null",
  "task_type": "http",
  "payload": {},
  "priority": 10,
  "available_at": "2026-01-01T00:00:00Z",
  "execution_timeout_seconds": 60,
  "overall_deadline": null,
  "max_attempts": 5
}
```

创建响应需要明确是否为幂等重放：

```json
{
  "data": {
    "task": {},
    "idempotent_replay": false
  }
}
```

---

# 25. Worker gRPC 约定

## 25.1 Deadline 建议

每个 RPC 均由配置控制，不能写死在业务逻辑中。原则：

- Register/Heartbeat 较短；
- Fetch 允许短轮询或普通请求；
- Renew 必须显著短于 Lease 剩余时间；
- ReportResult 可比 Renew 略长；
- 不使用无限 Deadline。

## 25.2 Renew 时间关系

应满足：

```text
renew_interval
<
lease_duration / 2
```

并为：

- 网络抖动；
- 一次临时数据库故障；
- 重试；

预留安全余量。

Worker 不应在 Lease 已明确丢失后继续尝试将结果作为有效提交。

## 25.3 业务错误映射

- 明确 Stale：`FailedPrecondition`；
- 不存在：`NotFound`；
- 未注册：`Unauthenticated` 或 `FailedPrecondition`；
- 参数错误：`InvalidArgument`；
- 临时数据库故障：`Unavailable`；
- Deadline：`DeadlineExceeded`；
- 结果冲突：`AlreadyExists` 或 `Aborted`，项目内固定一种映射。

---

# 26. 配置清单

## 26.1 Server

```text
HTTP_ADDR
GRPC_ADDR
METRICS_ADDR
DATABASE_URL
GORM_MAX_OPEN_CONNS
GORM_MAX_IDLE_CONNS
PGX_MAX_CONNS
PGX_MIN_CONNS
SERVER_MAX_FETCH_BATCH
DEFAULT_LEASE_DURATION
MAX_LEASE_DURATION
REAPER_INTERVAL
REAPER_BATCH_SIZE
REQUEST_TIMEOUT
MAX_REQUEST_BODY_BYTES
MAX_TASK_PAYLOAD_BYTES
MAX_TASK_RESULT_BYTES
TOKEN_PEPPER
TOKEN_LENGTH_BYTES
WORKER_HEARTBEAT_TIMEOUT
```

## 26.2 Worker

```text
ORBIT_GRPC_TARGET
WORKER_NAME
WORKER_CAPACITY
WORKER_SUPPORTED_TASK_TYPES
WORKER_PROCESS_VERSION
FETCH_MAX_BATCH
FETCH_EMPTY_BACKOFF_MIN
FETCH_EMPTY_BACKOFF_MAX
RPC_REGISTER_TIMEOUT
RPC_FETCH_TIMEOUT
RPC_RENEW_TIMEOUT
RPC_REPORT_TIMEOUT
RENEW_INTERVAL
SHUTDOWN_GRACE_PERIOD
METRICS_ADDR
HTTP_EXECUTOR_ALLOWED_HOSTS
HTTP_EXECUTOR_MAX_REQUEST_BYTES
HTTP_EXECUTOR_MAX_RESPONSE_BYTES
```

## 26.3 Relay

```text
DATABASE_URL
KAFKA_BROKERS
KAFKA_TASK_EVENTS_TOPIC
RELAY_INSTANCE_ID
RELAY_BATCH_SIZE
RELAY_POLL_INTERVAL
RELAY_CLAIM_TTL
RELAY_PUBLISH_TIMEOUT
RELAY_RETRY_BASE
RELAY_RETRY_MAX
RELAY_MAX_IN_FLIGHT
OUTBOX_RETENTION
```

## 26.4 Consumer

```text
DATABASE_URL
KAFKA_BROKERS
KAFKA_TASK_EVENTS_TOPIC
KAFKA_TASK_EVENTS_DLQ_TOPIC
KAFKA_CONSUMER_GROUP
CONSUMER_PROCESSING_TIMEOUT
CONSUMER_RETRY_BASE
CONSUMER_RETRY_MAX
POISON_MAX_ATTEMPTS
DLQ_PUBLISH_TIMEOUT
```

## 26.5 MySQL Lab

```text
MYSQL_LAB_DSN
MYSQL_LAB_MAX_OPEN_CONNS
MYSQL_LAB_MAX_IDLE_CONNS
MYSQL_LAB_CONN_MAX_LIFETIME
MYSQL_LAB_CONN_MAX_IDLE_TIME
MYSQL_LAB_LOCK_WAIT_TIMEOUT
MYSQL_LAB_TX_TIMEOUT
MYSQL_LAB_DEADLOCK_MAX_RETRIES
MYSQL_LAB_DEADLOCK_RETRY_BASE
MYSQL_LAB_DATASET_SIZE
MYSQL_LAB_RANDOM_SEED
```

MySQL Lab 配置不进入生产 Server、Worker、Relay 或 Consumer 的配置结构。

所有配置启动时校验，非法值立即失败，不静默使用危险默认值。

---

# 27. 测试矩阵

## 27.1 单元测试

- Task 状态机；
- 终态判断；
- Retry Backoff；
- Jitter；
- Hash；
- Token Hash；
- Scope；
- Job 聚合；
- Cursor 编解码；
- Gin 参数校验；
- Worker Semaphore；
- Registry；
- Mock Executor；
- HTTP URL 校验；
- Event Schema；
- Consumer 错误分类。

## 27.2 PostgreSQL 集成测试

- 多 Worker 并发领取；
- Task 不重复领取；
- Attempt No 递增；
- Lease 续租；
- Lease 过期；
- Stale Result；
- Reaper 与 Complete 竞态；
- Cancel 与 Complete 竞态；
- 创建幂等；
- 结果提交幂等；
- 同 Key 不同 Payload；
- Worker Instance 重启；
- Outbox 与业务状态同事务；
- Outbox Claim；
- 多 Relay Claim；
- Consumer Event ID 幂等插入。

## 27.3 Kafka 集成测试

- Outbox 正常发布；
- Kafka 不可用；
- 发布成功但未标记；
- 重复发布；
- Consumer 重复消息；
- Consumer DB 成功但 Offset 未提交；
- Consumer 重启；
- 同 Task Event 顺序；
- DLQ；
- Outbox 积压恢复。

## 27.4 MySQL 8 实验测试

- Migration；
- CRUD；
- 唯一约束；
- Cursor Pagination；
- Offset 对比；
- 联合索引；
- 覆盖索引；
- `EXPLAIN ANALYZE`；
- READ COMMITTED；
- REPEATABLE READ；
- 行锁；
- Gap Lock；
- Next-Key Lock；
- 死锁；
- 有限重试；
- `SKIP LOCKED`；
- 并发幂等；
- 连接池和 Context 超时。

## 27.5 并发检查

```bash
go test -race ./...
make test-mysql-concurrency
```

重点覆盖：

- Worker Registry；
- Semaphore；
- Heartbeat；
- Renew；
- Shutdown；
- Relay Worker Pool；
- Consumer Shutdown；
- Metrics 更新；
- MySQL Lab 并发测试辅助代码；
- 死锁重试状态；
- MySQL 并发领取结果聚合。

---

# 28. AI 使用规范

## 28.1 每次让 AI 开发前必须提供

- 当前 Phase；
- 当前仓库结构；
- 已有接口；
- 固定技术边界；
- 本次允许修改的文件；
- 本次禁止修改的文件；
- 需要新增的测试；
- 本次停止点；
- Git 检查点名称；
- 完成定义。

## 28.2 禁止 AI 做的事

- 为了让测试通过而删除测试；
- 用内存 Map 替代 PostgreSQL 核心语义；
- 用 SQLite 代替 PostgreSQL 并发测试；
- 用 Channel 代替 Kafka；
- 为规避竞态而给所有操作加一个全局 Mutex；
- 将 Fetch/Lease/Result 关键路径改为 GORM；
- 混用 GORM 和 pgx 事务；
- 无限制重试；
- 吞掉错误；
- 把 Task ID 作为 Prometheus Label；
- 悄悄增加 Redis、etcd、Raft；
- 用 MySQL 替换 PostgreSQL 主调度链路；
- 在生产进程中加入 PostgreSQL/MySQL 双写；
- 把核心 SQL 隐藏在难以审查的 ORM 魔法中；
- 在未解释的情况下改变执行语义。

## 28.3 AI 生成后必须完成的四层讲解

### 第一层：业务调用链

例如：

```text
Worker Fetch RPC
→ gRPC Handler
→ Scheduler Service
→ pgx Store
→ PostgreSQL Transaction
→ Attempt/Outbox
→ Response
```

### 第二层：数据和状态变化

要求列出：

- 输入；
- 查询条件；
- 更新前状态；
- 更新后状态；
- 写入哪些表；
- 哪些字段是幂等依据；
- 哪些字段是 Fencing 依据。

### 第三层：并发与故障

要求解释：

- 哪些操作可能同时发生；
- 谁持有什么锁；
- 哪个条件更新保证正确；
- 事务提交前崩溃怎样；
- 提交后响应前崩溃怎样；
- 重试是否安全。

### 第四层：逐文件讲解

要求 AI 按文件说明：

- 文件职责；
- 对外接口；
- 关键函数；
- 为什么放在该包；
- 哪些代码是样板；
- 哪些代码必须亲手复刻。

---

# 29. 可复用 AI 提示词

## 29.1 Phase 开发提示词模板

```text
你正在开发 Orbit Scheduler 的 Phase X。

固定项目边界：
- 生产主链路：Go + Gin + GORM + pgx + PostgreSQL + gRPC + Kafka + Prometheus + Testcontainers。
- MySQL 8 只允许出现在独立 Phase 8B Lab，不允许被生产进程依赖。
- 普通 CRUD 使用 GORM。
- Fetch/Renew/Report/Reaper/幂等/Outbox Claim 等关键事务使用 pgx 和手写 SQL。
- PostgreSQL 是 Task 权威状态，Kafka 不是任务队列。
- 执行语义为 At-least-once，不伪装 Exactly-once。
- 不加入 Redis、etcd、Kubernetes、Raft、DAG 或多 Scheduler 选主。
- 不创建 PostgreSQL/MySQL 双写，不修改生产数据库选型。

本次目标：
[填写目标]

允许修改：
[填写文件/目录]

禁止修改：
[填写文件/目录]

必须新增测试：
[填写测试]

本次只生成接口、骨架、测试和 TODO，在核心链路实现前停止。
不要填写 [核心函数名] 的最终实现。
完成后列出建议 commit，并确认可以打 Tag：[tag-name-start]。
```

## 29.2 AI 参考实现提示词模板

```text
基于当前已经提交并打 Tag 的骨架，实现 [核心链路] 的完整参考版本。

要求：
1. 不删除或弱化现有测试；
2. 使用 pgx 完整事务，不混用 GORM Transaction；
3. 所有状态变化使用条件更新；
4. 业务状态、Attempt、Outbox 在同一事务；
5. 明确处理事务提交前失败、提交后响应丢失和重复请求；
6. 增加并发集成测试；
7. 保持错误码稳定；
8. 运行 gofmt、go test、go test -race；
9. 完成后逐文件列出修改，并给出建议 commit；
10. 不进行与本核心链路无关的重构。
```

## 29.3 AI 讲解提示词模板

```text
不要继续修改代码。请讲解刚完成的 [核心链路]。

按以下顺序：
1. 从入口到数据库的完整调用链；
2. 每个函数的输入、输出和职责；
3. SQL 每个条件、锁和 RETURNING 的含义；
4. 事务边界以及写入的表；
5. 至少列出 5 个并发或故障场景；
6. 说明每个场景由哪个条件、约束或幂等字段保证；
7. 区分样板代码与必须掌握的核心代码；
8. 给我一份不看参考代码即可手写的实现步骤；
9. 给出 10 个面试追问和标准回答要点；
10. 最后给出我应该从哪个 Git Tag 拉学习分支。
```

## 29.4 手写复刻评审提示词模板

```text
这是我从 [start-tag] 拉出的学习分支，我已经亲手实现 [核心链路]。
请只做代码评审，不直接覆盖我的实现。

重点检查：
- 事务边界；
- 条件更新；
- 锁；
- Fencing；
- 幂等；
- 提交前/提交后故障；
- 错误分类；
- Goroutine 生命周期；
- 测试是否真正覆盖竞态；
- 与 [reference-tag] 相比的优缺点。

先列出问题和风险，再给出最小修复建议。除非我明确要求，否则不要直接重写整个文件。
```

---

# 30. 每个 Phase 的完成定义

一个 Phase 只有同时满足以下条件才算完成：

1. 功能实现完成；
2. 单元测试通过；
3. 必要集成测试通过；
4. Race Detector 通过；
5. 错误路径已验证；
6. 日志和指标存在；
7. README/Phase 文档更新；
8. AI 已完成四层讲解；
9. 核心链路存在 `start` 和 `reference` Tag；
10. Tag 已 Push；
11. 用户已完成至少一次手写复刻或明确记录暂缓；
12. 学习分支已 Push；
13. 如属于 Phase 8B，实验日志、查询计划和数据库对比文档已提交；
14. Phase 最终提交已合入 main；
15. main 可编译、可测试、可运行。

---

# 31. 最终完整验收标准

项目 v1.0.0 必须满足：

1. Gin API 完整；
2. GORM 普通 CRUD 边界清晰；
3. pgx 核心事务正确；
4. 多 Worker 不重复领取；
5. Lease 与 Attempt No 正确；
6. Stale Result 被拒绝；
7. 创建幂等；
8. 结果提交幂等；
9. Retry、Timeout、Cancel 正确；
10. Worker 有界并发；
11. Worker Graceful Shutdown；
12. PostgreSQL 集成测试完整；
13. Outbox 与业务状态同事务；
14. Kafka At-least-once 发布；
15. Consumer 幂等；
16. DLQ 正常；
17. Kafka 不可用不影响核心调度；
18. Prometheus 指标完整；
19. Testcontainers 覆盖 PostgreSQL、Kafka，并独立覆盖 MySQL 8 Lab；
20. Race Detector 通过；
21. 故障注入通过；
22. 性能报告可复现；
23. CI 全部通过；
24. README 和架构文档完整；
25. 核心 Git 检查点全部存在并已 Push；
26. MySQL 8 Lab 与生产进程完全隔离；
27. MySQL Migration、索引、分页和查询计划实验完整；
28. MySQL MVCC、隔离级别和锁实验完整；
29. MySQL 死锁有限重试正确；
30. MySQL `SKIP LOCKED` 并发领取正确；
31. PostgreSQL/MySQL 对比文档完整；
32. 用户能够不看代码讲清核心执行语义和数据库选型。

---

# 32. 推荐最终 Git 历史骨架

```text
p0-bootstrap
  └── p1-schema-domain
       └── p2-fetch-start
            └── p2-fetch-reference
                 └── p2-lease-result-start
                      └── p2-lease-result-reference
                           └── p2-reaper-start
                                └── p2-reaper-reference
                                     └── p2-scheduler-core
                                          └── p3-create-idempotency-start
                                               └── p3-create-idempotency-reference
                                                    └── p3-business-api
                                                         └── p4-worker-loop-start
                                                              └── p4-worker-loop-reference
                                                                   └── p4-worker-runtime
                                                                        └── p5-shutdown-start
                                                                             └── p5-shutdown-reference
                                                                                  └── p5-executor-shutdown
                                                                                       └── p6-outbox-relay-start
                                                                                            └── p6-outbox-relay-reference
                                                                                                 └── p6-consumer-start
                                                                                                      └── p6-consumer-reference
                                                                                                           └── p6-outbox-kafka
                                                                                                                └── p7-observability-verification
                                                                                                                     └── p8-fault-performance
                                                                                                                          └── p8b-mysql-foundation
                                                                                                                               └── p8b-mysql-deadlock-start
                                                                                                                                    └── p8b-mysql-deadlock-reference
                                                                                                                                         └── p8b-mysql-skip-locked-start
                                                                                                                                              └── p8b-mysql-skip-locked-reference
                                                                                                                                                   └── p8b-mysql-engineering-lab
                                                                                                                                                        └── v1.0.0
```

学习分支并行保留：

```text
learn/2-atomic-fetch
learn/2-lease-result
learn/2-reaper-races
learn/3-create-idempotency
learn/4-worker-loop
learn/5-graceful-shutdown
learn/6-outbox-relay
learn/6-audit-consumer
learn/8b-mysql-deadlock
learn/8b-mysql-skip-locked
learn/8b-mysql-index-query
```

---

# 33. 简历描述

**Orbit Scheduler｜Go 可靠分布式任务执行系统**

- 基于 Gin、GORM 与 PostgreSQL 实现 Project、Job、Task、Worker 等业务管理 API，支持 Token 鉴权、分页筛选、任务取消、批量创建和请求幂等。
- 使用 pgx、PostgreSQL 事务及 `FOR UPDATE SKIP LOCKED` 实现多 Worker 原子任务领取，通过 Lease、Worker Instance ID 和单调递增 Attempt No 拒绝失去租约的旧 Worker 提交结果。
- 基于 gRPC、Go Context、Semaphore 和 WaitGroup 实现 Worker 有界并发、周期续租、协作式取消和优雅退出，并通过条件更新处理 Complete、Cancel 与 Lease Reaper 竞态。
- 明确提供 At-least-once 执行语义，使用 Creation Request Hash、Result Hash、数据库唯一约束和下游 Idempotency Key 处理重复创建、重复执行和结果响应丢失。
- 使用 Transactional Outbox 将 Task 状态变化与事件记录写入同一 PostgreSQL 事务，由 Outbox Relay 以 At-least-once 语义发布 Kafka，Audit Consumer 通过 Event ID 唯一约束实现消费幂等与 DLQ。
- 接入 Prometheus、Testcontainers 和 Go Race Detector，通过 Worker 宕机、Lease 过期、旧结果提交、Kafka 不可用、重复投递和 Consumer 重启等故障注入验证恢复能力，并输出可复现性能报告。
- 在独立 MySQL 8 工程实验模块中实现 Migration、联合/覆盖索引、Cursor Pagination、`EXPLAIN ANALYZE`、隔离级别与锁实验、死锁有限重试及 `FOR UPDATE SKIP LOCKED` 并发领取，并形成 PostgreSQL/MySQL 选型对比报告；该模块不参与 Orbit 生产双写。

---

# 34. 项目最终证明的能力

完成本项目后，应能证明：

```text
Go
Gin
GORM
pgx
PostgreSQL
MySQL 8
InnoDB
SQL
联合索引
覆盖索引
EXPLAIN ANALYZE
Cursor Pagination
MVCC
隔离级别
行锁
Gap Lock
Next-Key Lock
死锁重试
SKIP LOCKED
gRPC
Protobuf
Context
Goroutine
Semaphore
状态机
Lease
Fencing
幂等
重试
故障恢复
Transactional Outbox
Kafka
Consumer Group
消费幂等
DLQ
Prometheus
Testcontainers
Race Detector
CI
性能测试
业务后端工程能力
数据库工程与选型能力
分布式任务执行能力
```

更重要的是，你应当能够通过 Git 学习分支证明：核心链路不是只让 AI 生成，而是自己从骨架独立实现、测试和解释过；同时能够明确说明 PostgreSQL 与 MySQL 各自承担什么角色，以及为什么没有为了堆关键词破坏系统架构。
