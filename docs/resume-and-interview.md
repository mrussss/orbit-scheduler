# 简历描述与面试讲解

这份文档用于准确介绍 Orbit Scheduler。所有表述都以仓库当前可运行、
可测试的实现为边界，不把规划功能写成已完成功能。

## 简历项目名称

**Orbit Scheduler — Go 分布式任务执行平台**

技术栈：Go、Gin、GORM、pgx、PostgreSQL、gRPC、Prometheus、
OpenAI-compatible API、Testcontainers、MySQL 8（独立实验模块）

下面的 LLM 描述适用于 `v0.2.0` 候选实现；只有 PostgreSQL、LLM Smoke、
Race、Build 和 MySQL 全部门禁通过并形成发布记录后，才应作为已发布能力使用。

## 推荐简历描述

篇幅有限时使用下面三条：

- 基于 Go、PostgreSQL 和 gRPC 实现 Pull 模式分布式任务执行平台，完成
  Project/Token/Job/Task API、Worker 注册心跳、任务领取、Lease 续租、
  结果上报、失败重试和优雅退出的完整链路。
- 使用 pgx 手写 `FOR UPDATE SKIP LOCKED` 调度事务，将 Task、Attempt、
  Lease、Transactional Outbox 及 Worker/Project 并发计数原子更新；通过
  `worker_instance_id + attempt_no` Fencing、结果 Hash 和条件更新处理旧
  Attempt、重复上报及 Reaper/Complete/Cancel 竞态。
- 使用真实 PostgreSQL/MySQL 8 Testcontainers、Race Detector 和黑盒 Smoke
  Test 验证并发与回滚语义；在隔离的 MySQL Lab 中复现隔离级别、死锁重试、
  唯一约束幂等、`SKIP LOCKED` 领取及 10 万行 Cursor/Offset 索引计划对比。
- 在既有 Executor Registry 中实现 OpenAI-compatible 非流式 LLM Executor，
  将模型调用纳入 Lease、Attempt、Retry、Cancel 和 Fencing；通过模型白名单、
  服务端密钥、请求/响应上限、Provider 并发以及 Fake Provider 隔离外部风险。

篇幅允许时可以补充：

- 实现有界 Worker Runtime、执行 Deadline、Lease 丢失取消和 Graceful
  Shutdown；HTTP Executor 对协议、Host、DNS 解析 IP、重定向、实际 Dial
  目标、敏感 Header 及请求/响应大小实施安全限制。
- 通过 GitHub Actions 分别运行 Go Race/Build、PostgreSQL 集成测试、完整
  HTTP → gRPC → Worker Smoke Test 和 MySQL 并发实验，并保存真实查询计划与
  验收记录。
- 对 429/5xx、4xx、网络错误、Timeout 和 Context Cancel 做固定 Outcome
  映射，记录低基数 Token、延迟和整数成本指标，并明确 At-least-once 模型调用
  仍可能导致重复请求和重复计费。

## 30 秒项目介绍

Orbit 是一个以 PostgreSQL 为权威状态的 Pull 模式任务执行系统。API 把任务
持久化后，多个 Worker 通过 gRPC 并发领取。领取使用 `SKIP LOCKED`，Lease
负责故障恢复，Attempt No 作为 Fencing Token 阻止过期 Worker 写回。系统不
承诺 Exactly-once，而是提供 At-least-once 执行和幂等结果提交。LLM 只是复用
同一可靠执行链路的新 Executor：密钥和路由由 Worker 配置，任务只能选择白名单
模型，重试由 Orbit Attempt 统一管理。仓库还有一个完全隔离的 MySQL 8 Lab，
用真实数据库测试索引、事务、死锁与并发领取，但 MySQL 不参与生产链路。

## 五分钟讲解顺序

1. 从 `internal/api/router.go` 说明租户 API、Scope 和 HTTP 边界。
2. 从 `internal/pgstore/fetch.go` 说明任务筛选、行锁、容量和原子副作用。
3. 从 `internal/pgstore/lease_result.go` 说明续租、Fencing 和幂等上报。
4. 从 `internal/pgstore/reaper_cancel.go` 说明 Lease 恢复与竞态条件更新。
5. 从 `internal/worker/runtime.go` 说明有界执行、续租、取消和上报。
6. 从 `internal/worker/lifecycle.go` 说明 Draining 和 Graceful Shutdown。
7. 从 `internal/executor/http.go` 说明 SSRF 防护为什么必须覆盖重定向和 Dial。
8. 从 `internal/executor/llm` 说明 Payload、Provider、错误映射、成本和指标边界。
9. 运行 `make smoke-llm`，展示 429 后 Attempt 递增，以及 SIGTERM 取消在途模型
   请求后由新 Worker 恢复。
10. 最后运行 `make verify`，用完整门禁证明模块已经连成真实链路。

## 必须能回答的问题

### 为什么 PostgreSQL 是任务权威状态，而不是内存队列？

任务状态、当前 Attempt、Lease、重试次数和终态都需要在崩溃后恢复，并且要
与 Attempt、Outbox 和容量计数原子提交。PostgreSQL 的事务和条件更新直接
承担正确性边界，Worker 和进程内存都可以随时丢失。

### `SKIP LOCKED` 是否保证 Exactly-once？

不保证。它避免多个事务同时领取同一行，但领取提交后响应可能丢失，或者
Worker 可能在外部副作用完成后崩溃。Orbit 因此明确提供 At-least-once；
Lease 负责恢复，Fencing 只保证旧 Attempt 不能覆盖新状态。

### 为什么需要 Attempt No？

Worker Instance ID 表示进程身份，Attempt No 表示同一 Task 的领取代次。任务
重新领取后 Attempt No 单调增加，旧 Worker 即使恢复也无法通过 Renew 或
Report 的条件校验。

### Result 响应丢失怎么办？

Worker 可以使用相同 Attempt、Outcome 和 Result Hash 重试。数据库已经提交时，
服务端把完全相同的上报视为幂等成功；不同结果返回冲突，过期 Attempt 返回
Stale Lease。

### 为什么 GORM 和 pgx 同时存在？

GORM 处理普通租户 CRUD 和查询，提高开发效率；pgx 与手写 SQL 处理调度状态机、
锁、条件更新和多表事务，让并发语义明确。一个事务内不会混用两套连接。

### 为什么 MySQL 使用独立 Module？

MySQL Lab 用于提供 InnoDB 索引、隔离、锁和死锁的真实工程证据，不是生产选型。
独立 Module 保证根目录生产二进制不会意外引入 MySQL Driver，也避免为了展示
技能而制造双写或万能数据库抽象。

### 为什么把 LLM 做成 Executor，而不是独立调度系统？

模型调用同样需要持久化、并发限制、超时、取消、重试、Lease 恢复和结果查询。
复用 Executor Registry 可以继承这些语义，Provider 只负责一次协议调用，不直接
接触 Scheduler Store，避免出现两套状态机和两套重试策略。

### 为什么 429/5xx 可以重试，而 400/401/403/404 不重试？

429、5xx 和传输错误通常是暂时故障，因此映射为 `RETRYABLE_FAILURE`，由下一次
Orbit Attempt 按统一 Backoff 重试。400 表示同一 Payload 无效，401/403 表示
凭据或权限配置错误，404 表示模型配置错误；原样重试只会重复失败和消耗资源。

### 为什么 Executor 不在内部循环重试？

Worker 已经有 Attempt 级重试。如果 Provider 内部再做业务重试，两层次数会相乘，
还会隐藏每次外部调用的状态和成本。当前 Provider 每个 Attempt 只发一次请求，
429 等结果交回 Scheduler 决定下一次可领取时间。

### Fencing 能避免模型重复调用和重复计费吗？

不能。请求写出后连接可能断开，此时 Provider 可能已经生成并计费。Orbit 会把
传输失败视为可重试，后续 Attempt 可能再次调用模型。Fencing 只保证旧 Attempt
不能覆盖权威结果，不保证外部副作用或 Provider 账单 Exactly-once。

### LLM API Key 如何隔离？

API Key 只从 Worker 环境读取，由 Provider 注入 Authorization。Payload 使用严格
解码，不能包含 API Key、Base URL 或 Header；自动重定向被禁用，Provider 错误
不复制响应体，日志和指标也不记录密钥、Prompt 或 Response。

### 为什么第一版不做 Streaming、RAG、MCP 和 Tool Calling？

第一版目标是把一次不可靠的外部模型调用纳入可靠调度语义。Streaming 会引入
部分结果恢复，RAG/MCP/Agent 会扩大数据和权限边界，Tool Calling 需要独立工具
白名单、Schema、轮数、超时和审计。它们不能在基础执行链路未稳定时混入。

### Token 和成本指标如何避免高基数？

Label 只使用配置中的 Provider、白名单 Model、固定 Outcome 和固定 Token Type。
Task ID、Project ID、Prompt、Response、Trace ID 和 Provider Request ID 都不进入
Label。成本按配置费率用整数 microunit 计算；没有费率时只记录 Usage，不伪造成本。

## 不应写进简历的内容

- “Orbit 同时支持 PostgreSQL 和 MySQL 生产运行”。
- “实现了 Exactly-once 任务执行”。
- “Kafka Relay、Audit Consumer 和 DLQ 已完成”。
- “实现了 Tool Calling、RAG、MCP 或多 Agent 工作流”。
- “Fencing 保证模型调用或模型计费 Exactly-once”。
- “支持公网安全的 gRPC 接入”。
- “达到某个 QPS、可用性或生产规模”，除非以后有对应环境、原始数据和报告。
- “完整实现了 Kubernetes、高可用部署、全链路追踪或告警体系”。

## 投递前个人检查

- 不看答案，画出 Fetch 事务和 Worker 生命周期。
- 亲手解释至少一个 PostgreSQL 集成测试的并发控制方式。
- 亲手修改一个 Mock Task 场景并运行 `make demo`。
- 能说明 MySQL 覆盖索引为什么让 Cursor 查询只扫描目标行。
- 能说明死锁重试为什么必须创建新事务并限制重试次数。
- 能手写 LLM Payload 白名单校验和 Error → Outcome 映射。
- 能解释 Worker SIGTERM 如何通过 Context 取消在途 Provider 请求。
- 能解释为什么旧 Attempt 结果被拒绝仍不能避免重复模型计费。
- 能运行 `make test-llm-executor` 和 `make smoke-llm` 并说明每项证据。
- 能明确说出项目尚未实现的边界，而不是回避它们。
