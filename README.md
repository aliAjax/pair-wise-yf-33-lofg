# 多站卫星地面站排程系统

使用标准库与 SQLite 实现的独立排程原型。系统维护卫星、地面站、天线、维护时段、可见窗口、租户配额和数据请求，并检查速率、数据量、截止时间、设备重叠、卫星同时接收、天气和租户配额。

## 代码结构（规则、持久化、请求入口分开维护）

- `persistence.py`：SQLite 建表/迁移、事务、审计与 `receive_progress` 进度留档（只追加）。
- `rules.py`：排程、接收状态机、断点续传的全部业务规则（容量、维护、天气、冲突、截止、配额复核）。
- `app.py`：HTTP 请求入口与路由；为兼容历史调用，仍导出 `ApiError`、`SatelliteSchedulingService`、`iso` 等。
- `static/index.html`：进度、掉线说明与续传结果展示。

## 断点续传

接收状态机在 `scheduled → receiving → received` 之外增加可恢复掉线：

1. `POST /api/schedules/{id}/progress` `{received_mb}`：接收过程登记本段累计数据量（不可回退、不可超本段容量）。
2. 未到目标调用 `/complete` 会被拒绝（`incomplete_transfer`，返回缺口）。
3. `POST /api/schedules/{id}/offline` `{reason, ends_at?, received_mb?}`：掉线写明原因，截断 `actual_ends_at` 释放剩余时段（后续排程可占用），请求回到可恢复状态，进度写入只追加的 `receive_progress`。
4. `POST /api/requests/{id}/resume`：重新选窗口、天线、时段，按**剩余量**复核容量、速率、维护、天气、天线/卫星冲突、截止时刻与租户配额；通过后新建接收段接着累计，`resume_of_schedule_id` 记录来源，历史进度不被覆盖。掉线请求走普通 `/schedule` 会返回 `resume_required`。
5. 可见窗口缩短（`/visibility-windows/{id}/change`）影响**接收中**任务时，转为可恢复掉线（`offline_recoverable`）并释放剩余时段；普通待接收任务照旧抢占。

掉线段配额只按 `actual_ends_at` 之前的实际占用时长计。

## 运行

```bash
python3 app.py --db satellite_scheduling.db
```

默认监听 `127.0.0.1:8204`，首页 `/`，健康检查 `/health`。

身份头为 `X-User-Id`、`X-Role`；`requester` 还需 `X-Tenant`。角色：`viewer`、`requester`、`operator`、`commander`、`auditor`。

## 主要接口

- `POST /api/satellites`、`/api/stations`、`/api/antennas`、`/api/maintenance`、`/api/visibility-windows`、`/api/quotas`：资源配置。
- `POST /api/requests`：创建数据接收请求。
- `POST /api/requests/{id}/schedule`、`/reschedule`、`/resume`：排程、重排被抢占请求、掉线后续传。
- `POST /api/schedules/{id}/start`、`/complete`、`/cancel`、`/preempt`、`/progress`、`/offline`：接收状态、累计进度登记、掉线留档和紧急抢占。
- `POST /api/visibility-windows/{id}/change`：窗口变化并返回受影响排程；已接收数据保留。
- `GET /api/state`、`GET /api/schedules/{id}`：权限化状态查询。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 主要局限

速率和容量按静态 Mbps 与时长计算，不包含链路预算、调制编码、雨衰、天线跟踪和存储卸载策略。租户身份使用请求头模拟；SQLite 和单进程 HTTP 服务适用于原型，生产环境需要统一身份、共享数据库和分布式资源锁。
