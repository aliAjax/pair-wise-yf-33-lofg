# 多站卫星地面站排程系统

使用标准库与 SQLite 实现的独立排程原型。系统维护卫星、地面站、天线、维护时段、可见窗口、租户配额和数据请求，并检查速率、数据量、截止时间、设备重叠、卫星同时接收、天气和租户配额。长时接收支持断点续传：接收中登记累计数据量，掉线留档后可按剩余量重新排程续传。

## 运行

```bash
python3 app.py --db satellite_scheduling.db
```

默认监听 `127.0.0.1:8204`，首页 `/`，健康检查 `/health`。

身份头为 `X-User-Id`、`X-Role`；`requester` 还需 `X-Tenant`。角色：`viewer`、`requester`、`operator`、`commander`、`auditor`。

## 主要接口

- `POST /api/satellites`、`/api/stations`、`/api/antennas`、`/api/maintenance`、`/api/visibility-windows`、`/api/quotas`：资源配置。
- `POST /api/requests`：创建数据接收请求。
- `POST /api/requests/{id}/schedule`、`/reschedule`：排程或重排被抢占请求。
- `POST /api/requests/{id}/resume`：掉线请求续传，重新选择窗口、天线和时段，按剩余数据量复核容量、维护、天气、冲突、截止时刻和租户额度。
- `POST /api/schedules/{id}/start`、`/complete`、`/cancel`、`/preempt`：接收状态和紧急抢占；`complete` 要求累计接收量达到请求目标，否则拒绝。
- `POST /api/schedules/{id}/progress`：登记接收进度（本次会话累计 MB，单调不减）。
- `POST /api/schedules/{id}/drop`：接收掉线，原因必填；释放剩余时段，进度留档，请求转为 `interrupted`。
- `POST /api/visibility-windows/{id}/change`：窗口变化并返回受影响排程；已接收数据保留，接收中排程转为可续传的 `dropped`，普通待接收排程照旧转为 `preempted`。
- `GET /api/state`、`GET /api/schedules/{id}`、`GET /api/requests/{id}/progress`：权限化状态与进度留档查询。

## 断点续传

- 排程状态新增 `dropped`（掉线可续传），请求状态新增 `interrupted`（等待续传）。
- `dropped` 排程不再占用天线/卫星时段，也不计入租户当日配额，剩余时段即被释放；其 `received_mb` 与 `disposition_reason` 永久留档，不可取消或覆盖。
- 续传通过后请求回到 `scheduled`，新会话继续累计；请求累计量为各会话 `received_mb` 之和，达到 `data_mb` 才允许 `complete`。
- 进度、掉线、续传、完成均写入 `progress_log`（只追加），页面展示进度条、掉线说明和续传结果。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 主要局限

速率和容量按静态 Mbps 与时长计算，不包含链路预算、调制编码、雨衰、天线跟踪和存储卸载策略。租户身份使用请求头模拟；SQLite 和单进程 HTTP 服务适用于原型，生产环境需要统一身份、共享数据库和分布式资源锁。
