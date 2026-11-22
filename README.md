# 网关工坊 · API接口采集与数据网关平台

面向第三方API接入注册、接口鉴权编排、限流熔断、响应解析、schema映射、结果缓存、重放恢复与交付订阅的API接口采集与数据网关平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/base.py     共享服务骨架
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 接口提供方 | `api_provider` | API提供方 | 提供方编号、提供方名称、认证方式 |
| 鉴权配置 | `auth_profile` | 鉴权方案 | 方案编号、提供方、鉴权类型 |
| 接口定义 | `endpoint_def` | 接口端点 | 端点编号、提供方、请求路径 |
| 限流熔断 | `rate_limit` | 限流策略 | 策略编号、目标端点、QPS上限 |
| 请求批次 | `request_batch` | 调用批次 | 批次编号、关联端点、参数模板 |
| 响应解析 | `response_parse` | 解析规则 | 规则编号、目标端点、JSON路径 |
| Schema差异 | `schema_diff` | 差异记录 | 记录编号、关联端点、变更字段 |
| 结果缓存 | `data_cache` | 缓存策略 | 策略编号、目标端点、缓存TTL |
| 重放队列 | `retry_queue` | 重放任务 | 任务编号、失败端点、失败原因 |
| 数据投递 | `data_delivery` | 投递管道 | 管道编号、数据来源、目标地址 |
| 订阅管理 | `subscriber` | 订阅方 | 订阅编号、订阅方、数据主题 |
| 接口监控 | `api_monitor` | 监控指标 | 指标编号、目标端点、监控类型 |
| Webhook路由 | `webhook_router` | 回调规则 | 规则编号、触发事件、目标URL |
| 传输加密 | `data_encrypt` | 加密配置 | 配置编号、数据范围、加密算法 |
| 调用日志 | `call_log` | 调用记录 | 记录编号、接口端点、请求参数 |
| 数据版本 | `data_version` | 数据快照 | 版本编号、数据来源、行数统计 |
| 管道审计 | `pipeline_audit` | 审计记录 | 记录编号、管道名称、操作人 |
| 治理报告 | `govern_report` | 治理报告 | 报告编号、评估范围、合规指标 |
| 增量追踪 | `incr_tracker` | 增量标记 | 标记编号、目标端点、增量字段 |
| 抽样校验 | `sample_check` | 抽样计划 | 计划编号、目标端点、抽样比例 |
| 用量计费 | `cost_account` | 用量账单 | 账单编号、提供方、计费周期 |
| 任务调度 | `task_schedule` | 调度计划 | 计划编号、关联端点、Cron表达式 |

## 调用耗时标准化（迁移口径）

调用日志同时存在两类历史耗时：人工登记的 `响应耗时`（可能带单位，如 `0.2 s`，
也可能只有数值 `32`）和采集动作自动写入的 `响应耗时(ms)`。监控/统计统一读取
`规范耗时(ms)`，旧字段原样保留，绝不按零延迟或默认单位入账。

标准字段：

| 字段 | 含义 |
| --- | --- |
| `规范耗时(ms)` | 换算后的毫秒数；单位不可确定时为空 |
| `耗时单位` / `耗时换算依据` | 原始单位与换算关系，如 `1 s = 1000 ms` |
| `耗时来源字段` | 实际采用的旧字段 |
| `耗时迁移状态` | 已换算 / 未换算 / 已回滚 |
| `耗时回填批次` / `耗时修订` | 写入批次与重算次数，便于追溯 |
| `耗时未换算原因` | 缺单位、负值、无法解析等可读原因 |

操作接口：

```bash
# 影子核对（不写标准字段）
curl -X POST 127.0.0.1:8000/api/call-duration/backfill -H 'Content-Type: application/json' -d '{"dry_run":true}'
# 按批回填，中断后再次调用会续跑同一批次
curl -X POST 127.0.0.1:8000/api/call-duration/backfill -H 'Content-Type: application/json' -d '{"limit":2}'
# 校对缺单位旧值（例如把 CALL-0003 的 32 按毫秒重新入账）
curl -X POST 127.0.0.1:8000/api/call-duration/recompute -H 'Content-Type: application/json' -d '{"record_no":"CALL-0003","unit":"ms"}'
# 回滚标准字段，旧字段保持可读
curl -X POST 127.0.0.1:8000/api/call-duration/rollback -H 'Content-Type: application/json' -d '{}'
# 部署后报告：实际采用耗时、换算依据、样本分母、未转换记录、旧字段可读性
curl 127.0.0.1:8000/api/call-duration/report
# 单笔耗时溯源
curl 127.0.0.1:8000/api/call-duration/records/CALL-0001
```

要点：

- 可确定单位的旧值换算后毫秒数不变（`0.2 s → 200`，自动写入的 `120 → 120`）；
- 缺单位旧值（如 `32`）标记 `未换算`，计入未转换清单，监控分母剔除；
- 回填中断时监控对混合记录实时回退旧字段计算，标准值缺失不影响统计；
- 回填支持分批续跑、影子核对、完成校对、回滚后重算，重复提交同一 `记录编号`
  的调用只保留一条；
- 接口监控启用时按规范耗时计算区间延迟（均值/P50/P95），并把结果回写到监控
  指标行；治理与计费读取的指标同样使用该口径。

回归测试（标准库 unittest，无需额外依赖）：

```bash
cd backend
.venv/bin/python -m unittest discover -s tests -v
```

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

领域：数据工程
子方向：数据采集、爬虫与RPA
