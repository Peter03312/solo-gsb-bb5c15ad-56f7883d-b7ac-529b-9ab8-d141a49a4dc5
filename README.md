# Reliable Webhook Delivery API

一个本地可运行的 Webhook 可靠投递服务：接收带 **目标端点** 和 **幂等键** 的事件
（单条 `POST /events` 或批量 `POST /events/batch`），
校验 HMAC-SHA256 签名后入队，由后台调度器按顺序、带退避重试地向目标端点投递；
三次重试仍失败则进入死信。支持 **投递时间窗**（`deliver_after` / `expires_at`）：
可预约首次投递时刻，也可为排队等待设置截止期限。每个目标端点还带有**自动熔断器**：
连续 3 次投递失败即熔断 10 秒，到期后仅放行队头事件作为唯一 half-open 探针。
此外可为任意目标设置**维护开关**（`PUT /targets/pause`）：暂停后不再领取该目标的
新事件、其他目标照常投递，恢复时不重置任何退避/熔断/尝试状态。还可为任意目标设置
**投递限速**（`PUT /targets/pace`）：为该目标的两次领取设置最小间隔（0～3600 秒，
0 为关闭），首次投递与重试都占用间隔，只阻挡该目标队头，其他目标照常投递。
技术栈：**Python · FastAPI · SQLite**。

## 特性

- **HMAC-SHA256 签名校验**：签名针对**原始请求体字节**计算（`X-Webhook-Signature: sha256=<hex>`），密钥来自环境变量 `WEBHOOK_SECRET`，使用恒定时间比较。
- **幂等接收**：`idempotency_key` 在数据库中有唯一约束。重复提交返回**原事件**（HTTP 200，响应头 `Idempotent-Replay: true`），**不会再次入队或投递**；即使重复请求携带了不同甚至无效的时间窗字段，也一律返回原事件。
- **批量接收**：`POST /events/batch` 接受 **1～100 个**事件组成的 JSON 数组，签名同样针对**整个原始请求体**校验。批内幂等键重复 → `422`；已存在的幂等键返回原事件并标记 `created=false`（该项其他字段一律忽略）；新事件沿用单事件的目标 URL 与时间窗校验，并在**同一个 SQLite 事务**中按数组顺序入队——任一新事件无效或写入失败，本批新事件**全部不入库**。响应按输入顺序逐项给出事件与 `created` 标记；重发同一批不会重复投递，后续调度仍遵循队头顺序、1/2/4 退避与熔断规则。
- **投递时间窗**：创建事件可携带 RFC 3339 的 `deliver_after`（默认创建时刻，提前到该时刻之前不投递）与 `expires_at`（必须晚于 `deliver_after`，否则 422）。调度器每次领取前在同一事务中把已到 `expires_at` 且仍为 `queued` 的事件标记为 `expired`：不产生任何投递尝试，并不再阻塞同目标的后续事件；已进入 `processing` 的事件不因到期而中断。
- **同一目标端点严格有序**：以创建顺序（SQLite `rowid`）为准，前一条事件未成功、进入死信或过期前，后一条不会被取出投递——即使前一条只是在等待自己的 `deliver_after`。不同端点之间并行投递，互不阻塞。
- **重试与死信**：首次立即投递（或按 `deliver_after` 延迟）；失败后分别等待 **1s / 2s / 4s** 重试（共 4 次尝试）；第 4 次仍失败（非 2xx、超时、连接错误）即进入死信（状态 `dead`）。
- **按目标端点的自动熔断器**：同一目标**连续 3 次**投递失败（非 2xx、超时、连接错误）后进入 **open**，10 秒内该目标的任何事件都不会被领取；熔断到期**且队头事件已到原定投递时间**（退避/`deliver_after`）时，只允许该队头事件作为唯一的 **half-open 探针**领取，其余事件继续排队。探针返回 2xx → 熔断 **closed**、连续失败计数清零；探针失败 → 重新 open 10 秒（计数继续累计）。熔断**只影响出问题的目标**，其他端点照常并行投递。状态（`closed`/`open`/`half-open`）、连续失败数、截止时间与探针事件 id 全部持久化在 SQLite，**重启后继续生效**，并与队头顺序、1/2/4 退避、过期清扫和死信重放完全兼容。
- **熔断器查询**：`GET /targets/breakers` 返回每个有过投递尝试的目标的当前状态、`consecutive_failures`、open 截止时间（`open_until`，RFC 3339 UTC）与 half-open 探针事件 id（`probe_event_id`），支持按 `state` / `target` 过滤。
- **目标维护开关**：`PUT /targets/pause`（请求体 `{"target": <URL>, "paused": <bool>}`）按与入站事件**相同的目标 URL 规范化**精确匹配目标，重复设置幂等；`GET /targets/pauses` 返回所有已设置目标及其暂停状态。暂停后调度器**不再领取**该目标的新事件，**已领取（`processing`）的在途请求正常完成**，**其他目标照常投递**；暂停期间排队事件**仍按 `expires_at` 正常清扫过期**。恢复只解除暂停：队头顺序、预约时间、1/2/4 退避与熔断（open 截止时间、失败计数、half-open 探针）一律保持，**不重置尝试次数或熔断状态**。开关写入与调度领取在同一把数据库锁的串行事务中生效，状态保存在 SQLite `target_pauses` 表，**重启后保留**，老库启动自动建表。
- **按目标限速**：`PUT /targets/pace`（请求体 `{"target": <URL>, "interval_seconds": <0~3600>}`）按与入站事件**相同的目标 URL 规范化**精确匹配目标，重复设置幂等；`GET /targets/paces` 返回所有已配置目标及其间隔与上次领取时刻。每次领取（**首次投递或重试**）都占用一次间隔：仅当 `上次领取时刻 + 间隔 <= 当前时刻` 时该目标的队头事件才可被领取（从未领取过则立即放行）；限速只阻挡该目标队头，**其他目标照常并行投递**。**调整间隔**后按已保存的上次领取时刻**重算**下一可领取时刻，设为 `0` **立即解除**限速。预约（`deliver_after`）、1/2/4 退避、熔断与暂停规则继续叠加生效，**过期清扫不受限速影响**。配置写入与调度领取在同一把数据库锁的串行事务中生效，配置与上次领取时刻保存在 SQLite `target_paces` 表，**重启后保留**，老库启动自动建表。
- **死信重放**：`POST /events/{event_id}/replay` 仅接受 `dead` 事件，在一个事务内将其重新入队并开启**新一轮完整**的 1s/2s/4s 退避（再次享有 4 次尝试）。历史 `attempts` 全部保留、`attempt_no` 连续递增，`replay_count` 累计并记录 `last_replayed_at`，幂等键与事件 id 不变；并发重复重放只会生效一次，不会重复投递。
- **事件查询**：返回每次尝试的时间、HTTP 响应码/错误信息、最终状态（`queued` / `processing` / `delivered` / `dead` / `expired`），时间窗字段 `deliver_after` / `expires_at`，以及 `replay_count` / `last_replayed_at` 重放信息。
- **崩溃恢复**：投递状态全部在 SQLite（WAL 模式）中，重启后把卡在 `processing` 的事件重新排队；出站请求携带 `X-Event-Id` / `X-Idempotency-Key`，投递语义为 at-least-once。
- 内置一个实验用目标端点 `/dev/sink`，方便本地演示成功/失败/死信。

## 一条命令启动（Docker Compose）

```bash
docker compose up --build
```

启动后服务监听：

| 项目 | 地址 |
| --- | --- |
| API 根地址 | <http://localhost:8000> |
| 健康检查 | <http://localhost:8000/health> |
| 交互文档 (Swagger UI) | <http://localhost:8000/docs> |
| OpenAPI Schema | <http://localhost:8000/openapi.json> |

宿主机端口 `8000`，容器内端口 `8000`（在 `docker-compose.yml` 的 `ports` 中可改）。
数据（事件、投递尝试、死信与重放记录、熔断器状态、目标暂停开关、目标限速配置与上次
领取时刻）持久化在命名卷 `webhook-data`（容器内 `/data/webhooks.db`）；老版本数据库
启动时会自动补齐重放、时间窗相关列以及熔断器表、目标暂停表、目标限速表，无需手工迁移。

生产环境请覆盖默认密钥：

```bash
echo "WEBHOOK_SECRET=$(openssl rand -hex 32)" > .env
docker compose up --build
```

> 不想用 Compose 时：`docker build -t reliable-webhook . && docker run -p 8000:8000 -e WEBHOOK_SECRET=dev-secret-change-me -v webhook-data:/data reliable-webhook`

## 不用 Docker，本地直接运行

需要 Python 3.11+：

```bash
pip install -r requirements.txt
export WEBHOOK_SECRET=dev-secret-change-me
export WEBHOOK_DB_PATH=./data/webhooks.db
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## API

### 1. 创建事件 `POST /events`

请求头：

```
Content-Type: application/json
X-Webhook-Signature: sha256=<HMAC_SHA256(secret, 原始请求体) 的十六进制摘要>
```

请求体（JSON）：

| 字段 | 说明 |
| --- | --- |
| `idempotency_key` | 必填，调用方生成的唯一幂等键 |
| `target` | 必填，目标投递端点的完整 URL（http/https） |
| `payload` | 可选，任意 JSON，原样转发给目标端点 |
| `deliver_after` | 可选，RFC 3339 时间戳（须带时区偏移），该时刻之前不投递；缺省为创建时刻 |
| `expires_at` | 可选，RFC 3339 时间戳（须带时区偏移），必须**晚于** `deliver_after`；到期仍为 `queued` 的事件转为 `expired`，不再投递 |

- 签名缺失/错误 → `401`
- `expires_at` 早于或等于 `deliver_after`、时间格式非法（非 RFC 3339 / 缺时区）→ `422`
- 首次创建 → `201`
- 幂等键重复 → `200`，返回数据库中原事件，带响应头 `Idempotent-Replay: true`，**不会重新投递**；新请求里的时间窗字段被忽略（即使无效也不会 422）

### 2. 批量创建事件 `POST /events/batch`

请求头与签名规则和单事件接口完全相同（`X-Webhook-Signature` 针对**整个原始请求体**计算）。
请求体为 **1～100 个**事件对象组成的 JSON 数组，每个元素的字段同 `POST /events`。

逐项语义（按数组顺序）：

- 幂等键**已存在** → 返回数据库中的原事件并标记 `created=false`；该项重新提交的其他字段
  （`target` / `payload` / 时间窗，无论是否有效）**一律忽略**，不会再次入队或投递。
- 幂等键**为新** → 沿用单事件接口的目标 URL 与时间窗校验；全部新事件在**同一个 SQLite
  事务**中按数组顺序入队（创建顺序即同目标下的投递排队顺序）。

整体规则：

| 情况 | 结果 |
| --- | --- |
| 签名缺失/错误 | `401` |
| 请求体不是 JSON / 不是数组 | `400` |
| 数组为空或超过 100 个元素 | `422` |
| 某个元素不是对象、缺少/非法 `idempotency_key`（响应体 `detail` 含从 0 开始的元素下标） | `422` |
| 批内幂等键重复 | `422`，整批不入库 |
| 任一新事件字段非法（目标 URL、时间窗等）或写入失败 | `422` / `5xx`，**本批所有新事件都不入库** |
| 全部为新事件 | `201` |
| 含有已存在的幂等键 | `200`，带响应头 `Idempotent-Replay: true` |

响应按输入顺序逐项返回事件与 `created` 标记：

```json
{
  "events": [
    {"created": true,  "event": {"id": "9f3c1ab2…", "idempotency_key": "evt-1001", "status": "queued"}},
    {"created": false, "event": {"id": "308e5ff7…", "idempotency_key": "evt-0001", "status": "delivered"}}
  ]
}
```

重发同一批请求：所有项都命中已有幂等键，全部返回 `created=false`，**不会重复投递**；
入队后的批量事件与普通事件完全等同，仍按既有队头顺序、1/2/4 退避与熔断规则调度。

### 3. 查询事件

- `GET /events/{event_id}` — 查询单个事件，含全部投递尝试
- `GET /events/key/{idempotency_key}` — 按幂等键查询
- `GET /events?status=dead&limit=50&offset=0` — 列表，可按状态过滤（`queued` / `processing` / `delivered` / `dead` / `expired`）
- `GET /targets/breakers?state=open&target=<url>&limit=200&offset=0` — 查询各目标端点的熔断器（见第 5 节）
- `GET /targets/pauses?target=<url>&paused=true` — 查询各目标端点的维护开关（见第 6 节）
- `GET /targets/paces?target=<url>&limit=200&offset=0` — 查询各目标端点的限速配置（见第 7 节）

响应示例（4 次尝试后进入死信）：

```json
{
  "id": "308e5ff7…",
  "idempotency_key": "evt-0001",
  "target": "http://localhost:8000/dev/sink?fail_times=4",
  "payload": {"hello": "world"},
  "status": "dead",
  "attempts_count": 4,
  "created_at": "2026-09-17T12:27:13.160640+00:00",
  "deliver_after": "2026-09-17T12:27:13.160640+00:00",
  "expires_at": null,
  "delivered_at": null,
  "replay_count": 0,
  "last_replayed_at": null,
  "attempts": [
    {"attempt_no": 1, "status_code": 500, "error": null, "attempted_at": "2026-09-17T12:27:13.333930+00:00"},
    {"attempt_no": 2, "status_code": 500, "error": null, "attempted_at": "2026-09-17T12:27:14.498397+00:00"},
    {"attempt_no": 3, "status_code": 500, "error": null, "attempted_at": "2026-09-17T12:27:16.701202+00:00"},
    {"attempt_no": 4, "status_code": 500, "error": null, "attempted_at": "2026-09-17T12:27:20.934826+00:00"}
  ]
}
```

### 4. 重放死信事件 `POST /events/{event_id}/replay`

仅允许 `status=dead` 的事件。服务在同一个事务里把事件重新置为 `queued`
（`next_attempt_at=now`，首次尝试立即发生），并开启**新一轮完整**的 1s/2s/4s
退避——即新一轮再次享有 4 次尝试，而不是立即再次进入死信。

- 历史 `attempts` 全部保留，`attempt_no` 全局连续递增（如 4 次失败后重放，新尝试从 5 开始编号）
- `replay_count` 加一，`last_replayed_at` 记录本次重放时间
- 幂等键、事件 id、创建顺序（同端点有序性）均不变，出站签名与幂等头行为不变

响应与错误：

| 情况 | 状态码 |
| --- | --- |
| 重放成功，返回重放后的事件（`status=queued`） | `200` |
| 事件不存在 | `404` |
| 事件为 `queued` / `processing` / `delivered` / `expired`（响应体说明当前状态） | `409` |

并发重复重放由条件更新（`UPDATE … WHERE status='dead'`）保证只有一个请求生效，
其余返回 `409`，**不会重复入队或重复投递**。

### 5. 查询熔断器 `GET /targets/breakers`

返回所有有过投递尝试的目标端点的熔断器状态，支持查询参数：

| 参数 | 说明 |
| --- | --- |
| `state` | 可选，仅返回该状态：`open` / `half-open` / `closed`（其他值 `422`） |
| `target` | 可选，按**完整目标 URL** 精确匹配 |
| `limit` / `offset` | 可选，分页（`limit` 1–500，默认 200） |

响应示例：

```json
{
  "breakers": [
    {
      "target": "http://localhost:8000/t/fail-always",
      "state": "open",
      "consecutive_failures": 3,
      "open_until": "2026-09-17T12:27:31.934826+00:00",
      "probe_event_id": null
    },
    {
      "target": "http://localhost:8000/dev/sink",
      "state": "half-open",
      "consecutive_failures": 3,
      "open_until": null,
      "probe_event_id": "308e5ff7…"
    },
    {
      "target": "http://localhost:8000/t/record",
      "state": "closed",
      "consecutive_failures": 0,
      "open_until": null,
      "probe_event_id": null
    }
  ],
  "limit": 200,
  "offset": 0
}
```

字段语义：

- `state`：`closed`（正常，失败计数 < 3）/ `open`（熔断中，截止前不可领取该目标的事件）/ `half-open`（已放行唯一探针事件，正在投递）。
- `consecutive_failures`：当前连续失败次数；任意一次 2xx 立即清零。
- `open_until`：open 截止时间（RFC 3339 UTC）；closed / half-open 时为 `null`。
- `probe_event_id`：half-open 时正在投递的唯一探针事件 id；其他状态为 `null`。

熔断规则：

1. 某目标**连续** `BREAKER_FAILURE_THRESHOLD`（默认 3）次投递失败 → open，截止时间为当前时刻 + `BREAKER_OPEN_SECONDS`（默认 10 秒）。
2. open 期间调度器不会领取该目标的**任何**事件（其他目标不受影响）；熔断不会中断已在 `processing` 中的投递。
3. 截止时间过后，仅当**队头**事件本身也已到原定投递时间（1/2/4 退避或 `deliver_after`，以两者较晚者为准）时，该事件才被领取为唯一 half-open 探针；同一目标的其他事件继续严格排队，探针在途时不会被并发领取。
4. 探针 2xx → closed，计数清零，队列恢复正常；探针失败（非 2xx/超时/连接错误）→ 重新 open 10 秒，随后仍由当时的队头事件作为下一枚探针。
5. 熔断状态、失败计数与截止时间均在 SQLite `breakers` 表中：重启不会清零计数或提前结束 open 窗口；重启时若发现探针事件因崩溃卡在 `processing`，事件重新排队、熔断器以**已到期的 open** 恢复，下一个 tick 即重新探针，不会永久卡死该目标。

熔断与既有机制的关系：熔断器叠加在事件状态机之上，不改变单次事件的 1/2/4 退避与死信规则——若探针恰好是该轮第 4 次失败，事件照常进入死信（随后可重放），而熔断器同步重新 open；`queued` 事件在 open 期间到达 `expires_at` 仍会正常转为 `expired` 并让位；重放死信事件时若熔断仍 open，事件会排队等到可探针为止。

### 6. 目标维护开关

为单个目标端点暂停/恢复投递，用于目标侧维护。开关状态持久化在 SQLite `target_pauses`
表，重启后保留；老数据库启动时 `CREATE TABLE IF NOT EXISTS` 自动建表，无需手工迁移。

#### `PUT /targets/pause`

请求体：

```json
{"target": "http://example.com/hooks", "paused": true}
```

| 字段 | 说明 |
| --- | --- |
| `target` | 必填，目标 URL（http/https）；按与 `POST /events` **相同的规则规范化**（scheme/host 小写、默认端口省略、路径归一），与事件库里保存的目标串精确匹配 |
| `paused` | 必填，布尔：`true` 暂停投递，`false` 恢复 |

响应 `200`：

```json
{
  "target": "http://example.com/hooks",
  "paused": true,
  "updated_at": "2026-09-27T12:27:13.160640+00:00"
}
```

- 重复设置相同值是**幂等**的：行内容（含 `updated_at`）不变；从暂停改为恢复（或反向）才刷新 `updated_at`。
- 字段缺失/类型不对、目标不是合法 URL → `422`。该接口不需要签名（运维管理面）。

暂停/恢复与调度器领取的串行语义：开关写入与 `claim_due_events` 领取都在同一把数据库锁
（`db_lock`）保护的单条 SQLite 事务中，因此二者严格串行：

1. **暂停提交后**，后续领取事务不再取出该目标的任何 `queued` 事件（其他目标不受影响、照常并行投递）；
2. **暂停前已领取**（状态 `processing`）的事件**不会被中断**，在途 HTTP 请求正常完成并照常进入 `delivered` / 重试 / 死信流程；
3. 暂停期间排队事件**继续按 `expires_at` 清扫**：到期仍为 `queued` 的事件转为 `expired`（零尝试）并解除对同目标后续事件的阻塞；
4. **恢复仅解除暂停**：之后的领取仍遵守原有的队头顺序、`deliver_after` 预约时间、1/2/4 退避和熔断条件（open 截止时间继续倒计时、half-open 探针规则不变），**尝试次数、退避进度与熔断状态均不重置**。

#### `GET /targets/pauses`

返回所有**显式设置过**开关的目标（从未设置的目标不出现在列表中，行为等同未暂停）：

| 参数 | 说明 |
| --- | --- |
| `target` | 可选，按规范化后的完整目标 URL 精确匹配 |
| `paused` | 可选，`true` / `false`，仅返回暂停中/已恢复的行 |
| `limit` / `offset` | 可选，分页（`limit` 1–500，默认 200） |

```json
{
  "pauses": [
    {
      "target": "http://example.com/hooks",
      "paused": true,
      "updated_at": "2026-09-27T12:27:13.160640+00:00"
    }
  ],
  "limit": 200,
  "offset": 0
}
```

### 7. 目标限速

为单个目标端点设置两次**领取**之间的最小间隔（投递限速），用于保护下游接口的
调用频率。配置与上次领取时刻持久化在 SQLite `target_paces` 表，重启后保留；老数据库
启动时 `CREATE TABLE IF NOT EXISTS` 自动建表，无需手工迁移。

#### `PUT /targets/pace`

请求体：

```json
{"target": "http://example.com/hooks", "interval_seconds": 5}
```

| 字段 | 说明 |
| --- | --- |
| `target` | 必填，目标 URL（http/https）；按与 `POST /events` **相同的规则规范化**（scheme/host 小写、默认端口省略、路径归一），与事件库里保存的目标串精确匹配 |
| `interval_seconds` | 必填，**0～3600**（秒）：该目标两次领取之间的最小间隔；`0` 表示关闭限速 |

响应 `200`：

```json
{
  "target": "http://example.com/hooks",
  "interval_seconds": 5.0,
  "last_claimed_at": "2026-09-27T12:27:13.160640+00:00",
  "updated_at": "2026-09-27T12:30:00.000000+00:00"
}
```

- `last_claimed_at`：该目标最近一次领取时刻（RFC 3339 UTC）；配置后尚未领取过为 `null`。
- 重复设置相同值是**幂等**的：行内容（含 `updated_at`）不变；调整间隔才刷新 `updated_at`。
- 字段缺失/类型不对、`interval_seconds` 超出 0～3600、目标不是合法 URL → `422`。该接口不需要签名（运维管理面）。

限速与调度器领取的串行语义：配置写入与 `claim_due_events` 领取都在同一把数据库锁
（`db_lock`）保护的单条 SQLite 事务中，因此二者严格串行——已提交的间隔对下一个领取
事务立即生效，先提交的领取不会被回滚。具体规则：

1. 每次领取时按该目标**上次领取时刻**与**当前间隔**判断是否放行：仅当
   `上次领取时刻 + 间隔 <= 当前时刻`（或从未领取过）时，该目标的队头事件才可被领取；
2. **首次投递与重试都占用间隔**：每一次领取（无论第几次尝试）都在同一事务里把
   `last_claimed_at` 刷新为当前时刻，因此重试间隔取 `max(退避, 限速)` 的实际效果；
3. **调整间隔**只改 `interval_seconds`，不动上次领取时刻——下一可领取时刻按
   `上次领取时刻 + 新间隔` **重算**（例如从 30 秒改为 1 秒，若上次领取已在 1 秒前则
   立即可领取）；**设为 `0` 立即解除限速**（行保留，上次领取时刻继续跟踪，重新开启时
   仍按真实的上次领取时刻计算）；
4. 限速**只阻挡该目标的队头事件**：其他目标照常并行投递；预约（`deliver_after`）、
   1/2/4 退避、熔断（open/half-open 门控）与暂停规则继续叠加生效——全部条件都满足
   才放行；已在 `processing` 的在途请求不受限速影响；
5. **过期清扫不受限速影响**：排队事件到期仍转为 `expired`（零尝试）并让位给后续事件。

#### `GET /targets/paces`

返回所有**显式配置过**限速的目标（从未配置的目标不出现在列表中，也不会被限速；
`interval_seconds = 0` 的行表示已配置但当前关闭）：

| 参数 | 说明 |
| --- | --- |
| `target` | 可选，按规范化后的完整目标 URL 精确匹配 |
| `limit` / `offset` | 可选，分页（`limit` 1–500，默认 200） |

```json
{
  "paces": [
    {
      "target": "http://example.com/hooks",
      "interval_seconds": 5.0,
      "last_claimed_at": "2026-09-27T12:27:13.160640+00:00",
      "updated_at": "2026-09-27T12:30:00.000000+00:00"
    }
  ],
  "limit": 200,
  "offset": 0
}
```

### 出站投递格式

服务向 `target` 发送 `POST`（`Content-Type: application/json`），请求体：

```json
{"id":"…","idempotency_key":"…","target":"…","payload":{…},"created_at":"…","attempt":1}
```

请求头：`X-Webhook-Signature`（用同一密钥对出站请求体签名）、`X-Event-Id`、
`X-Idempotency-Key`、`X-Delivery-Attempt`。目标端点返回任意 **2xx** 即视为成功。

## 快速试用（curl）

启动服务后另开一个终端：

```bash
BASE=http://localhost:8000
KEY="evt-$(date +%s)"
BODY="{\"idempotency_key\":\"$KEY\",\"target\":\"$BASE/dev/sink\",\"payload\":{\"msg\":\"hi\"}}"

# 用原始请求体计算签名
SIG="sha256=$(printf '%s' "$BODY" | openssl dgst -sha256 -hmac "${WEBHOOK_SECRET:-dev-secret-change-me}" | sed 's/^.* //')"

# 提交事件（/dev/sink 默认成功）
curl -s -X POST "$BASE/events" -H "X-Webhook-Signature: $SIG" -d "$BODY"

# 批量提交：一个请求入队 2 个事件（1～100 个），签名同样针对整个原始请求体
BATCH="[{\"idempotency_key\":\"${KEY}-b1\",\"target\":\"$BASE/dev/sink\",\"payload\":{\"n\":1}},{\"idempotency_key\":\"${KEY}-b2\",\"target\":\"$BASE/dev/sink\",\"payload\":{\"n\":2}}]"
SIGB="sha256=$(printf '%s' "$BATCH" | openssl dgst -sha256 -hmac "${WEBHOOK_SECRET:-dev-secret-change-me}" | sed 's/^.* //')"
curl -s -X POST "$BASE/events/batch" -H "X-Webhook-Signature: $SIGB" -d "$BATCH"
# 响应按输入顺序逐项返回 {"created": ..., "event": {...}}；
# 再次发送同一批（同一个 BATCH 请求体）会全部返回 created=false，不会重复投递

# 查询
curl -s "$BASE/events/key/$KEY"

# 演示失败重试 + 死信：让目标前 4 次都返回 500
BODY2="{\"idempotency_key\":\"${KEY}-fail\",\"target\":\"$BASE/dev/sink?fail_times=4\",\"payload\":{}}"
SIG2="sha256=$(printf '%s' "$BODY2" | openssl dgst -sha256 -hmac "${WEBHOOK_SECRET:-dev-secret-change-me}" | sed 's/^.* //')"
curl -s -X POST "$BASE/events" -H "X-Webhook-Signature: $SIG2" -d "$BODY2"
# 约 9 秒（1+2+4 退避）后 GET /events/key/... 可看到 4 条尝试与 status=dead

# 重放死信事件：重新入队并开启新一轮完整退避（仅 status=dead 允许）
DEAD_ID=$(curl -s "$BASE/events/key/${KEY}-fail" | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')
curl -s -X POST "$BASE/events/$DEAD_ID/replay"

# 观察按目标熔断器：上面连续失败的目标会经历 open（10s）→ half-open（单探针）→ closed
curl -s "$BASE/targets/breakers"                 # 全部目标
curl -s "$BASE/targets/breakers?state=open"     # 只看熔断中的目标

# 目标维护开关：暂停某目标（无需签名），该目标不再被领取，其他目标照常投递
curl -s -X PUT "$BASE/targets/pause" -H 'Content-Type: application/json' \
  -d "{\"target\":\"$BASE/dev/sink\",\"paused\":true}"
curl -s "$BASE/targets/pauses"                  # 查看所有已设置开关
# 恢复：只解除暂停，退避/尝试次数/熔断状态全部保持原样
curl -s -X PUT "$BASE/targets/pause" -H 'Content-Type: application/json' \
  -d "{\"target\":\"$BASE/dev/sink\",\"paused\":false}"

# 目标限速：该目标两次领取至少间隔 5 秒（无需签名），只阻挡该目标队头
curl -s -X PUT "$BASE/targets/pace" -H 'Content-Type: application/json' \
  -d "{\"target\":\"$BASE/dev/sink\",\"interval_seconds\":5}"
curl -s "$BASE/targets/paces"                   # 查看所有限速配置（含上次领取时刻）
# 调小间隔按上次领取时刻重算下一可领取时刻；设为 0 立即解除限速
curl -s -X PUT "$BASE/targets/pace" -H 'Content-Type: application/json' \
  -d "{\"target\":\"$BASE/dev/sink\",\"interval_seconds\":0}"
```

Python 生成签名：

```python
import hashlib, hmac
signature = "sha256=" + hmac.new(
    WEBHOOK_SECRET.encode(), raw_body_bytes, hashlib.sha256
).hexdigest()
```

## 投递语义与配置

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `WEBHOOK_SECRET` | `dev-secret-change-me` | HMAC 签名密钥（入站校验 + 出站签名） |
| `WEBHOOK_DB_PATH` | `/data/webhooks.db` | SQLite 数据库路径 |
| `WORKER_POLL_INTERVAL` | `0.2` | 调度器扫描间隔（秒） |
| `WORKER_MAX_CONCURRENCY` | `8` | 同时进行投递的不同端点数 |
| `DELIVERY_TIMEOUT` | `10` | 出站 HTTP 超时（秒） |
| `BREAKER_FAILURE_THRESHOLD` | `3` | 同一目标连续失败多少次后熔断 |
| `BREAKER_OPEN_SECONDS` | `10` | 熔断打开时长（秒），失败探针重新 open 同样时长 |

重试计划固定为 **1s、2s、4s**（首次失败后），共 4 次尝试；如需调整可修改
`app/config.py` 中的 `RETRY_BACKOFF` / `MAX_ATTEMPTS`。

投递时间窗的实现要点：`deliver_after` 会折叠进 `next_attempt_at`（首次尝试不早于它）；
`expires_at` 另存一份 epoch 秒（`expires_at_ts`）供调度器比较。调度器每次领取前在**同一个
事务**里先把 `expires_at_ts <= now` 且仍为 `queued` 的事件置为 `expired`（`next_attempt_at`
清空，永不再产生尝试），再按常规规则领取到期事件——因此过期事件会立即解除对同目标后续
事件的阻塞。`processing` 中的事件不受到期影响，在途尝试会正常完成并进入
`delivered` / 重试 / 死信流程。

死信重放的实现要点：`events` 表上的 `round_base_attempts` 记录本轮退避的起点；
重放把它提升到当前 `attempts_count`，新一轮尝试的退避位置按轮内序号重新从 1s
开始，而 `attempt_no` 保持全局连续。启动时会自动为老数据库补齐 `replay_count` /
`last_replayed_at` / `round_base_attempts` / `deliver_after` / `expires_at` /
`expires_at_ts` 六列（`ALTER TABLE`，幂等），并把老行的 `deliver_after` 回填为
`created_at`（立即到期、从不过期，行为与旧版一致），无需手工迁移。

批量接收的实现要点：`POST /events/batch` 先对每项提取幂等键做一次 `IN` 批量查重，
已存在的键直接返回原事件（其余字段不校验、不生效）；新事件由
`db.create_events_batch` 在**同一个事务**里先全部完成时间窗校验、再按数组顺序依次
`INSERT`（同批共享一个创建时间戳，rowid 顺序即数组顺序，因此同目标下仍按数组顺序
投递）。任一校验失败或写入异常都会整体 `ROLLBACK`——本批新事件要么全部入队、要么
一个都不入库。入队后的批量事件与普通事件完全等同，由调度器按既有队头顺序、退避与
熔断规则投递；重发同一批时所有键均已存在，只返回查重结果，不产生任何写入或投递。

有序性实现要点：调度器在一个事务中只挑选每个目标端点上「创建最早且退避已到期」
的事件并原子地置为 `processing`；更早的事件仍在 `queued`/`processing` 时，
后续事件不会被选中，直到前者 `delivered`、`dead` 或 `expired`。

熔断器实现要点：`breakers` 表（`target` 主键）保存 `state` /
`consecutive_failures` / `open_until_ts`（epoch 秒）/ `probe_event_id`，
`CREATE TABLE IF NOT EXISTS` 使老库启动即自动获得该表，无需手工迁移。领取事件的
同一事务会 `LEFT JOIN breakers` 进行门控：无记录或 `closed` 放行；`open` 仅当
`open_until_ts <= now` 时放行队头，并把该行原子翻转为 `half-open`、记录探针事件 id；
`half-open`（探针在途）一律不放行。每次投递结果在 `record_attempt` 的**同一事务**
里更新熔断器：2xx 直接 upsert 为 `closed` 并清零；失败则计数加一，达到阈值或探针
失败时写入新的 `open_until_ts`，探针失败无论计数多少都会立即重新 open。启动恢复时
`reset_stuck_events` 保留普通 open 行的截止时间，只把卡在 half-open 的行改为
**已到期**的 open（探针事件同时重新排队），使该目标下一个 tick 即可重新探针。

维护开关实现要点：`target_pauses` 表（`target` 主键，`paused` 0/1，`updated_at`）
由 `init_db` 以 `CREATE TABLE IF NOT EXISTS` 创建，老库启动即自动获得，状态持久化
且重启保留。`PUT /targets/pause` 复用与入站事件同一个 `HttpUrl` 规范化（`str(HttpUrl)`），
保证按事件库里保存的目标串精确匹配；upsert 时若行已存在且标志相同则原样返回，
`updated_at` 不变（重复设置幂等）。领取事件的同一事务额外 `LEFT JOIN target_pauses`
门控：`paused = 1` 的目标不参与领取，但领取事务**开头的过期清扫不看该标志**，
所以暂停期间排队事件照常到期转 `expired`。开关写入与领取都在 `db_lock` 保护的单条
事务内，二者严格串行：已提交的暂停对下一个领取事务立即生效；先提交的领取不会被回滚，
其事件留在 `processing` 由 worker 正常完成。恢复只是 upsert 回 `paused = 0`，
不触碰 `events` / `breakers` 的任何字段，因此队头顺序、`next_attempt_at`（退避与
预约时间）、尝试计数与熔断状态全部保持，恢复后按原状态机继续。

限速实现要点：`target_paces` 表（`target` 主键，`interval_seconds` REAL，
`last_claimed_at_ts` epoch 秒，`updated_at`）由 `init_db` 以
`CREATE TABLE IF NOT EXISTS` 创建，老库启动即自动获得，配置与上次领取时刻持久化、
重启保留。`PUT /targets/pace` 复用与入站事件同一个 `HttpUrl` 规范化；upsert 时间隔
相同则原样返回（幂等），调整间隔只改 `interval_seconds` 与 `updated_at`，**不动**
`last_claimed_at_ts`——因此下一可领取时刻按「上次领取时刻 + 新间隔」重算，设 `0`
立即放行。领取事件的同一事务额外 `LEFT JOIN target_paces` 门控：无记录、
`interval_seconds <= 0`、从未领取过、或 `last_claimed_at_ts + interval_seconds <= now`
时队头才可被领取；每次领取（首次投递或重试）都在**同一事务**里把
`last_claimed_at_ts` 刷新为当前时刻。门控只作用于该目标队头，其他目标不受影响；
领取事务开头的过期清扫不看该表，排队事件照常到期转 `expired`。

## 运行测试

```bash
pip install -r requirements-dev.txt
python -m pytest tests/ -v
```

测试启动真实 uvicorn 端口，端到端覆盖：签名校验（缺失/错误/篡改）、成功投递与查询、
幂等不重复入队、1/2/4 秒退避后死信、失败后恢复、同端点严格有序（含前序失败阻塞后序直到死信）、
死信重放（完整新退避轮次、历史保留与连续编号、replay_count 累计、非死信 409/未知 404、并发重放仅生效一次）、
投递时间窗（`deliver_after` 延迟投递且未到时的前序事件仍阻塞同目标后续事件、无效时间窗 422、
幂等键重复返回原事件、queued 到期转 `expired` 零尝试并让位、`processing` 不受到期影响），
批量接收（签名校验、非数组/空数组/超 100 个 400/422、元素形状校验、批内幂等键重复 422、
按数组顺序入队与同目标有序投递、已存在键返回原事件 `created=false` 且忽略其他字段、
任一新事件非法时整批回滚、重发同批不重复投递、批量事件排在既有队头之后），
以及按目标熔断器（连续 3 次失败 open、open 期间全目标冻结、到期仅队头作为 half-open
探针、探针成功 closed 清零/失败重新 open、其他目标并行不受影响、查询接口字段与过滤、
重启后 open 截止时间保持且崩溃中断的探针自动恢复、默认阈值 3 次/10 秒），
以及目标维护开关（URL 规范化精确匹配与重复设置幂等、暂停后不再领取而其他目标照常投递、
在途请求正常完成、暂停/恢复不重置尝试计数与 1/2/4 退避、熔断 open 截止/计数/探针保持、
暂停期间排队事件照常过期清扫、恢复后按原队头顺序投递、开关写入与领取在数据库事务中
串行生效、老库自动建表且暂停状态跨重启保留），
以及按目标限速（URL 规范化与重复设置幂等、0～3600 边界校验、首次投递与重试都占用间隔、
只阻挡该目标队头而其他目标照常投递、调整间隔按上次领取时刻重算、设 0 立即解除、
限速期间排队事件照常过期清扫、配置写入与领取在数据库事务中串行生效、
老库自动建表且配置与上次领取时刻跨重启保留）。

## 项目结构

```
.
├── app/
│   ├── main.py        # FastAPI 路由：签名校验、单事件/批量接收、查询、死信重放、熔断器查询、目标维护开关与限速、dev sink
│   ├── config.py      # 环境变量配置（含熔断阈值/open 时长）
│   ├── db.py          # SQLite 表结构、状态机、有序 claim、熔断器状态机、目标暂停开关与限速、批量事务写入、幂等查询
│   ├── delivery.py    # 出站 HTTP 投递与 HMAC 签名
│   └── worker.py      # 后台调度器（每端点有序、跨端点线程池并行）
├── tests/             # 端到端测试
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── README.md
```
