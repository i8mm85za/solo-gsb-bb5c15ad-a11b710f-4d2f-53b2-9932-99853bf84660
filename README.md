# 断点分片上传协调 API（Chunked Upload Coordinator）

只协调**元数据**的上传调度服务：创建会话、分片租约、完成上报、提交汇总与状态查询。
**不接收、不保存任何文件内容**，文件字节流由客户端自行点对点传输或写入对象存储。

技术栈：Python 3.12 · FastAPI · SQLite（WAL）

## 快速启动（单条命令）

```bash
docker compose up --build
```

启动后访问：

| 地址 | 说明 |
|---|---|
| `http://localhost:8000` | 服务信息 |
| `http://localhost:8000/docs` | Swagger 交互式 API 文档 |
| `http://localhost:8000/openapi.json` | OpenAPI 描述 |
| `http://localhost:8000/healthz` | 健康检查 |

依赖安装已纳入镜像构建，SQLite 建库建表在应用启动时自动完成，无需额外初始化步骤。
数据通过命名卷 `coordinator-data` 持久化，删除卷即清空全部状态。

## 配置（环境变量）

均可在 `docker-compose.yml` 的 `environment` 中调整：

| 变量 | 默认值 | 说明 |
|---|---|---|
| `UPLOAD_DB_PATH` | `/data/coordinator.db` | SQLite 数据库文件路径 |
| `DEFAULT_LEASE_TTL_SECONDS` | `300` | 租约默认有效期（秒） |
| `MAX_LEASE_TTL_SECONDS` | `3600` | 租约有效期上限（秒），超出会被截断 |
| `DEFAULT_BASE_RETRY_SECONDS` | `5` | 创建会话未显式指定时的失败退避基数（秒） |
| `DEFAULT_MAX_RETRY_SECONDS` | `300` | 创建会话未显式指定时的失败退避冷却上限（秒） |

端口映射默认为宿主机 `8000` → 容器 `8000`，可在 `docker-compose.yml` 的 `ports` 中修改。

## 核心语义

- **会话**：创建时登记 `total_chunks`（总分片数）与 `file_digest`（期望的完整文件摘要），分片序号从 `0` 开始。可选 `digest_algorithm`（默认 `sha256`，仅作记录；取 `sha256-merkle-v1` 时启用提交时的 Merkle 完整性校验）。可选失败退避参数 `base_retry_seconds`（默认 `5`，必须为正）与 `max_retry_seconds`（默认 `300`，必须为正且不小于 base）。
- **依赖（可选）**：创建会话时可提交 `dependencies`（键与值均为分片序号），声明某分片领取租约前必须完成的前置分片。越界、自依赖、有环的依赖图会被拒绝（`422`）；不提供时保持无依赖的原有行为。
- **租约**：同一分片同一时刻仅允许一个有效租约；租约过期后可被重新领取（令牌轮换）。前置分片未全部完成时领取返回 `409` 及按序缺失的 `missing_dependencies`；前置全部完成后无需额外操作即可领取。
- **失败上报与退避重试（可选自动生效）**：`POST /sessions/{id}/chunks/{index}/fail` 必须携带该分片**当前有效租约的令牌**；校验通过后在同一事务内**原子清除租约、`failure_count` 加 1**，并将分片置于退避冷却中，第 n 次失败冷却 `min(max_retry_seconds, base_retry_seconds × 2^(n-1))` 秒（默认序列 5、10、20、40、80、160、300、300…）。冷却中的分片**单片与批量领取均跳过/拒绝**，`/ready` 单独列出 `cooling_chunks`；冷却结束后无需干预即可重新领取。令牌不匹配、租约已过期、分片已完成或会话已提交均返回 `409`。
- **完成上报**：必须携带该分片**当前有效租约的令牌**与分片摘要；以相同令牌 + 相同摘要重复上报为幂等成功（返回 `idempotent_replay: true`），摘要不同则返回 `409`。完成后**清除退避冷却但保留 `failure_count`**。
- **完成纠错回退（reopen）**：`POST /sessions/{id}/chunks/{index}/reopen` 凭目标分片**完成时保留的租约令牌**与 `expected_digest`（完成时上报的摘要）发起，仅允许**未提交会话中已完成且凭据匹配**的分片。校验通过后服务端沿依赖图找出**全部传递下游**，在**同一事务**内将目标及下游重置为 `pending`：清除摘要、完成时间、有效租约与退避截止时间，**保留累计失败次数**；同时会话 `revision` 递增 1。响应中的 `affected_chunks` 按**（拓扑层级, 分片序号）**升序稳定排列（目标层级为 0，层级取受影响子图中的最长路径）。任一校验失败（会话已提交 / 分片未完成 / 凭据不匹配）返回 `409` 且**不产生任何部分回退**；整个回退与并发领取互斥，领取方不会看到中间状态。
- **进度分支（fork）**：`POST /sessions/{id}/fork` 从一个 **`active`** 会话分叉出全新的上传会话，请求体包含 `expected_revision`（调用方认为源会话所处的修订号，不匹配返回 `409` 及 `current_revision`）、幂等键 `request_id` 与 `reuse_chunks`（要复用进度的分片序号列表，可为空）。仅当 `reuse_chunks` 中**每一项都已在源会话完成**（越界序号按未完成处理，响应以 `not_completed_chunks` 返回），且集合**包含这些分片的全部传递前置分片**（缺失项以 `missing_prerequisites` 升序返回）时才会创建；任一校验失败返回 `409` 且**不落库**。新会话复制源会话的**全部配置**（总分片数、文件摘要、摘要算法、并发上限、退避参数）与**完整依赖图**：复用分片的**摘要与完成时间**复制为 `completed`，其余分片初始化为 `pending`；**不复制**租约（令牌/过期时间）、退避冷却与累计失败次数。新会话 `revision` 从 0 开始、状态为 `active`，与源会话相互独立。**同一源会话 + 同一 `request_id` 的重放**（含仅顺序/重复不同的 `reuse_chunks`）返回**首次创建的新会话**（`200`、`idempotent_replay: true`）；相同键但 `expected_revision` 或 `reuse_chunks` 变化返回 `409`。已提交（`finalized`）的会话不可再分支。
- **会话级并发上限（可选）**：创建会话时可设置 `max_active_leases`，限制该会话**当前有效（未过期）租约**的总数。单片领取在槽位占满时返回 `409`；批量领取则原子地领取至多 `min(count, 剩余槽位, 可领取分片数)` 个租约。不设置（`null`）时保持原有不限并发的行为。
- **批量领取**：`POST /sessions/{id}/leases/batch` 在**同一临界区**内完成资格计算与写入：仅从“未完成、无有效租约（过期租约视为空闲）、**不在退避冷却中**、前置已全部完成”的分片中，按分片序号升序领取，返回各分片独立令牌；并发请求之间不会重复分配、也不会突破并发上限。无可领取项时返回空列表及 `active` / `blocked` / `next_expiry` 汇总，其中 **`next_expiry` 取有效租约最早过期时间与各分片退避最早结束时间中的更早者**。
- **跨会话公平调度**：`POST /dispatch/lease` 携带 `queue_key`、候选 `session_ids` 与可选 `lease_ttl_seconds`。候选会话**去重后按（创建时间, id）稳定排序**（未知会话按 id 排在最后），服务端按 `queue_key` 持久化**最近一次成功会话**作为游标，在**同一写锁事务**内从游标之后**循环扫描**，向首个可用会话发放其**最低序号可领取分片**——资格判断复用批量领取的全部规则（依赖、退避、有效租约、`max_active_leases`），并发请求不会重复发放。**无可发放项时不推进游标**，返回各会话的不可用原因（`not_found` / `finalized` / `completed` / `max_active_leases_reached` / `leases_active` / `in_backoff` / `blocked_by_dependencies`）与最早的 `next_retry_at`；**成功后才更新游标**，连续请求因此在可用会话间轮转。
- **提交**：仅当全部分片完成才允许提交，返回按分片序号升序汇总的清单；否则返回 `409` 及 `missing_chunks` 缺失分片列表。重复提交幂等返回同一清单。会话使用 `sha256-merkle-v1` 时，提交前先按分片序号构建 Merkle 树并与 `file_digest` 常量时间比较，不匹配返回 `409` 及 `computed_digest`，会话保持 `active`（见下文「完整性校验」）。
- 已完成的分片不可再领取租约；已提交的会话进入只读状态。

## 完整性校验（sha256-merkle-v1，可选）

创建会话时将 `digest_algorithm` 设为 `sha256-merkle-v1` 即启用端到端完整性校验；
其余取值（含默认的 `sha256`）仅作记录，行为与之前完全一致。

启用后的约束与规则：

- **摘要格式**：创建会话的 `file_digest` 与每次 complete 的 `chunk_digest` 必须是
  **64 位小写十六进制**（SHA-256 的 32 字节十六进制编码），否则返回 `422`。
  `chunk_digest` 应取该分片原始内容的 SHA-256。
- **提交时校验**：finalize 按分片序号升序读取各 `chunk_digest`，构建 Merkle 树：
  - 叶节点：`leaf = SHA-256(0x00 || chunk_digest 的原始 32 字节)`
  - 父节点：`parent = SHA-256(0x01 || 左节点 32 字节 || 右节点 32 字节)`
  - 每层节点数为奇数时，**末节点复制自身**后配对
  - 逐层归约直至单一根（单分片会话的根即唯一叶节点）
- **比较与结果**：根的小写十六进制与 `file_digest` 做**常量时间比较**。
  不匹配返回 `409` 及 `computed_digest`（服务端算出的根），**不做任何写入、会话保持
  `active`**，可通过 reopen 纠错后重试；匹配才沿用现有的原子提交与幂等响应。

参考计算（Python）：

```python
import hashlib

def merkle_root(chunk_digests_hex):
    level = [hashlib.sha256(b"\x00" + bytes.fromhex(d)).digest()
             for d in chunk_digests_hex]
    while len(level) > 1:
        if len(level) % 2 == 1:
            level.append(level[-1])            # 奇数末节点复制自身
        level = [hashlib.sha256(b"\x01" + level[i] + level[i + 1]).digest()
                 for i in range(0, len(level), 2)]
    return level[0].hex()
```

调用示例（curl）：

```bash
# 1. 客户端先算出每个分片的 SHA-256（64 位小写十六进制），再算出 Merkle 根
ROOT=$(python3 -c '...')   # 上述 merkle_root([d0, d1, d2, ...])

# 2. 创建启用完整性校验的会话，file_digest 登记 Merkle 根
SID=$(curl -s -X POST http://localhost:8000/sessions \
  -H 'Content-Type: application/json' \
  -d "{\"total_chunks\": 3, \"file_digest\": \"$ROOT\",
       \"digest_algorithm\": \"sha256-merkle-v1\"}" \
  | python3 -c 'import sys, json; print(json.load(sys.stdin)["id"])')

# 3. 逐分片领取租约并上报完成；chunk_digest 必须是 64 位小写十六进制
curl -s -X POST http://localhost:8000/sessions/$SID/chunks/0/complete \
  -H 'Content-Type: application/json' \
  -d "{\"lease_token\": \"$TOKEN\", \"chunk_digest\": \"$D0\"}"

# 4. 提交：根与 file_digest 匹配则正常提交；不匹配返回 409 与 computed_digest，
#    会话保持 active，可 reopen 出错分片、重新上报后再次提交
curl -s -X POST http://localhost:8000/sessions/$SID/finalize
# => 409 {"detail": "file digest mismatch", "computed_digest": "<算出的根>"}
```

状态查询与创建/提交响应均暴露 `digest_algorithm` 字段。旧 SQLite 数据无需重写：
既有会话的 `digest_algorithm` 不是 `sha256-merkle-v1`，行为完全不变。

## API 一览

| 方法 | 路径 | 说明 |
|---|---|---|
| `POST` | `/sessions` | 创建上传会话（可选 `dependencies` 依赖图、`max_active_leases` 并发上限、`base_retry_seconds`/`max_retry_seconds` 退避参数） |
| `GET` | `/sessions/{session_id}` | 查询会话与逐分片状态（含各分片 `dependencies`、`failure_count`、`retry_after`、退避参数、当前 `active_leases`、会话 `revision` 与 `source_session_id`） |
| `POST` | `/sessions/{session_id}/fork` | 进度分支：从 active 会话创建新会话，复用已完成分片进度（校验修订号/完成状态/传递前置闭包，`request_id` 幂等） |
| `GET` | `/sessions/{session_id}/ready` | 当前可领取的分片、被依赖阻塞的分片，以及退避冷却中的分片（`cooling_chunks`） |
| `POST` | `/sessions/{session_id}/chunks/{index}/lease` | 领取分片租约（同样受 `max_active_leases` 与退避冷却约束） |
| `POST` | `/sessions/{session_id}/leases/batch` | 原子批量领取租约（按分片序号，数量不超过 `count` 与剩余并发槽位；自动跳过冷却分片） |
| `POST` | `/dispatch/lease` | 跨会话公平调度：按 `queue_key` 轮转游标，从首个可用会话领取最低序号分片；无可发放项时返回各会话原因与最早 `next_retry_at` |
| `POST` | `/sessions/{session_id}/chunks/{index}/complete` | 上报分片完成（幂等；清除退避、保留失败次数） |
| `POST` | `/sessions/{session_id}/chunks/{index}/reopen` | 完成纠错回退（凭完成时的令牌与摘要，原子重置目标及全部传递下游，`revision` 递增 1） |
| `POST` | `/sessions/{session_id}/chunks/{index}/fail` | 上报分片失败（校验有效租约，原子清除租约并进入指数退避） |
| `POST` | `/sessions/{session_id}/finalize` | 提交会话，返回按序分片清单（`sha256-merkle-v1` 会话先校验 Merkle 根，不匹配返回 409 及 `computed_digest`） |
| `GET` | `/healthz` | 健康检查 |

## 批量领取与并发上限（curl）

```bash
# 1. 创建会话：8 个分片，同一时刻最多 3 个有效租约
SID=$(curl -s -X POST http://localhost:8000/sessions \
  -H 'Content-Type: application/json' \
  -d '{"total_chunks": 8, "file_digest": "<文件SHA256>", "max_active_leases": 3}' \
  | python3 -c 'import sys, json; print(json.load(sys.stdin)["id"])')

# 2. 一次申请 5 个、TTL 120 秒：实际只发 3 个（受剩余并发槽位限制），按分片序号升序返回
curl -s -X POST http://localhost:8000/sessions/$SID/leases/batch \
  -H 'Content-Type: application/json' -d '{"count": 5, "lease_ttl_seconds": 120}'
# => {
#      "requested": 5,
#      "granted": [
#        {"chunk_index": 0, "lease_token": "...", "expires_at": "..."},
#        {"chunk_index": 1, "lease_token": "...", "expires_at": "..."},
#        {"chunk_index": 2, "lease_token": "...", "expires_at": "..."}
#      ],
#      "active": 3, "blocked": 0, "next_expiry": "2026-...Z",
#      "max_active_leases": 3, "remaining_slots": 0
#    }

# 3. 槽位占满时再申请：返回空列表与汇总，可按 next_expiry 等待重试
curl -s -X POST http://localhost:8000/sessions/$SID/leases/batch \
  -H 'Content-Type: application/json' -d '{"count": 5}'
# => {"requested": 5, "granted": [], "active": 3, "blocked": 0,
#     "next_expiry": "2026-...Z", "max_active_leases": 3, "remaining_slots": 0}

# 4. 用令牌上报完成后，槽位与后续分片同时释放，再批量领取即可拿到新的分片
#    （单片接口 POST .../chunks/{index}/lease 同样占用槽位，占满时返回 409）

# 5. 状态查询暴露并发上限与当前有效租约数（过期租约不计入）
curl -s http://localhost:8000/sessions/$SID
# => {"max_active_leases": 3, "active_leases": 3, ...}
```

依赖图与批量领取可以组合使用：被前置依赖阻塞的分片不会进入 `granted`，
其数量汇总在响应的 `blocked` 中；前置分片陆续完成后会自动具备领取资格。
不设置 `max_active_leases` 的会话中，`max_active_leases` 与 `remaining_slots` 均为 `null`，
批量领取只受 `count` 与可领取分片数约束。

## 调用示例（curl）

```bash
# 1. 创建会话：4 个分片，登记期望的文件 SHA-256
SID=$(curl -s -X POST http://localhost:8000/sessions \
  -H 'Content-Type: application/json' \
  -d '{"total_chunks": 4, "file_digest": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", "digest_algorithm": "sha256"}' \
  | python3 -c 'import sys, json; print(json.load(sys.stdin)["id"])')

# 2. 领取分片 0 的租约（有效期 120 秒）
LEASE=$(curl -s -X POST http://localhost:8000/sessions/$SID/chunks/0/lease \
  -H 'Content-Type: application/json' -d '{"lease_ttl_seconds": 120}')
TOKEN=$(echo "$LEASE" | python3 -c 'import sys, json; print(json.load(sys.stdin)["lease_token"])')

# 3. 客户端自行传输该分片后，上报完成（携带租约令牌与分片摘要）
curl -s -X POST http://localhost:8000/sessions/$SID/chunks/0/complete \
  -H 'Content-Type: application/json' \
  -d "{\"lease_token\": \"$TOKEN\", \"chunk_digest\": \"<分片0的SHA256>\"}"

# 4. 对其余分片重复 2-3 步，然后提交会话（缺分片时返回 409 + missing_chunks）
curl -s -X POST http://localhost:8000/sessions/$SID/finalize

# 5. 随时查询状态
curl -s http://localhost:8000/sessions/$SID
```

## 依赖调度示例（curl）

```bash
# 1. 创建带依赖的会话：分片 2 依赖 0 和 1，分片 3 依赖 2（键/值均为分片序号）
SID=$(curl -s -X POST http://localhost:8000/sessions \
  -H 'Content-Type: application/json' \
  -d '{"total_chunks": 4, "file_digest": "<文件SHA256>", "digest_algorithm": "sha256",
       "dependencies": {"2": [0, 1], "3": [2]}}' \
  | python3 -c 'import sys, json; print(json.load(sys.stdin)["id"])')

# 2. 查看当前可领取与被阻塞的分片
#    => ready_chunks: [0, 1]
#    => blocked_chunks: [{chunk_index: 2, missing_dependencies: [0, 1]}, {chunk_index: 3, ...}]
curl -s http://localhost:8000/sessions/$SID/ready

# 3. 前置未完成时领取分片 2：返回 409 及按序缺失的前置分片
#    => {"detail": "chunk dependencies not completed", "missing_dependencies": [0, 1]}
curl -s -X POST http://localhost:8000/sessions/$SID/chunks/2/lease

# 4. 完成分片 0、1 后，分片 2 自动变为可领取，无需额外操作
TOKEN=$(curl -s -X POST http://localhost:8000/sessions/$SID/chunks/0/lease \
  | python3 -c 'import sys, json; print(json.load(sys.stdin)["lease_token"])')
curl -s -X POST http://localhost:8000/sessions/$SID/chunks/0/complete \
  -H 'Content-Type: application/json' \
  -d "{\"lease_token\": \"$TOKEN\", \"chunk_digest\": \"<分片0的SHA256>\"}"
# ... 同样完成分片 1，之后即可正常领取分片 2

# 5. 状态查询中每个分片都带有 dependencies 字段
curl -s http://localhost:8000/sessions/$SID
```

## 失败上报与退避重试（curl）

```bash
# 1. 创建会话时指定退避参数（缺省 base=5、max=300，须为正且 max≥base）
SID=$(curl -s -X POST http://localhost:8000/sessions \
  -H 'Content-Type: application/json' \
  -d '{"total_chunks": 4, "file_digest": "<文件SHA256>",
       "base_retry_seconds": 5, "max_retry_seconds": 300}' \
  | python3 -c 'import sys, json; print(json.load(sys.stdin)["id"])')

# 2. 领取租约后传输失败，凭“当前有效租约令牌”上报失败
TOKEN=$(curl -s -X POST http://localhost:8000/sessions/$SID/chunks/0/lease \
  -H 'Content-Type: application/json' -d '{}' \
  | python3 -c 'import sys, json; print(json.load(sys.stdin)["lease_token"])')
curl -s -X POST http://localhost:8000/sessions/$SID/chunks/0/fail \
  -H 'Content-Type: application/json' -d "{\"lease_token\": \"$TOKEN\"}"
# => {"status": "pending", "failure_count": 1,
#     "retry_after_seconds": 5, "retry_after": "2026-...Z"}
# 第 n 次失败冷却 min(300, 5×2^(n-1))：5、10、20、40、80、160、300、300…

# 3. 冷却结束前，单片领取返回 409，批量领取跳过该分片
curl -s -X POST http://localhost:8000/sessions/$SID/chunks/0/lease \
  -H 'Content-Type: application/json' -d '{}'
# => 409 {"detail": "chunk in failure backoff",
#         "failure_count": 1, "retry_after": "2026-...Z"}
curl -s http://localhost:8000/sessions/$SID/ready
# => {"ready_chunks": [1,2,3], "blocked_chunks": [],
#     "cooling_chunks": [{"chunk_index": 0, "failure_count": 1, "retry_after": "..."}]}

# 4. 批量接口在无可领取项时，next_expiry = min(最早租约过期, 最早退避结束)
curl -s -X POST http://localhost:8000/sessions/$SID/leases/batch \
  -H 'Content-Type: application/json' -d '{"count": 8}'
# => {"granted": [...], "next_expiry": "2026-...Z", ...}

# 5. retry_after 过后重新领取并完成：退避状态清除，failure_count 保留
curl -s -X POST http://localhost:8000/sessions/$SID/chunks/0/complete \
  -H 'Content-Type: application/json' \
  -d "{\"lease_token\": \"<新租约令牌>\", \"chunk_digest\": \"<分片0的SHA256>\"}"
# 状态查询中：status=completed, failure_count=1, retry_after=null
```

## 完成纠错回退（curl）

已完成分片发现内容错误时，凭**完成时保留的租约令牌与摘要**调用 reopen，
将目标分片连同依赖图中的全部传递下游原子重置为 `pending` 后重新上传：

```bash
# 1. 创建带依赖的会话：2 依赖 0、1；3 依赖 2
SID=$(curl -s -X POST http://localhost:8000/sessions \
  -H 'Content-Type: application/json' \
  -d '{"total_chunks": 4, "file_digest": "<文件SHA256>",
       "dependencies": {"2": [0, 1], "3": [2]}}' \
  | python3 -c 'import sys, json; print(json.load(sys.stdin)["id"])')

# 2. 依次完成 0、1、2（略：领取租约 -> 上报完成），此时发现分片 1 的内容有误

# 3. 凭分片 1 完成时保留的 lease_token 与摘要发起回退
curl -s -X POST http://localhost:8000/sessions/$SID/chunks/1/reopen \
  -H 'Content-Type: application/json' \
  -d '{"lease_token": "<分片1完成时的租约令牌>", "expected_digest": "<分片1完成时上报的摘要>"}'
# => {
#      "session_id": "...", "chunk_index": 1, "revision": 1,
#      "affected_chunks": [
#        {"chunk_index": 1, "level": 0},
#        {"chunk_index": 2, "level": 1},
#        {"chunk_index": 3, "level": 2}
#      ]
#    }
# 目标 1 与全部传递下游（2、3）在同一事务内被重置为 pending：
# 摘要、完成时间、有效租约、退避截止时间全部清除，failure_count 保留；
# 分片 0 不受影响。会话已提交、分片未完成或凭据不匹配时返回 409 且不做任何修改。

# 4. 状态查询中的 revision 已递增（每次成功回退 +1），可据此判断发生过纠错
curl -s http://localhost:8000/sessions/$SID
# => {"revision": 1, "completed_chunks": 1, "missing_chunks": [1, 2, 3], ...}

# 5. 按依赖顺序重新领取、重新完成受影响分片后即可正常提交
curl -s -X POST http://localhost:8000/sessions/$SID/finalize
```

## 进度分支（fork，curl）

从一个进行中的会话**分叉**出新会话：已完成且依赖闭包完整的分片直接复用为
`completed`（只复制摘要与完成时间），其余分片回到 `pending` 重新上传；
源会话与新会话互不影响。新会话可用于「换个客户端/换条链路续传」或
「保留当前进度的同时重试剩余分片」。

```bash
# 1. 源会话：4 个分片，依赖图 2 -> [0, 1]、3 -> [2]；先完成 0、1、2
SID=...   # 同前，POST /sessions 创建并逐分片领取/完成

# 2. 基于源会话当前修订号（GET /sessions/$SID 的 revision 字段）创建分支，
#    复用已完成的 0、1、2（集合包含全部传递前置：2 的前置 0、1 也在列表中）
curl -s -X POST http://localhost:8000/sessions/$SID/fork \
  -H 'Content-Type: application/json' \
  -d '{"expected_revision": 0, "request_id": "resume-2026-09-19-001",
       "reuse_chunks": [0, 1, 2]}'
# => 201 {
#      "id": "<新会话id>", "source_session_id": "<源会话id>",
#      "request_id": "resume-2026-09-19-001",
#      "total_chunks": 4, "status": "active", "revision": 0,
#      "reused_chunks": [0, 1, 2], "pending_chunks": [3],
#      "file_digest": "...", "digest_algorithm": "sha256",
#      "max_active_leases": null, "base_retry_seconds": 5,
#      "max_retry_seconds": 300, "idempotent_replay": false,
#      "created_at": "2026-..."
#    }
# 新会话中 0、1、2 为 completed（摘要/完成时间已复制），3 为 pending；
# 租约、退避冷却、failure_count 均不复制，3 可立即领取后完成并提交。

# 3. 前置闭包不完整：只复用 2 而不带 0、1 -> 409 且不落库
curl -s -X POST http://localhost:8000/sessions/$SID/fork \
  -H 'Content-Type: application/json' \
  -d '{"expected_revision": 0, "request_id": "bad-closure",
       "reuse_chunks": [2]}'
# => 409 {"detail": "reuse_chunks must include all transitive prerequisites",
#         "missing_prerequisites": [0, 1]}
# 复用了未完成（或越界）分片：
# => 409 {"detail": "reuse_chunks must reference completed chunks only",
#         "not_completed_chunks": [3]}
# 修订号不匹配（源会话期间发生过 reopen）：
# => 409 {"detail": "revision mismatch",
#         "expected_revision": 0, "current_revision": 1}

# 4. 网络重试安全：同一 request_id + 相同参数重放，返回首次创建的会话
#    （200，idempotent_replay=true；reuse_chunks 顺序不同或含重复项视为同参数）
curl -s -X POST http://localhost:8000/sessions/$SID/fork \
  -H 'Content-Type: application/json' \
  -d '{"expected_revision": 0, "request_id": "resume-2026-09-19-001",
       "reuse_chunks": [2, 1, 0]}'
# => 200 {"id": "<与首次相同的新会话id>", "idempotent_replay": true, ...}
# 相同 request_id 但参数变化 -> 409
#    {"detail": "request_id already used with different parameters"}

# 5. 状态查询暴露来源会话（原生会话为 null）
curl -s http://localhost:8000/sessions/<新会话id>
# => {"source_session_id": "<源会话id>", "revision": 0, ...}
```

## 跨会话公平调度（curl）

多个上传会话组成一个逻辑队列，调用方只关心「给我下一个可做的分片」：服务端按
`queue_key` 维护轮转游标，每次从游标之后的首个可用会话发放其最低序号可领取分片，
连续请求在可用会话间公平轮转；某会话临时不可用（租约占满、退避冷却、并发槽位满）
时会被自动跳过，恢复后重新参与轮转。

```bash
# 1. 准备两个上传会话（略：POST /sessions 创建，记为 SID_A、SID_B）

# 2. 同一 queue_key 连续领取：第一次在 SID_A，第二次轮转到 SID_B，第三次回到 SID_A
curl -s -X POST http://localhost:8000/dispatch/lease \
  -H 'Content-Type: application/json' \
  -d "{\"queue_key\": \"ingest-2026-09\", \"session_ids\": [\"$SID_A\", \"$SID_B\"],
       \"lease_ttl_seconds\": 120}"
# => {"queue_key": "ingest-2026-09", "granted": true,
#     "session_id": "<SID_A>", "chunk_index": 0,
#     "lease_token": "...", "expires_at": "2026-...",
#     "unavailable": [], "next_retry_at": null}

# 3. 拿到的是普通分片租约：完成/失败上报沿用既有接口，令牌完全通用
curl -s -X POST http://localhost:8000/sessions/$SID_A/chunks/0/complete \
  -H 'Content-Type: application/json' \
  -d "{\"lease_token\": \"$TOKEN\", \"chunk_digest\": \"<分片0的SHA256>\"}"

# 4. 所有候选会话都不可发放时：granted=false，逐会话给出原因，
#    next_retry_at 为各会话最早可重试时刻的最小值；游标不推进
curl -s -X POST http://localhost:8000/dispatch/lease \
  -H 'Content-Type: application/json' \
  -d "{\"queue_key\": \"ingest-2026-09\", \"session_ids\": [\"$SID_A\", \"$SID_B\"]}"
# => {"queue_key": "ingest-2026-09", "granted": false,
#     "session_id": null, "chunk_index": null, "lease_token": null, "expires_at": null,
#     "unavailable": [
#       {"session_id": "<SID_A>", "reason": "leases_active",
#        "next_retry_at": "2026-..."},
#       {"session_id": "<SID_B>", "reason": "in_backoff",
#        "next_retry_at": "2026-..."}
#     ],
#     "next_retry_at": "2026-..."}
```

候选列表**去重后按（创建时间, id）稳定排序**，与请求中的顺序无关；未知会话 id
按 id 排在最后并以 `not_found` 归因。游标只在成功发放时推进；不同 `queue_key`
的游标相互独立。发放的租约与单片/批量领取的租约完全等价：占用会话
`max_active_leases` 槽位，过期后可被重新领取。

## 本地开发（不用 Docker）

```bash
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

运行测试（需额外安装 `pytest`、`httpx`）：

```bash
python3 -m pytest tests/ -q
```

## 项目结构

```
app/
  main.py      # FastAPI 应用与全部端点
  db.py        # SQLite 连接与建库建表（启动时自动执行）
  schemas.py   # Pydantic 请求/响应模型
tests/
  test_api.py  # 租约独占/过期、幂等、提交等端到端行为测试
Dockerfile
docker-compose.yml
requirements.txt
```
