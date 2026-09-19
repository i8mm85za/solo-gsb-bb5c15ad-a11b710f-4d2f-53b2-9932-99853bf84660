"""断点分片上传协调 API。

仅协调元数据：不接收、不保存任何文件内容。
- 创建会话：记录总分片数与期望文件摘要，可选提交分片依赖图（越界/自依赖/成环将被拒绝），
  可选失败退避参数 base_retry_seconds（默认 5）与 max_retry_seconds（默认 300）
- 领取租约：同一分片同一时刻仅一个有效租约，过期后可重新领取；
  声明了依赖的分片须等前置分片全部完成后才可领取；失败退避冷却中的分片不可领取
- 上报完成：需携带匹配租约令牌与分片摘要，重复上报幂等；完成后清除退避冷却、保留累计失败次数
- 上报失败：需携带当前有效租约令牌，成功后原子清除租约并递增失败次数，
  第 n 次失败冷却 min(max, base×2^(n-1)) 秒，冷却期间单片/批量领取均跳过该分片
- 完成纠错回退（reopen）：凭目标分片完成时保留的租约令牌与摘要发起，仅允许未提交会话中
  已完成且凭据匹配的分片；同一事务内将目标及依赖图中的全部传递下游重置为 pending
  （清除摘要、完成时间、有效租约与退避截止时间，保留累计失败次数），会话 revision 递增 1；
  任一校验失败不产生部分回退，并发领取不会看到中间状态
- 提交会话：仅当全部分片完成，返回按序分片清单，否则列出缺失分片；
  会话使用 sha256-merkle-v1 时先按分片序号构建 Merkle 树并与 file_digest
  常量时间比较，不匹配返回 409 及 computed_digest、会话保持 active
- 进度分支（fork）：POST /sessions/{id}/fork 携带 expected_revision、幂等 request_id
  与 reuse_chunks，仅可从 active 会话创建新会话；复用分片必须全部已完成且包含其全部
  传递前置分片，否则返回 409 及缺失项且不落库；新会话复制会话配置与依赖图，复用分片
  复制摘要与完成时间为 completed，其余为 pending，不复制租约、退避与失败次数；
  同一源会话 + request_id 重放返回首次新会话，参数变化返回 409
- 状态查询：会话与逐分片状态（含各分片依赖、失败次数与冷却截止时间，以及会话 revision
  与 source_session_id）
- 就绪查询：当前可领取的分片、被依赖阻塞的分片以及退避冷却中的分片
- 跨会话公平调度：POST /dispatch/lease 携带 queue_key、候选 session_ids 与
  lease_ttl_seconds；候选去重后按（创建时间, id）稳定排序，服务端按 queue_key 持久化
  最近一次成功会话作为游标，在同一写锁事务内从游标之后循环扫描，从首个可用会话领取
  最低序号可领取分片（资格判断复用依赖、退避、有效租约与 max_active_leases 规则，
  并发请求不重复发放）；无可发放项时不推进游标，返回各会话不可用原因与最早
  next_retry_at；成功后才更新游标，使连续请求在可用会话间轮转
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import secrets
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import Body, FastAPI, HTTPException
from fastapi.responses import JSONResponse

from .db import DB_LOCK, get_conn, init_db
from .schemas import (
    MERKLE_DIGEST_ALGORITHM,
    BatchLeaseItem,
    BatchLeaseRequest,
    BatchLeaseResponse,
    BlockedChunk,
    ChunkStatus,
    CompleteRequest,
    CompleteResponse,
    CoolingChunk,
    CreateSessionRequest,
    DispatchLeaseRequest,
    DispatchLeaseResponse,
    DispatchUnavailableSession,
    FailRequest,
    FailResponse,
    FinalizeResponse,
    ForkSessionRequest,
    ForkSessionResponse,
    LeaseRequest,
    LeaseResponse,
    ManifestEntry,
    ReadyResponse,
    ReopenAffectedChunk,
    ReopenRequest,
    ReopenResponse,
    SessionResponse,
    SessionStatusResponse,
    is_valid_merkle_digest,
)

DEFAULT_LEASE_TTL_SECONDS = int(os.environ.get("DEFAULT_LEASE_TTL_SECONDS", "300"))
MAX_LEASE_TTL_SECONDS = int(os.environ.get("MAX_LEASE_TTL_SECONDS", "3600"))
DEFAULT_BASE_RETRY_SECONDS = int(os.environ.get("DEFAULT_BASE_RETRY_SECONDS", "5"))
DEFAULT_MAX_RETRY_SECONDS = int(os.environ.get("DEFAULT_MAX_RETRY_SECONDS", "300"))


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value)


@asynccontextmanager
async def lifespan(_: FastAPI):
    init_db()  # 启动时建库建表
    yield


app = FastAPI(
    title="Chunked Upload Coordinator",
    description="断点分片上传协调服务：只管理元数据（会话 / 租约 / 分片摘要），不接收文件内容。",
    version="1.0.0",
    lifespan=lifespan,
)


# ---------------------------------------------------------------- 工具函数

def get_session_or_404(session_id: str):
    row = get_conn().execute("SELECT * FROM sessions WHERE id = ?", (session_id,)).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="session not found")
    return row


def get_chunk_or_404(session_id: str, chunk_index: int):
    row = get_conn().execute(
        "SELECT * FROM chunks WHERE session_id = ? AND chunk_index = ?",
        (session_id, chunk_index),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="chunk not found")
    return row


def lease_valid(chunk, now: datetime) -> bool:
    return (
        chunk["lease_expires_at"] is not None
        and parse_ts(chunk["lease_expires_at"]) > now
    )


def backoff_active(chunk, now: datetime) -> bool:
    """该分片是否正处于失败退避冷却中（backoff_until 为空或已过期均视为未冷却）。"""
    return (
        chunk["backoff_until"] is not None
        and parse_ts(chunk["backoff_until"]) > now
    )


def backoff_delay(session, failure_count: int) -> int:
    """第 n 次失败的冷却秒数：min(max_retry_seconds, base_retry_seconds × 2^(n-1))。"""
    base = session["base_retry_seconds"]
    cap = session["max_retry_seconds"]
    return min(cap, base * (2 ** (failure_count - 1)))


def merkle_root_sha256(digests: list[str]) -> str:
    """sha256-merkle-v1：按分片序号升序的摘要列表（64 位小写十六进制）计算 Merkle 根。

    字节级规则：
    - 叶节点 = SHA-256(0x00 || 摘要的原始 32 字节)
    - 父节点 = SHA-256(0x01 || 左节点 32 字节 || 右节点 32 字节)
    - 每层节点数为奇数时，末节点复制自身后配对
    - 逐层归约直至单一根，返回其小写十六进制编码
    """
    level = [hashlib.sha256(b"\x00" + bytes.fromhex(d)).digest() for d in digests]
    while len(level) > 1:
        if len(level) % 2 == 1:
            level.append(level[-1])  # 奇数末节点复制自身后配对
        level = [
            hashlib.sha256(b"\x01" + level[i] + level[i + 1]).digest()
            for i in range(0, len(level), 2)
        ]
    return level[0].hex()


def load_dependencies(session_id: str) -> dict[int, list[int]]:
    """读取会话的依赖图：chunk_index -> 升序前置分片列表。"""
    rows = get_conn().execute(
        "SELECT chunk_index, depends_on FROM chunk_dependencies"
        " WHERE session_id = ? ORDER BY chunk_index, depends_on",
        (session_id,),
    ).fetchall()
    deps: dict[int, list[int]] = {}
    for row in rows:
        deps.setdefault(row["chunk_index"], []).append(row["depends_on"])
    return deps


def missing_transitive_prerequisites(
    deps: dict[int, list[int]], reuse: set[int]
) -> list[int]:
    """临界区内调用：返回 reuse 集合中各分片依赖图上的全部传递前置分片中，
    未被 reuse 集合包含的部分（升序）。空列表表示传递闭包完整。"""
    missing: set[int] = set()
    for chunk_index in reuse:
        stack = list(deps.get(chunk_index, []))
        while stack:
            pred = stack.pop()
            if pred in reuse or pred in missing:
                continue
            missing.add(pred)
            stack.extend(deps.get(pred, []))
    return sorted(missing)


def incomplete_dependencies(session_id: str, chunk_index: int) -> list[int]:
    """返回该分片尚未完成的前置分片（升序）。"""
    rows = get_conn().execute(
        "SELECT d.depends_on FROM chunk_dependencies d"
        " JOIN chunks c ON c.session_id = d.session_id AND c.chunk_index = d.depends_on"
        " WHERE d.session_id = ? AND d.chunk_index = ? AND c.status != 'completed'"
        " ORDER BY d.depends_on",
        (session_id, chunk_index),
    ).fetchall()
    return [row["depends_on"] for row in rows]


def collect_reopen_affected(session_id: str, target: int) -> list[tuple[int, int]]:
    """临界区内调用：沿依赖图找出目标分片的全部传递下游，返回
    （分片序号, 拓扑层级）列表，按（层级, 序号）升序稳定排列。

    受影响集合 = 目标分片 + 全部传递下游；目标层级为 0，其余节点取受影响
    子图中的最长路径层级（Kahn 分层：节点仅在全部受影响前置处理完后进入
    下一层），保证按（层级, 序号）排序后任意受影响前置都排在其下游之前。
    依赖图在创建会话时已校验无环，分层必然收敛。
    """
    deps = load_dependencies(session_id)  # chunk_index -> 升序前置列表
    children: dict[int, list[int]] = {}
    for chunk_index, preds in deps.items():
        for pred in preds:
            children.setdefault(pred, []).append(chunk_index)

    # 沿反向边（前置 -> 依赖它的分片）收集全部传递下游
    affected = {target}
    stack = [target]
    while stack:
        node = stack.pop()
        for child in children.get(node, []):
            if child not in affected:
                affected.add(child)
                stack.append(child)

    # Kahn 分层：仅统计受影响子图内的入度，逐波推进得到最长路径层级
    indegree = {
        n: sum(1 for p in deps.get(n, []) if p in affected) for n in affected
    }
    level: dict[int, int] = {}
    frontier = [n for n in affected if indegree[n] == 0]
    current = 0
    while frontier:
        nxt: list[int] = []
        for node in frontier:
            level[node] = current
            for child in children.get(node, []):
                if child in affected:
                    indegree[child] -= 1
                    if indegree[child] == 0:
                        nxt.append(child)
        frontier = nxt
        current += 1

    return sorted(level.items(), key=lambda item: (item[1], item[0]))


def count_active_leases(session_id: str, now: datetime) -> int:
    """会话内当前有效（未过期）租约数；已完成分片与过期租约均不计入。"""
    return get_conn().execute(
        "SELECT COUNT(*) AS n FROM chunks"
        " WHERE session_id = ? AND status != 'completed'"
        " AND lease_expires_at IS NOT NULL AND lease_expires_at > ?",
        (session_id, now.isoformat()),
    ).fetchone()["n"]


def earliest_wakeup(session_id: str, now: datetime) -> Optional[datetime]:
    """会话内最早的可重试时刻：有效租约的最早过期时间与各分片退避冷却的最早结束时间
    中更早者；均不存在（或均已过期）时返回 None。"""
    conn = get_conn()
    lease_row = conn.execute(
        "SELECT MIN(lease_expires_at) AS earliest FROM chunks"
        " WHERE session_id = ? AND status != 'completed'"
        " AND lease_expires_at IS NOT NULL AND lease_expires_at > ?",
        (session_id, now.isoformat()),
    ).fetchone()
    backoff_row = conn.execute(
        "SELECT MIN(backoff_until) AS earliest FROM chunks"
        " WHERE session_id = ? AND status != 'completed'"
        " AND backoff_until IS NOT NULL AND backoff_until > ?",
        (session_id, now.isoformat()),
    ).fetchone()
    candidates = [
        parse_ts(lease_row["earliest"]) if lease_row["earliest"] else None,
        parse_ts(backoff_row["earliest"]) if backoff_row["earliest"] else None,
    ]
    candidates = [ts for ts in candidates if ts is not None]
    return min(candidates) if candidates else None


def claimable_chunks(session_id: str, now: datetime) -> tuple[list[int], int]:
    """临界区内调用：返回（按序号升序的可领取分片列表, 被依赖阻塞的未完成分片数）。

    可领取 = 未完成、无有效租约（过期租约视为空闲）、不在失败退避冷却中且前置分片全部完成；
    被阻塞 = 未完成且至少一个前置分片尚未完成（无论该分片当前是否持有有效租约或处于冷却）。
    """
    rows = get_conn().execute(
        "SELECT * FROM chunks WHERE session_id = ? ORDER BY chunk_index",
        (session_id,),
    ).fetchall()
    completed = {r["chunk_index"] for r in rows if r["status"] == "completed"}
    deps = load_dependencies(session_id)
    ready: list[int] = []
    blocked = 0
    for row in rows:
        if row["status"] == "completed":
            continue
        chunk_index = row["chunk_index"]
        if any(d not in completed for d in deps.get(chunk_index, [])):
            blocked += 1
        elif not lease_valid(row, now) and not backoff_active(row, now):
            ready.append(chunk_index)
    return ready, blocked


def conflict(message: str, **extra) -> JSONResponse:
    return JSONResponse(status_code=409, content={"detail": message, **extra})


# ---------------------------------------------------------------- 会话

@app.post(
    "/sessions",
    status_code=201,
    response_model=SessionResponse,
    tags=["sessions"],
    summary="创建上传会话",
)
def create_session(req: CreateSessionRequest):
    now = utcnow()
    session_id = uuid.uuid4().hex
    deps = req.dependencies or {}
    with DB_LOCK:
        conn = get_conn()
        conn.execute(
            "INSERT INTO sessions (id, total_chunks, file_digest, digest_algorithm, status,"
            " max_active_leases, base_retry_seconds, max_retry_seconds, created_at)"
            " VALUES (?, ?, ?, ?, 'active', ?, ?, ?, ?)",
            (session_id, req.total_chunks, req.file_digest, req.digest_algorithm,
             req.max_active_leases, req.base_retry_seconds, req.max_retry_seconds,
             now.isoformat()),
        )
        conn.executemany(
            "INSERT INTO chunks (session_id, chunk_index) VALUES (?, ?)",
            [(session_id, i) for i in range(req.total_chunks)],
        )
        conn.executemany(
            "INSERT INTO chunk_dependencies (session_id, chunk_index, depends_on) VALUES (?, ?, ?)",
            [(session_id, idx, dep) for idx, dep_list in deps.items() for dep in dep_list],
        )
        conn.commit()
    return SessionResponse(
        id=session_id,
        total_chunks=req.total_chunks,
        file_digest=req.file_digest,
        digest_algorithm=req.digest_algorithm,
        status="active",
        source_session_id=None,
        max_active_leases=req.max_active_leases,
        base_retry_seconds=req.base_retry_seconds,
        max_retry_seconds=req.max_retry_seconds,
        created_at=now,
    )


@app.get(
    "/sessions/{session_id}",
    response_model=SessionStatusResponse,
    tags=["sessions"],
    summary="查询会话状态",
)
def get_session_status(session_id: str):
    session = get_session_or_404(session_id)
    rows = get_conn().execute(
        "SELECT * FROM chunks WHERE session_id = ? ORDER BY chunk_index",
        (session_id,),
    ).fetchall()
    deps = load_dependencies(session_id)
    now = utcnow()

    chunks: list[ChunkStatus] = []
    missing: list[int] = []
    for row in rows:
        status = row["status"]
        lease_expires_at = parse_ts(row["lease_expires_at"]) if row["lease_expires_at"] else None
        if status == "leased" and (lease_expires_at is None or lease_expires_at <= now):
            status = "pending"  # 租约已过期，读时按可领取展示
            lease_expires_at = None
        # 冷却截止时间按读时口径展示：已过期的退避不再报告
        retry_after = parse_ts(row["backoff_until"]) if row["backoff_until"] else None
        if retry_after is not None and retry_after <= now:
            retry_after = None
        if row["status"] != "completed":
            missing.append(row["chunk_index"])
        chunks.append(
            ChunkStatus(
                chunk_index=row["chunk_index"],
                status=status,
                lease_expires_at=lease_expires_at,
                chunk_digest=row["chunk_digest"],
                completed_at=parse_ts(row["completed_at"]) if row["completed_at"] else None,
                dependencies=deps.get(row["chunk_index"], []),
                failure_count=row["failure_count"] or 0,
                retry_after=retry_after,
            )
        )

    return SessionStatusResponse(
        id=session["id"],
        status=session["status"],
        total_chunks=session["total_chunks"],
        completed_chunks=session["total_chunks"] - len(missing),
        missing_chunks=missing,
        file_digest=session["file_digest"],
        digest_algorithm=session["digest_algorithm"],
        source_session_id=session["source_session_id"],
        revision=session["revision"],
        max_active_leases=session["max_active_leases"],
        base_retry_seconds=session["base_retry_seconds"],
        max_retry_seconds=session["max_retry_seconds"],
        active_leases=sum(
            1 for c in chunks
            if c.status == "leased" and c.lease_expires_at is not None
        ),
        created_at=parse_ts(session["created_at"]),
        finalized_at=parse_ts(session["finalized_at"]) if session["finalized_at"] else None,
        chunks=chunks,
    )


# ---------------------------------------------------------------- 进度分支

def build_fork_response(
    new_id: str,
    source_id: str,
    request_id: str,
    session,
    reuse: list[int],
    created_at: datetime,
    idempotent_replay: bool,
) -> ForkSessionResponse:
    reuse_set = set(reuse)
    return ForkSessionResponse(
        id=new_id,
        source_session_id=source_id,
        request_id=request_id,
        total_chunks=session["total_chunks"],
        file_digest=session["file_digest"],
        digest_algorithm=session["digest_algorithm"],
        status="active",
        revision=0,
        reused_chunks=reuse,
        pending_chunks=[i for i in range(session["total_chunks"]) if i not in reuse_set],
        max_active_leases=session["max_active_leases"],
        base_retry_seconds=session["base_retry_seconds"],
        max_retry_seconds=session["max_retry_seconds"],
        created_at=created_at,
        idempotent_replay=idempotent_replay,
    )


@app.post(
    "/sessions/{session_id}/fork",
    response_model=ForkSessionResponse,
    status_code=201,
    tags=["sessions"],
    summary="从 active 会话分叉出新的上传会话（复用已完成分片进度）",
    responses={
        200: {"description": "同一 request_id 的幂等重放（idempotent_replay=true，返回首次新会话）"},
        409: {"description": "源会话已提交、修订号不匹配、复用分片未完成或缺少传递前置、"
                             "同一 request_id 参数变化"},
    },
)
def fork_session(session_id: str, req: ForkSessionRequest):
    """在同一 DB_LOCK 事务内：校验源会话存在且 active → 校验 expected_revision 与源会话
    revision 一致 → 校验 reuse_chunks 中每项均已完成（越界序号同样视为未完成）→ 校验
    reuse_chunks 包含其全部传递前置分片。任一校验失败返回 409 且不落库。

    通过后复制会话配置（total_chunks / file_digest / digest_algorithm /
    max_active_leases / 退避参数）与完整依赖图；复用分片复制摘要与完成时间为 completed，
    其余分片初始化为 pending——不复制租约、退避截止时间与累计失败次数。
    （源会话, request_id）唯一：参数一致的重放返回首次创建的新会话，参数变化返回 409。
    """
    now = utcnow()
    reuse = sorted(set(req.reuse_chunks))
    reuse_set = set(reuse)
    with DB_LOCK:
        conn = get_conn()
        session = get_session_or_404(session_id)

        # 幂等优先：同一（源会话, request_id）已存在时，按参数决定重放或冲突
        existing = conn.execute(
            "SELECT * FROM fork_requests WHERE source_session_id = ? AND request_id = ?",
            (session_id, req.request_id),
        ).fetchone()
        if existing is not None:
            params_changed = (
                existing["expected_revision"] != req.expected_revision
                or json.loads(existing["reuse_chunks"]) != reuse
            )
            if params_changed:
                raise HTTPException(
                    status_code=409,
                    detail="request_id already used with different parameters",
                )
            new_session = conn.execute(
                "SELECT * FROM sessions WHERE id = ?",
                (existing["new_session_id"],),
            ).fetchone()
            return JSONResponse(
                status_code=200,
                content=build_fork_response(
                    new_id=new_session["id"],
                    source_id=session_id,
                    request_id=req.request_id,
                    session=new_session,
                    reuse=reuse,
                    created_at=parse_ts(existing["created_at"]),
                    idempotent_replay=True,
                ).model_dump(mode="json"),
            )

        if session["status"] == "finalized":
            raise HTTPException(status_code=409, detail="session already finalized")
        if session["revision"] != req.expected_revision:
            return conflict(
                "revision mismatch",
                expected_revision=req.expected_revision,
                current_revision=session["revision"],
            )

        rows = conn.execute(
            "SELECT chunk_index, status, chunk_digest, completed_at FROM chunks"
            " WHERE session_id = ?",
            (session_id,),
        ).fetchall()
        by_index = {r["chunk_index"]: r for r in rows}
        not_completed = sorted(
            idx for idx in reuse
            if idx not in by_index or by_index[idx]["status"] != "completed"
        )
        if not_completed:
            return conflict(
                "reuse_chunks must reference completed chunks only",
                not_completed_chunks=not_completed,
            )

        deps = load_dependencies(session_id)
        missing_prerequisites = missing_transitive_prerequisites(deps, reuse_set)
        if missing_prerequisites:
            return conflict(
                "reuse_chunks must include all transitive prerequisites",
                missing_prerequisites=missing_prerequisites,
            )

        # 全部校验先于写入：以下任一失败都不会发生，整个复制在同一事务/临界区内完成
        new_id = uuid.uuid4().hex
        conn.execute(
            "INSERT INTO sessions (id, total_chunks, file_digest, digest_algorithm, status,"
            " max_active_leases, base_retry_seconds, max_retry_seconds, revision,"
            " source_session_id, created_at)"
            " VALUES (?, ?, ?, ?, 'active', ?, ?, ?, 0, ?, ?)",
            (new_id, session["total_chunks"], session["file_digest"],
             session["digest_algorithm"], session["max_active_leases"],
             session["base_retry_seconds"], session["max_retry_seconds"],
             session_id, now.isoformat()),
        )
        # 复用分片：复制摘要与完成时间为 completed；其余默认 pending。
        # 不复制 lease_token / lease_expires_at / backoff_until / failure_count
        reused_rows = [
            (new_id, idx, by_index[idx]["chunk_digest"], by_index[idx]["completed_at"])
            for idx in reuse
        ]
        pending_rows = [
            (new_id, idx)
            for idx in range(session["total_chunks"])
            if idx not in reuse_set
        ]
        conn.executemany(
            "INSERT INTO chunks (session_id, chunk_index, status, chunk_digest,"
            " completed_at) VALUES (?, ?, 'completed', ?, ?)",
            reused_rows,
        )
        conn.executemany(
            "INSERT INTO chunks (session_id, chunk_index) VALUES (?, ?)",
            pending_rows,
        )
        # 完整复制依赖图：新会话的调度约束与源会话一致
        conn.execute(
            "INSERT INTO chunk_dependencies (session_id, chunk_index, depends_on)"
            " SELECT ?, chunk_index, depends_on FROM chunk_dependencies WHERE session_id = ?",
            (new_id, session_id),
        )
        conn.execute(
            "INSERT INTO fork_requests (source_session_id, request_id, new_session_id,"
            " expected_revision, reuse_chunks, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (session_id, req.request_id, new_id, req.expected_revision,
             json.dumps(reuse), now.isoformat()),
        )
        conn.commit()

    return build_fork_response(
        new_id=new_id,
        source_id=session_id,
        request_id=req.request_id,
        session=session,
        reuse=reuse,
        created_at=now,
        idempotent_replay=False,
    )


# ---------------------------------------------------------------- 就绪查询

@app.get(
    "/sessions/{session_id}/ready",
    response_model=ReadyResponse,
    tags=["sessions"],
    summary="查询当前可领取的分片与被依赖阻塞的分片",
)
def get_ready_chunks(session_id: str):
    get_session_or_404(session_id)
    rows = get_conn().execute(
        "SELECT * FROM chunks WHERE session_id = ? ORDER BY chunk_index",
        (session_id,),
    ).fetchall()
    deps = load_dependencies(session_id)
    now = utcnow()

    completed = {r["chunk_index"] for r in rows if r["status"] == "completed"}
    ready: list[int] = []
    blocked: list[BlockedChunk] = []
    cooling: list[CoolingChunk] = []
    for row in rows:
        chunk_index = row["chunk_index"]
        if row["status"] == "completed":
            continue
        missing_deps = [d for d in deps.get(chunk_index, []) if d not in completed]
        if missing_deps:
            blocked.append(
                BlockedChunk(chunk_index=chunk_index, missing_dependencies=missing_deps)
            )
        elif backoff_active(row, now):
            # 失败退避冷却中：不可被单片或批量领取，冷却结束后自动恢复
            cooling.append(
                CoolingChunk(
                    chunk_index=chunk_index,
                    failure_count=row["failure_count"] or 0,
                    retry_after=parse_ts(row["backoff_until"]),
                )
            )
        elif not lease_valid(row, now):
            # 无租约或租约已过期，且前置已全部完成：立即可领取
            ready.append(chunk_index)

    return ReadyResponse(
        session_id=session_id,
        ready_chunks=ready,
        blocked_chunks=blocked,
        cooling_chunks=cooling,
    )


# ---------------------------------------------------------------- 租约

@app.post(
    "/sessions/{session_id}/chunks/{chunk_index}/lease",
    response_model=LeaseResponse,
    tags=["leases"],
    summary="领取分片租约（限时，独占；前置分片未完成时返回 409）",
    responses={409: {"description": "租约冲突；前置未完成时响应体包含 missing_dependencies"}},
)
def claim_lease(
    session_id: str,
    chunk_index: int,
    req: Optional[LeaseRequest] = Body(default=None),
):
    ttl = DEFAULT_LEASE_TTL_SECONDS
    if req is not None and req.lease_ttl_seconds is not None:
        ttl = min(req.lease_ttl_seconds, MAX_LEASE_TTL_SECONDS)

    now = utcnow()
    with DB_LOCK:
        session = get_session_or_404(session_id)
        if session["status"] == "finalized":
            raise HTTPException(status_code=409, detail="session already finalized")
        chunk = get_chunk_or_404(session_id, chunk_index)

        if chunk["status"] == "completed":
            raise HTTPException(status_code=409, detail="chunk already completed")
        if lease_valid(chunk, now):
            return conflict(
                "chunk lease already held",
                lease_expires_at=chunk["lease_expires_at"],
            )
        if backoff_active(chunk, now):
            # 失败退避冷却中：单片领取同样拒绝，直至 retry_after
            return conflict(
                "chunk in failure backoff",
                failure_count=chunk["failure_count"] or 0,
                retry_after=chunk["backoff_until"],
            )
        missing_deps = incomplete_dependencies(session_id, chunk_index)
        if missing_deps:
            return conflict(
                "chunk dependencies not completed",
                missing_dependencies=missing_deps,
            )
        # 会话级并发上限：已占满槽位（过期租约不计入）时拒绝，避免单片接口绕过批量上限
        max_active = session["max_active_leases"]
        active = count_active_leases(session_id, now)
        if max_active is not None and active >= max_active:
            return conflict(
                "session max active leases reached",
                max_active_leases=max_active,
                active_leases=active,
            )

        token = secrets.token_urlsafe(32)
        expires_at = now + timedelta(seconds=ttl)
        get_conn().execute(
            "UPDATE chunks SET status = 'leased', lease_token = ?, lease_expires_at = ?"
            " WHERE session_id = ? AND chunk_index = ?",
            (token, expires_at.isoformat(), session_id, chunk_index),
        )
        get_conn().commit()

    return LeaseResponse(
        session_id=session_id,
        chunk_index=chunk_index,
        lease_token=token,
        expires_at=expires_at,
    )


@app.post(
    "/sessions/{session_id}/leases/batch",
    response_model=BatchLeaseResponse,
    tags=["leases"],
    summary="会话级原子批量领取租约（按分片序号，受并发上限约束）",
)
def claim_leases_batch(session_id: str, req: BatchLeaseRequest):
    """一次领取至多 count 个租约：资格计算与写入在同一临界区，保证并发请求不重复分配、
    不突破 max_active_leases；数量不超过请求值与剩余并发槽位；无可领取项时返回空列表。"""
    ttl = DEFAULT_LEASE_TTL_SECONDS
    if req.lease_ttl_seconds is not None:
        ttl = min(req.lease_ttl_seconds, MAX_LEASE_TTL_SECONDS)

    now = utcnow()
    with DB_LOCK:
        session = get_session_or_404(session_id)
        if session["status"] == "finalized":
            raise HTTPException(status_code=409, detail="session already finalized")

        # 资格计算与写入均在 DB_LOCK 临界区内：其它领取请求无法穿插看到中间状态
        ready, blocked = claimable_chunks(session_id, now)
        max_active = session["max_active_leases"]
        active = count_active_leases(session_id, now)
        remaining = None if max_active is None else max(0, max_active - active)
        quota = req.count if remaining is None else min(req.count, remaining)

        granted: list[BatchLeaseItem] = []
        if quota > 0:
            expires_at = now + timedelta(seconds=ttl)
            conn = get_conn()
            for chunk_index in ready[:quota]:
                token = secrets.token_urlsafe(32)
                conn.execute(
                    "UPDATE chunks SET status = 'leased', lease_token = ?, lease_expires_at = ?"
                    " WHERE session_id = ? AND chunk_index = ?",
                    (token, expires_at.isoformat(), session_id, chunk_index),
                )
                granted.append(
                    BatchLeaseItem(
                        chunk_index=chunk_index,
                        lease_token=token,
                        expires_at=expires_at,
                    )
                )
            conn.commit()

        active_after = active + len(granted)
        # 无可领取项时，调用方可据 next_expiry 等待：有效租约过期或退避冷却结束，取更早者
        next_expiry = earliest_wakeup(session_id, now)

    return BatchLeaseResponse(
        session_id=session_id,
        requested=req.count,
        granted=granted,
        active=active_after,
        blocked=blocked,
        next_expiry=next_expiry,
        max_active_leases=max_active,
        remaining_slots=(
            None if max_active is None else max(0, max_active - active_after)
        ),
    )


# ---------------------------------------------------------------- 跨会话公平调度

def dispatch_unavailability(session, now: datetime) -> tuple[str, Optional[datetime]]:
    """临界区内调用：会话当前不可发放租约时的归因与最早可重试时刻。

    前提：claimable_chunks 已判定该会话没有可领取分片，或并发槽位已满。
    最早可重试时刻复用 earliest_wakeup：有效租约最早过期时间与各分片退避
    最早结束时间中更早者。
    """
    sid = session["id"]
    next_retry = earliest_wakeup(sid, now)
    max_active = session["max_active_leases"]
    if max_active is not None and count_active_leases(sid, now) >= max_active:
        return "max_active_leases_reached", next_retry
    rows = get_conn().execute(
        "SELECT * FROM chunks WHERE session_id = ? AND status != 'completed'"
        " ORDER BY chunk_index",
        (sid,),
    ).fetchall()
    if not rows:
        return "completed", None  # 分片全部完成、等待提交：无时间性等待事件
    if any(lease_valid(r, now) for r in rows):
        return "leases_active", next_retry
    if any(backoff_active(r, now) for r in rows):
        return "in_backoff", next_retry
    # 剩余情形：全部未完成分片均被依赖阻塞。依赖图无环且无任何在途租约/冷却时
    # 必然存在可领取分片，正常不会到达；保留该原因以保证响应始终可解释
    return "blocked_by_dependencies", next_retry


@app.post(
    "/dispatch/lease",
    response_model=DispatchLeaseResponse,
    tags=["dispatch"],
    summary="跨会话公平调度：按 queue_key 轮转，从首个可用会话领取最低序号分片",
)
def dispatch_lease(req: DispatchLeaseRequest):
    """候选会话去重后按（创建时间, id）稳定排序（未知会话按 id 排在最后）；从
    queue_key 游标（最近一次成功会话）之后循环扫描，向首个可用会话发放其最低序号
    可领取分片。资格判断复用批量领取的全部规则（依赖、退避、有效租约、
    max_active_leases）；扫描、发放与游标推进在同一 DB_LOCK 临界区内完成，并发请求
    不会重复发放。无可发放项时不推进游标，返回各会话不可用原因与最早
    next_retry_at；成功后才更新游标，使连续请求在可用会话间轮转。"""
    ttl = DEFAULT_LEASE_TTL_SECONDS
    if req.lease_ttl_seconds is not None:
        ttl = min(req.lease_ttl_seconds, MAX_LEASE_TTL_SECONDS)

    now = utcnow()
    with DB_LOCK:
        conn = get_conn()
        # 去重后按（创建时间, id）稳定排序；未知会话没有创建时间，按 id 排在最后
        unique_ids = list(dict.fromkeys(req.session_ids))
        rows = conn.execute(
            f"SELECT * FROM sessions WHERE id IN ({','.join('?' * len(unique_ids))})",
            unique_ids,
        ).fetchall()
        by_id = {row["id"]: row for row in rows}
        known = sorted(by_id.values(), key=lambda r: (parse_ts(r["created_at"]), r["id"]))
        ordered = [r["id"] for r in known] + sorted(
            sid for sid in unique_ids if sid not in by_id
        )

        # 逐会话资格评估：复用批量领取的 claimable_chunks / count_active_leases 规则
        grantable: dict[str, int] = {}  # session_id -> 最低可领取分片序号
        reasons: dict[str, tuple[str, Optional[datetime]]] = {}
        for sid in ordered:
            session = by_id.get(sid)
            if session is None:
                reasons[sid] = ("not_found", None)
                continue
            if session["status"] == "finalized":
                reasons[sid] = ("finalized", None)
                continue
            ready, _ = claimable_chunks(sid, now)
            max_active = session["max_active_leases"]
            if ready and (max_active is None
                          or count_active_leases(sid, now) < max_active):
                grantable[sid] = ready[0]
            else:
                reasons[sid] = dispatch_unavailability(session, now)

        # 从游标（最近一次成功会话）之后循环扫描；无游标或游标不在候选中时从头开始
        cursor = conn.execute(
            "SELECT last_session_id FROM dispatch_cursors WHERE queue_key = ?",
            (req.queue_key,),
        ).fetchone()
        start = 0
        if cursor is not None and cursor["last_session_id"] in ordered:
            start = (ordered.index(cursor["last_session_id"]) + 1) % len(ordered)
        granted_sid = None
        for i in range(len(ordered)):
            sid = ordered[(start + i) % len(ordered)]
            if sid in grantable:
                granted_sid = sid
                break

        if granted_sid is not None:
            chunk_index = grantable[granted_sid]
            token = secrets.token_urlsafe(32)
            expires_at = now + timedelta(seconds=ttl)
            conn.execute(
                "UPDATE chunks SET status = 'leased', lease_token = ?, lease_expires_at = ?"
                " WHERE session_id = ? AND chunk_index = ?",
                (token, expires_at.isoformat(), granted_sid, chunk_index),
            )
            # 成功后才推进游标：连续请求在可用会话间轮转
            conn.execute(
                "INSERT INTO dispatch_cursors (queue_key, last_session_id, updated_at)"
                " VALUES (?, ?, ?)"
                " ON CONFLICT(queue_key) DO UPDATE SET"
                " last_session_id = excluded.last_session_id,"
                " updated_at = excluded.updated_at",
                (req.queue_key, granted_sid, now.isoformat()),
            )
            conn.commit()
            return DispatchLeaseResponse(
                queue_key=req.queue_key,
                granted=True,
                session_id=granted_sid,
                chunk_index=chunk_index,
                lease_token=token,
                expires_at=expires_at,
            )

        # 无可发放项：不推进游标，按稳定排序返回各会话原因与最早可重试时刻
        unavailable = [
            DispatchUnavailableSession(
                session_id=sid,
                reason=reasons[sid][0],
                next_retry_at=reasons[sid][1],
            )
            for sid in ordered
        ]
        retries = [u.next_retry_at for u in unavailable if u.next_retry_at is not None]

    return DispatchLeaseResponse(
        queue_key=req.queue_key,
        granted=False,
        unavailable=unavailable,
        next_retry_at=min(retries) if retries else None,
    )


# ---------------------------------------------------------------- 完成上报

@app.post(
    "/sessions/{session_id}/chunks/{chunk_index}/complete",
    response_model=CompleteResponse,
    tags=["chunks"],
    summary="上报分片完成（幂等）",
)
def complete_chunk(session_id: str, chunk_index: int, req: CompleteRequest):
    now = utcnow()
    with DB_LOCK:
        session = get_session_or_404(session_id)
        chunk = get_chunk_or_404(session_id, chunk_index)

        if session["digest_algorithm"] == MERKLE_DIGEST_ALGORITHM and not is_valid_merkle_digest(
            req.chunk_digest
        ):
            raise HTTPException(
                status_code=422,
                detail="chunk_digest must be 64 lowercase hex characters "
                       "for sha256-merkle-v1 sessions",
            )

        if chunk["status"] == "completed":
            # 幂等重放：同一租约令牌 + 同一摘要 => 返回首次完成结果
            if chunk["lease_token"] == req.lease_token and chunk["chunk_digest"] == req.chunk_digest:
                return CompleteResponse(
                    session_id=session_id,
                    chunk_index=chunk_index,
                    status="completed",
                    chunk_digest=chunk["chunk_digest"],
                    completed_at=parse_ts(chunk["completed_at"]),
                    idempotent_replay=True,
                )
            raise HTTPException(
                status_code=409,
                detail="chunk already completed with different lease token or digest",
            )

        if session["status"] == "finalized":
            raise HTTPException(status_code=409, detail="session already finalized")
        if chunk["lease_token"] is None or chunk["lease_token"] != req.lease_token:
            raise HTTPException(status_code=409, detail="lease token mismatch")
        if not lease_valid(chunk, now):
            raise HTTPException(status_code=409, detail="lease expired")

        get_conn().execute(
            "UPDATE chunks SET status = 'completed', chunk_digest = ?, completed_at = ?,"
            " backoff_until = NULL"
            " WHERE session_id = ? AND chunk_index = ?",
            (req.chunk_digest, now.isoformat(), session_id, chunk_index),
        )
        get_conn().commit()

    return CompleteResponse(
        session_id=session_id,
        chunk_index=chunk_index,
        status="completed",
        chunk_digest=req.chunk_digest,
        completed_at=now,
        idempotent_replay=False,
    )


# ---------------------------------------------------------------- 失败上报

@app.post(
    "/sessions/{session_id}/chunks/{chunk_index}/fail",
    response_model=FailResponse,
    tags=["chunks"],
    summary="上报分片失败（校验当前有效租约，清除租约并进入指数退避）",
    responses={409: {"description": "分片已完成、会话已提交、租约令牌不匹配或租约已过期"}},
)
def fail_chunk(session_id: str, chunk_index: int, req: FailRequest):
    """成功后在同一事务内原子地：校验令牌 → 清除租约（状态回到 pending、清空令牌与过期时间）
    → failure_count + 1 → 计算 backoff_until = now + min(max, base×2^(n-1))。"""
    now = utcnow()
    with DB_LOCK:
        session = get_session_or_404(session_id)
        chunk = get_chunk_or_404(session_id, chunk_index)

        if chunk["status"] == "completed":
            raise HTTPException(status_code=409, detail="chunk already completed")
        if session["status"] == "finalized":
            raise HTTPException(status_code=409, detail="session already finalized")
        if chunk["lease_token"] is None or chunk["lease_token"] != req.lease_token:
            raise HTTPException(status_code=409, detail="lease token mismatch")
        if not lease_valid(chunk, now):
            raise HTTPException(status_code=409, detail="lease expired")

        failure_count = (chunk["failure_count"] or 0) + 1
        delay = backoff_delay(session, failure_count)
        retry_after = now + timedelta(seconds=delay)
        # 原子清除租约 + 递增失败次数 + 设置冷却截止；failure_count 永不因本接口清零
        get_conn().execute(
            "UPDATE chunks SET status = 'pending', lease_token = NULL,"
            " lease_expires_at = NULL, failure_count = ?, backoff_until = ?"
            " WHERE session_id = ? AND chunk_index = ?",
            (failure_count, retry_after.isoformat(), session_id, chunk_index),
        )
        get_conn().commit()

    return FailResponse(
        session_id=session_id,
        chunk_index=chunk_index,
        status="pending",
        failure_count=failure_count,
        retry_after_seconds=delay,
        retry_after=retry_after,
    )


# ---------------------------------------------------------------- 完成纠错回退

@app.post(
    "/sessions/{session_id}/chunks/{chunk_index}/reopen",
    response_model=ReopenResponse,
    tags=["chunks"],
    summary="纠错回退：将已完成分片及其全部传递下游原子重置为 pending",
    responses={409: {"description": "会话已提交、分片未完成或完成凭据（令牌/摘要）不匹配"}},
)
def reopen_chunk(session_id: str, chunk_index: int, req: ReopenRequest):
    """凭目标分片完成时保留的租约令牌与摘要发起回退：全部校验先于任何写入，
    任一校验失败不产生部分回退；校验通过后在同一事务内将目标分片与依赖图中的
    全部传递下游重置为 pending（清除摘要、完成时间、有效租约与退避截止时间，
    保留累计失败次数），并将会话 revision 递增 1。资格计算与写入均在 DB_LOCK
    临界区内，并发领取不会看到中间状态。"""
    with DB_LOCK:
        session = get_session_or_404(session_id)
        chunk = get_chunk_or_404(session_id, chunk_index)

        if session["status"] == "finalized":
            raise HTTPException(status_code=409, detail="session already finalized")
        if chunk["status"] != "completed":
            raise HTTPException(status_code=409, detail="chunk is not completed")
        if chunk["lease_token"] is None or chunk["lease_token"] != req.lease_token:
            raise HTTPException(status_code=409, detail="lease token mismatch")
        if chunk["chunk_digest"] != req.expected_digest:
            raise HTTPException(status_code=409, detail="digest mismatch")

        affected = collect_reopen_affected(session_id, chunk_index)
        conn = get_conn()
        # failure_count 不在重置之列：累计失败次数跨回退保留
        conn.executemany(
            "UPDATE chunks SET status = 'pending', lease_token = NULL,"
            " lease_expires_at = NULL, chunk_digest = NULL, completed_at = NULL,"
            " backoff_until = NULL"
            " WHERE session_id = ? AND chunk_index = ?",
            [(session_id, idx) for idx, _ in affected],
        )
        conn.execute(
            "UPDATE sessions SET revision = revision + 1 WHERE id = ?",
            (session_id,),
        )
        conn.commit()

    return ReopenResponse(
        session_id=session_id,
        chunk_index=chunk_index,
        revision=session["revision"] + 1,
        affected_chunks=[
            ReopenAffectedChunk(chunk_index=idx, level=lvl) for idx, lvl in affected
        ],
    )


# ---------------------------------------------------------------- 提交

@app.post(
    "/sessions/{session_id}/finalize",
    response_model=FinalizeResponse,
    tags=["sessions"],
    summary="提交会话（全部分片完成后；sha256-merkle-v1 会话先校验 Merkle 根）",
    responses={409: {"description": "存在缺失分片（missing_chunks），"
                                    "或 Merkle 根与 file_digest 不匹配（computed_digest）"}},
)
def finalize_session(session_id: str):
    with DB_LOCK:
        session = get_session_or_404(session_id)
        rows = get_conn().execute(
            "SELECT * FROM chunks WHERE session_id = ? ORDER BY chunk_index",
            (session_id,),
        ).fetchall()

        missing = [r["chunk_index"] for r in rows if r["status"] != "completed"]
        if missing:
            return conflict("upload incomplete", missing_chunks=missing)

        if session["digest_algorithm"] == MERKLE_DIGEST_ALGORITHM:
            # 按分片序号升序读取摘要构建 Merkle 树，与 file_digest 常量时间比较；
            # 不匹配时不做任何写入，会话保持 active，可纠错后重试
            computed = merkle_root_sha256([r["chunk_digest"] for r in rows])
            if not hmac.compare_digest(computed, session["file_digest"]):
                return conflict("file digest mismatch", computed_digest=computed)

        now = utcnow()
        finalized_at = parse_ts(session["finalized_at"]) if session["finalized_at"] else None
        if session["status"] != "finalized":
            finalized_at = now
            get_conn().execute(
                "UPDATE sessions SET status = 'finalized', finalized_at = ? WHERE id = ?",
                (finalized_at.isoformat(), session_id),
            )
            get_conn().commit()
        # 已 finalized 时重复提交：幂等返回同一份清单

    return FinalizeResponse(
        session_id=session_id,
        status="finalized",
        file_digest=session["file_digest"],
        digest_algorithm=session["digest_algorithm"],
        total_chunks=session["total_chunks"],
        manifest=[
            ManifestEntry(
                chunk_index=r["chunk_index"],
                chunk_digest=r["chunk_digest"],
                completed_at=parse_ts(r["completed_at"]),
            )
            for r in rows
        ],
        finalized_at=finalized_at,
    )


# ---------------------------------------------------------------- 其他

@app.get("/healthz", tags=["meta"], summary="健康检查")
def healthz():
    get_conn().execute("SELECT 1")
    return {"status": "ok"}


@app.get("/", tags=["meta"], summary="服务信息")
def root():
    return {
        "service": "chunked-upload-coordinator",
        "docs": "/docs",
        "openapi": "/openapi.json",
        "default_lease_ttl_seconds": DEFAULT_LEASE_TTL_SECONDS,
        "max_lease_ttl_seconds": MAX_LEASE_TTL_SECONDS,
        "default_base_retry_seconds": DEFAULT_BASE_RETRY_SECONDS,
        "default_max_retry_seconds": DEFAULT_MAX_RETRY_SECONDS,
    }
