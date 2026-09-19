"""Pydantic 请求/响应模型。"""
from __future__ import annotations

import re
from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field, model_validator

# 可选完整性校验算法：创建会话时 digest_algorithm 取该值即启用。
# 启用后 file_digest 与每次 complete 的 chunk_digest 必须是 64 位小写十六进制
# （SHA-256 的 32 字节十六进制编码），finalize 按分片序号构建 Merkle 树并与
# file_digest 常量时间比较，不匹配则拒绝提交。
MERKLE_DIGEST_ALGORITHM = "sha256-merkle-v1"
_MERKLE_DIGEST_RE = re.compile(r"[0-9a-f]{64}")


def is_valid_merkle_digest(value: str) -> bool:
    """sha256-merkle-v1 的摘要格式：恰好 64 位小写十六进制字符。"""
    return _MERKLE_DIGEST_RE.fullmatch(value) is not None


def validate_dependencies(
    dependencies: dict[int, list[int]], total_chunks: int
) -> dict[int, list[int]]:
    """校验并规范化依赖图：拒绝越界、自依赖与有环关系；返回去重、升序后的图。"""
    normalized: dict[int, list[int]] = {}
    for chunk_index, deps in dependencies.items():
        if not 0 <= chunk_index < total_chunks:
            raise ValueError(
                f"dependency key {chunk_index} out of range [0, {total_chunks})"
            )
        unique = sorted(set(deps))
        for dep in unique:
            if not 0 <= dep < total_chunks:
                raise ValueError(
                    f"dependency {dep} of chunk {chunk_index} out of range [0, {total_chunks})"
                )
            if dep == chunk_index:
                raise ValueError(f"chunk {chunk_index} cannot depend on itself")
        if unique:
            normalized[chunk_index] = unique

    # 环检测（Kahn 拓扑）：仅键集合内部的子图可能成环，非键节点均为汇点
    indegree = {node: 0 for node in normalized}
    for deps in normalized.values():
        for dep in deps:
            if dep in indegree:
                indegree[dep] += 1
    queue = [node for node, deg in indegree.items() if deg == 0]
    visited = 0
    while queue:
        node = queue.pop()
        visited += 1
        for dep in normalized[node]:
            if dep in indegree:
                indegree[dep] -= 1
                if indegree[dep] == 0:
                    queue.append(dep)
    if visited != len(normalized):
        raise ValueError("dependencies contain a cycle")
    return normalized


class CreateSessionRequest(BaseModel):
    total_chunks: int = Field(ge=1, le=100_000, description="总分片数")
    file_digest: str = Field(
        min_length=1, max_length=256,
        description="期望的完整文件摘要；digest_algorithm 为 sha256-merkle-v1 时必须是"
                    " 64 位小写十六进制（各分片摘要构建的 Merkle 根）")
    digest_algorithm: str = Field(
        default="sha256", min_length=1, max_length=32,
        description="摘要算法标识；取 sha256-merkle-v1 时启用 Merkle 完整性校验，"
                    "其余取值仅作记录、保持原行为")
    dependencies: Optional[dict[int, list[int]]] = Field(
        default=None,
        description="分片依赖图：键为分片序号，值为该分片领取租约前必须完成的前置分片序号列表；"
                    "缺省表示无依赖（保持原有调度行为）")
    max_active_leases: Optional[int] = Field(
        default=None, ge=1, le=100_000,
        description="会话级并发租约上限：当前有效（未过期）租约数达到该值后，"
                    "单分片领取返回 409、批量领取只返回空列表；缺省/NULL 表示不限并发")
    base_retry_seconds: int = Field(
        default=5, ge=1, le=86_400,
        description="失败退避基数（秒）：第 1 次失败冷却 base，第 n 次为 base×2^(n-1)，"
                    "受 max_retry_seconds 封顶；必须为正数")
    max_retry_seconds: int = Field(
        default=300, ge=1, le=86_400,
        description="失败退避冷却上限（秒）：实际冷却取 min(max, base×2^(n-1))；"
                    "必须为正数且不小于 base_retry_seconds")

    @model_validator(mode="after")
    def _validate_dependencies(self):
        if self.dependencies:
            self.dependencies = validate_dependencies(self.dependencies, self.total_chunks)
        return self

    @model_validator(mode="after")
    def _validate_retry_range(self):
        if self.max_retry_seconds < self.base_retry_seconds:
            raise ValueError(
                "max_retry_seconds must be greater than or equal to base_retry_seconds"
            )
        return self

    @model_validator(mode="after")
    def _validate_merkle_file_digest(self):
        if self.digest_algorithm == MERKLE_DIGEST_ALGORITHM and not is_valid_merkle_digest(
            self.file_digest
        ):
            raise ValueError(
                "file_digest must be 64 lowercase hex characters "
                "when digest_algorithm is sha256-merkle-v1"
            )
        return self


class SessionResponse(BaseModel):
    id: str
    total_chunks: int
    file_digest: str
    digest_algorithm: str
    status: str
    source_session_id: Optional[str] = Field(
        default=None, description="fork 来源会话 id；原生会话为 null")
    max_active_leases: Optional[int] = Field(
        default=None, description="会话级并发租约上限；null 表示不限并发")
    base_retry_seconds: int = Field(description="失败退避基数（秒）")
    max_retry_seconds: int = Field(description="失败退避冷却上限（秒）")
    created_at: datetime
    finalized_at: Optional[datetime] = None


class ForkSessionRequest(BaseModel):
    expected_revision: int = Field(
        ge=0,
        description="调用方认为源会话当前所处的修订号；与源会话 revision 不一致时返回 409")
    request_id: str = Field(
        min_length=1, max_length=128,
        description="幂等键：同一源会话 + 同一 request_id 的重放返回首次创建的新会话；"
                    "相同键但 expected_revision 或 reuse_chunks 变化返回 409")
    reuse_chunks: list[int] = Field(
        description="新会话中直接复用为已完成的源会话分片序号列表；每项必须在源会话中已完成，"
                    "且集合必须包含这些分片的全部传递前置分片，否则返回 409 且不落库；"
                    "允许空列表（全部重新上传）。其余分片在新会话中初始化为 pending")

    @model_validator(mode="after")
    def _validate_reuse_chunks(self):
        if any(idx < 0 for idx in self.reuse_chunks):
            raise ValueError("reuse_chunks entries must be non-negative chunk indexes")
        # 服务端去重排序：重复序号等价于只列一次（越界序号留给事务内按未完成返回 409）
        self.reuse_chunks = sorted(set(self.reuse_chunks))
        return self


class LeaseRequest(BaseModel):
    lease_ttl_seconds: Optional[int] = Field(
        default=None, ge=1,
        description="租约有效期（秒），缺省取服务端默认值，超过服务端上限会被截断")


class LeaseResponse(BaseModel):
    session_id: str
    chunk_index: int
    lease_token: str
    expires_at: datetime


class BatchLeaseRequest(BaseModel):
    count: int = Field(
        ge=1, le=100_000,
        description="期望领取的分片数量上限；实际领取数还受剩余并发槽位与可领取分片数限制")
    lease_ttl_seconds: Optional[int] = Field(
        default=None, ge=1,
        description="本批租约统一有效期（秒），缺省取服务端默认值，超过服务端上限会被截断")


class BatchLeaseItem(BaseModel):
    chunk_index: int
    lease_token: str
    expires_at: datetime


class BatchLeaseResponse(BaseModel):
    session_id: str
    requested: int = Field(description="请求的 count")
    granted: list[BatchLeaseItem] = Field(
        description="本次实际领取的租约，按分片序号升序；无可领取项或无剩余槽位时为空列表")
    active: int = Field(description="本次领取后，会话内当前有效（未过期）租约总数")
    blocked: int = Field(
        description="未完成、且前置依赖尚未全部完成的分片数量（不含已完成、不看租约状态）")
    next_expiry: Optional[datetime] = Field(
        default=None,
        description="本次领取后会话内最早的可重试时刻：取当前有效租约的最早过期时间与"
                    "各分片退避冷却的最早结束时间中更早者（无可等待事件时为 null）")
    max_active_leases: Optional[int] = Field(
        default=None, description="会话级并发租约上限；null 表示不限并发")
    remaining_slots: Optional[int] = Field(
        default=None, description="当前剩余并发槽位；不限并发时为 null")


class CompleteRequest(BaseModel):
    lease_token: str = Field(min_length=1, max_length=128, description="当前有效租约令牌")
    chunk_digest: str = Field(
        min_length=1, max_length=256,
        description="该分片的摘要；会话使用 sha256-merkle-v1 时必须是 64 位小写十六进制"
                    "（分片原始内容的 SHA-256）")


class CompleteResponse(BaseModel):
    session_id: str
    chunk_index: int
    status: str
    chunk_digest: str
    completed_at: datetime
    idempotent_replay: bool = Field(default=False, description="是否为幂等重放")


class FailRequest(BaseModel):
    lease_token: str = Field(
        min_length=1, max_length=128,
        description="当前有效租约令牌：须与该分片持有的有效租约匹配")


class FailResponse(BaseModel):
    session_id: str
    chunk_index: int
    status: str = Field(description="上报后的分片状态：pending（租约已清除，等待冷却结束）")
    failure_count: int = Field(description="该分片累计失败次数（完成上报后仍保留）")
    retry_after_seconds: int = Field(
        description="本次失败的冷却时长（秒）：min(max_retry_seconds, base×2^(n-1))")
    retry_after: datetime = Field(description="冷却结束时间；在此之前该分片不可被领取")


class ReopenRequest(BaseModel):
    lease_token: str = Field(
        min_length=1, max_length=128,
        description="目标分片完成时所持有租约的令牌（完成上报后仍保留在分片记录中）")
    expected_digest: str = Field(
        min_length=1, max_length=256,
        description="目标分片完成时上报的摘要；与记录不一致时拒绝回退")


class ReopenAffectedChunk(BaseModel):
    chunk_index: int
    level: int = Field(
        description="拓扑层级：被重开的目标分片为 0，其直接下游为 1，依此类推"
                    "（取受影响子图中的最长路径层级）")


class ReopenResponse(BaseModel):
    session_id: str
    chunk_index: int = Field(description="被纠错重开的目标分片")
    revision: int = Field(description="本次回退后的会话修订号：每次成功 reopen 递增 1")
    affected_chunks: list[ReopenAffectedChunk] = Field(
        description="在同一事务内被重置为 pending 的全部分片：目标分片 + 依赖图中的全部传递下游，"
                    "按（拓扑层级, 分片序号）升序稳定排列")


class ForkSessionResponse(BaseModel):
    id: str = Field(description="新会话 id（与源会话相互独立）")
    source_session_id: str = Field(description="源会话 id")
    request_id: str = Field(description="本次分支使用的幂等键")
    total_chunks: int
    file_digest: str
    digest_algorithm: str
    status: str = Field(description="新会话状态：始终为 active")
    revision: int = Field(description="新会话修订号：从 0 开始")
    reused_chunks: list[int] = Field(
        description="复制为 completed 的分片序号（升序去重，即请求中的 reuse_chunks）")
    pending_chunks: list[int] = Field(
        description="初始化为 pending、需要重新上传的分片序号（升序）")
    max_active_leases: Optional[int] = Field(
        default=None, description="从源会话复制的并发租约上限；null 表示不限并发")
    base_retry_seconds: int = Field(description="从源会话复制的失败退避基数（秒）")
    max_retry_seconds: int = Field(description="从源会话复制的失败退避冷却上限（秒）")
    created_at: datetime
    idempotent_replay: bool = Field(
        default=False, description="是否为同一 request_id 的幂等重放（重放返回首次创建的会话）")


class ManifestEntry(BaseModel):
    chunk_index: int
    chunk_digest: str
    completed_at: datetime


class FinalizeResponse(BaseModel):
    session_id: str
    status: str
    file_digest: str
    digest_algorithm: str
    total_chunks: int
    manifest: list[ManifestEntry]
    finalized_at: datetime


class ChunkStatus(BaseModel):
    chunk_index: int
    status: str  # pending | leased | completed（过期租约/冷却中的分片按 pending 展示）
    lease_expires_at: Optional[datetime] = None
    chunk_digest: Optional[str] = None
    completed_at: Optional[datetime] = None
    dependencies: list[int] = Field(default_factory=list,
                                    description="领取租约前必须完成的前置分片（升序）")
    failure_count: int = Field(default=0, description="累计失败次数（完成上报后仍保留）")
    retry_after: Optional[datetime] = Field(
        default=None,
        description="退避冷却结束时间；仅当该分片正处于冷却中时非 null，冷却结束或完成上报后为 null")


class BlockedChunk(BaseModel):
    chunk_index: int
    missing_dependencies: list[int] = Field(description="尚未完成的前置分片（升序）")


class CoolingChunk(BaseModel):
    chunk_index: int
    failure_count: int = Field(description="该分片累计失败次数")
    retry_after: datetime = Field(description="退避冷却结束时间，此前不可领取")


class ReadyResponse(BaseModel):
    session_id: str
    ready_chunks: list[int] = Field(
        description="当前可领取的分片：未完成、无有效租约（过期租约视为可领取）、"
                    "不在失败退避冷却中且前置已全部完成")
    blocked_chunks: list[BlockedChunk] = Field(
        description="被依赖阻塞的分片及各自未完成的前置项")
    cooling_chunks: list[CoolingChunk] = Field(
        default_factory=list,
        description="因失败退避而暂时不可领取的分片（按序号升序）；冷却结束后自动恢复可领取")


class SessionStatusResponse(BaseModel):
    id: str
    status: str
    total_chunks: int
    completed_chunks: int
    missing_chunks: list[int]
    file_digest: str
    digest_algorithm: str
    source_session_id: Optional[str] = Field(
        default=None, description="fork 来源会话 id；原生会话为 null")
    revision: int = Field(
        default=0, description="会话修订号：每次成功 reopen 回退递增 1，创建时为 0")
    max_active_leases: Optional[int] = Field(
        default=None, description="会话级并发租约上限；null 表示不限并发")
    base_retry_seconds: int = Field(default=5, description="失败退避基数（秒）")
    max_retry_seconds: int = Field(default=300, description="失败退避冷却上限（秒）")
    active_leases: int = Field(
        default=0, description="当前有效（未过期）租约数，过期租约不计入")
    created_at: datetime
    finalized_at: Optional[datetime] = None
    chunks: list[ChunkStatus]


class DispatchLeaseRequest(BaseModel):
    queue_key: str = Field(
        min_length=1, max_length=128,
        description="调度队列键：同一 queue_key 共享一个轮转游标（最近一次成功发放的会话）")
    session_ids: list[str] = Field(
        min_length=1, max_length=1000,
        description="候选会话 id 列表；服务端去重后按（创建时间, id）稳定排序，"
                    "未知会话按 id 排在最后并以 not_found 归因")
    lease_ttl_seconds: Optional[int] = Field(
        default=None, ge=1,
        description="租约有效期（秒），缺省取服务端默认值，超过服务端上限会被截断")


class DispatchUnavailableSession(BaseModel):
    session_id: str
    reason: str = Field(
        description="不可用原因：not_found（会话不存在）| finalized（会话已提交）| "
                    "completed（分片全部完成、待提交）| max_active_leases_reached（并发槽位占满）| "
                    "leases_active（存在有效租约）| in_backoff（失败退避冷却中）| "
                    "blocked_by_dependencies（被依赖阻塞）")
    next_retry_at: Optional[datetime] = Field(
        default=None,
        description="该会话最早可能恢复可领取的时刻：有效租约最早过期时间与退避最早结束"
                    "时间中更早者；无时间性等待事件时为 null")


class DispatchLeaseResponse(BaseModel):
    queue_key: str
    granted: bool = Field(description="本次是否成功发放租约")
    session_id: Optional[str] = Field(
        default=None, description="granted=true 时：被发放的会话 id")
    chunk_index: Optional[int] = Field(
        default=None, description="granted=true 时：该会话当前可领取的最低序号分片")
    lease_token: Optional[str] = Field(
        default=None, description="granted=true 时：租约令牌")
    expires_at: Optional[datetime] = Field(
        default=None, description="granted=true 时：租约过期时间")
    unavailable: list[DispatchUnavailableSession] = Field(
        default_factory=list,
        description="granted=false 时：各候选会话（按稳定排序）的不可用原因")
    next_retry_at: Optional[datetime] = Field(
        default=None,
        description="granted=false 时：各会话 next_retry_at 中的最早者；"
                    "均无可等待事件时为 null")
