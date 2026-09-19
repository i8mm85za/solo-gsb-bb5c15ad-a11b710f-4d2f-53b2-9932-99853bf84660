"""SQLite 存储层：单连接 + 进程内写锁，启动时自动建库建表。"""
from __future__ import annotations

import os
import sqlite3
import threading
from pathlib import Path

DB_PATH = os.environ.get("UPLOAD_DB_PATH", "./data/coordinator.db")

# 进程内写锁：SQLite 单写者，串行化所有状态变更，保证租约/完成/提交的原子性。
DB_LOCK = threading.RLock()

_conn: sqlite3.Connection | None = None

SCHEMA = """
CREATE TABLE IF NOT EXISTS sessions (
    id                  TEXT PRIMARY KEY,
    total_chunks        INTEGER NOT NULL,
    file_digest         TEXT NOT NULL,
    digest_algorithm    TEXT NOT NULL DEFAULT 'sha256',
    status              TEXT NOT NULL DEFAULT 'active',   -- active | finalized
    max_active_leases   INTEGER,                          -- 会话级并发租约上限；NULL 表示不限
    base_retry_seconds  INTEGER NOT NULL DEFAULT 5,       -- 失败退避基数（第 1 次失败冷却秒数）
    max_retry_seconds   INTEGER NOT NULL DEFAULT 300,     -- 失败退避冷却上限（秒）
    revision            INTEGER NOT NULL DEFAULT 0,       -- 修订号：每次成功 reopen 回退递增 1
    source_session_id   TEXT,                             -- fork 来源会话；NULL 表示原生会话
    created_at          TEXT NOT NULL,
    finalized_at        TEXT
);

-- 进度分支幂等记录：(源会话, request_id) 唯一，重放返回首次创建的新会话
CREATE TABLE IF NOT EXISTS fork_requests (
    source_session_id  TEXT NOT NULL,
    request_id         TEXT NOT NULL,
    new_session_id     TEXT NOT NULL,
    expected_revision  INTEGER NOT NULL,                  -- 首次请求携带的修订号（重放校验参数变化）
    reuse_chunks       TEXT NOT NULL,                     -- 升序去重后复用分片序号的 JSON 列表
    created_at         TEXT NOT NULL,
    PRIMARY KEY (source_session_id, request_id)
);

CREATE TABLE IF NOT EXISTS chunks (
    session_id        TEXT NOT NULL REFERENCES sessions(id),
    chunk_index       INTEGER NOT NULL,
    status            TEXT NOT NULL DEFAULT 'pending',  -- pending | leased | completed
    lease_token       TEXT,
    lease_expires_at  TEXT,
    chunk_digest      TEXT,
    completed_at      TEXT,
    failure_count     INTEGER NOT NULL DEFAULT 0,       -- 累计失败次数（完成上报不清零）
    backoff_until     TEXT,                             -- 退避冷却结束时间；NULL 表示未在冷却
    PRIMARY KEY (session_id, chunk_index)
);

CREATE INDEX IF NOT EXISTS idx_chunks_session ON chunks(session_id);

-- 分片依赖图：chunk_index 领取租约前，depends_on 必须已完成
CREATE TABLE IF NOT EXISTS chunk_dependencies (
    session_id        TEXT NOT NULL,
    chunk_index       INTEGER NOT NULL,
    depends_on        INTEGER NOT NULL,
    PRIMARY KEY (session_id, chunk_index, depends_on),
    FOREIGN KEY (session_id, chunk_index) REFERENCES chunks(session_id, chunk_index)
);

CREATE INDEX IF NOT EXISTS idx_chunk_deps_session ON chunk_dependencies(session_id);

-- 跨会话公平调度游标：按 queue_key 持久化最近一次成功发放的会话 id，
-- 下次领取从该会话之后循环扫描；仅在成功发放时推进
CREATE TABLE IF NOT EXISTS dispatch_cursors (
    queue_key        TEXT PRIMARY KEY,
    last_session_id  TEXT NOT NULL,
    updated_at       TEXT NOT NULL
);
"""

# 增量迁移：针对早于当前 SCHEMA 创建的旧库，幂等执行（CREATE TABLE IF NOT EXISTS
# 不会为已存在的表补列，故需显式 ALTER）。条目为 (kind, target, ddl)：
# - ("column", "sessions.col", ALTER ...)：列不存在时补列
# - ("table", "table_name", CREATE TABLE ...)：表不存在时建表（新表无法用 ALTER 补齐）
MIGRATIONS = [
    # 0001: 会话级并发租约上限；旧库已存在的会话取 NULL，即保持“不限并发”的旧行为
    (
        "column",
        "sessions.max_active_leases",
        "ALTER TABLE sessions ADD COLUMN max_active_leases INTEGER",
    ),
    # 0002: 失败退避参数；旧库既有会话取服务端默认值 5/300，保持可继续退避
    (
        "column",
        "sessions.base_retry_seconds",
        "ALTER TABLE sessions ADD COLUMN base_retry_seconds INTEGER NOT NULL DEFAULT 5",
    ),
    (
        "column",
        "sessions.max_retry_seconds",
        "ALTER TABLE sessions ADD COLUMN max_retry_seconds INTEGER NOT NULL DEFAULT 300",
    ),
    # 0003: 分片级失败计数与冷却截止时间；既有分片从 0/NULL 开始，无冷却
    (
        "column",
        "chunks.failure_count",
        "ALTER TABLE chunks ADD COLUMN failure_count INTEGER NOT NULL DEFAULT 0",
    ),
    (
        "column",
        "chunks.backoff_until",
        "ALTER TABLE chunks ADD COLUMN backoff_until TEXT",
    ),
    # 0004: 会话修订号；每次成功 reopen 回退递增 1，既有会话从 0 开始
    (
        "column",
        "sessions.revision",
        "ALTER TABLE sessions ADD COLUMN revision INTEGER NOT NULL DEFAULT 0",
    ),
    # 0005: 进度分支来源会话；fork 产生的新会话记录源会话 id，既有原生会话为 NULL
    (
        "column",
        "sessions.source_session_id",
        "ALTER TABLE sessions ADD COLUMN source_session_id TEXT",
    ),
    # 0006: fork 幂等记录表；旧库直接按当前定义新建
    (
        "table",
        "fork_requests",
        """
CREATE TABLE fork_requests (
    source_session_id  TEXT NOT NULL,
    request_id         TEXT NOT NULL,
    new_session_id     TEXT NOT NULL,
    expected_revision  INTEGER NOT NULL,
    reuse_chunks       TEXT NOT NULL,
    created_at         TEXT NOT NULL,
    PRIMARY KEY (source_session_id, request_id)
)
        """,
    ),
    # 0007: 跨会话公平调度游标表；旧库直接按当前定义新建
    (
        "table",
        "dispatch_cursors",
        """
CREATE TABLE dispatch_cursors (
    queue_key        TEXT PRIMARY KEY,
    last_session_id  TEXT NOT NULL,
    updated_at       TEXT NOT NULL
)
        """,
    ),
]


def _existing_tables(conn: sqlite3.Connection) -> set[str]:
    return {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    # PRAGMA table_info: (cid, name, type, notnull, dflt_value, pk)；
    # 使用位置下标以兼容未设置 row_factory 的连接。
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def run_migrations(conn: sqlite3.Connection) -> None:
    with DB_LOCK:
        tables = _existing_tables(conn)
        for kind, target, ddl in MIGRATIONS:
            if kind == "table":
                if target not in tables:
                    conn.execute(ddl)
                continue
            table, _, column_name = target.partition(".")
            if column_name not in _table_columns(conn, table):
                conn.execute(ddl)
        conn.commit()


def get_conn() -> sqlite3.Connection:
    """返回进程级共享连接（惰性创建，首次调用时建库建表）。"""
    global _conn
    if _conn is None:
        Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
        _conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        _conn.row_factory = sqlite3.Row
        _conn.execute("PRAGMA journal_mode=WAL")
        _conn.execute("PRAGMA foreign_keys=ON")
        _conn.execute("PRAGMA busy_timeout=5000")
        with DB_LOCK:
            _conn.executescript(SCHEMA)
            _conn.commit()
        run_migrations(_conn)  # 补齐旧库缺失的列
    return _conn


def init_db() -> None:
    """启动时显式初始化数据库。"""
    get_conn()
