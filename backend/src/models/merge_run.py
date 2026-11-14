"""合并批次：一次按水位线乱序合并的执行记录（算法 A 的运行载体）。"""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import JSON, DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.orm import Mapped, mapped_column

from src.models import Base
from src.utils.timeutil import utcnow

# MutableDict：让 JSON 字典的原地/引用变更在 MySQL 与 SQLite 上都能被标记为脏并持久化。
JSON_DICT = MutableDict.as_mutable(JSON)


class MergeRun(Base):
    __tablename__ = "merge_runs"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    run_no: Mapped[str] = mapped_column(String(40), unique=True, index=True)
    table_id: Mapped[int | None] = mapped_column(
        ForeignKey("source_tables.id", ondelete="SET NULL"), nullable=True, index=True
    )
    trigger: Mapped[str] = mapped_column(String(20), default="manual")
    status: Mapped[str] = mapped_column(String(20), default="queued", index=True)
    operator_email: Mapped[str] = mapped_column(String(160), default="system")
    allowed_lateness_seconds: Mapped[int] = mapped_column(Integer, default=30)
    events_in: Mapped[int] = mapped_column(Integer, default=0)
    upsert_rows: Mapped[int] = mapped_column(Integer, default=0)
    deleted_rows: Mapped[int] = mapped_column(Integer, default=0)
    late_count: Mapped[int] = mapped_column(Integer, default=0)
    dup_skipped: Mapped[int] = mapped_column(Integer, default=0)
    rejected_count: Mapped[int] = mapped_column(Integer, default=0)
    watermark_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    metrics: Mapped[dict] = mapped_column(JSON_DICT, default=dict)
    log_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    seed_key: Mapped[str | None] = mapped_column(String(120), unique=True, nullable=True)
    # —— 断点续跑（checkpoint）相关 ——
    # 批次启动时的事件快照边界：只处理 id <= snapshot_max_event_id 的事件，
    # 执行期间持续到达的新事件不污染本批（由下一批或续跑时重新裁决）。
    snapshot_max_event_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    # 已确认表的逐表结果（表完整折叠并落库后写入），续跑时直接跳过这些表。
    checkpoint: Mapped[dict] = mapped_column(JSON_DICT, default=dict)
    # 执行进度（每处理完一个分块刷新），用于页面展示处理中/剩余数量。
    progress: Mapped[dict] = mapped_column(JSON_DICT, default=dict)
    # 执行心跳：worker 每处理一个分块刷新；running 且心跳超时判 interrupted。
    heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, index=True)
    attempt_count: Mapped[int] = mapped_column(Integer, default=0)
    started_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
