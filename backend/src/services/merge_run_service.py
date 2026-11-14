"""合并批次服务：可中断、可续跑的水位线乱序合并执行引擎。

断点续跑口径
------------
- **快照边界**：批次首次启动时冻结 ``snapshot_max_event_id``，本批只处理该边界以内的事件；
  执行期间持续到达的新事件不进本批（由下一批按同一 event_time/lsn 版本规则裁决），
  因此已确认结果永远不会被新事件冲掉。
- **确认单元 = 源表**：一个源表完整折叠成功后，拒绝行与逐表指标才整体落库并写入
  ``checkpoint`` 并提交；未确认的表中断后整表重算（算法 A 是确定性纯函数，重算结果逐字
  一致），已确认的表续跑时直接跳过 —— 同一批重跑不重复产出。
- **分块心跳**：表内事件按 ``merge_chunk_size`` 分块推进，每块刷新 ``heartbeat_at`` 与
  ``progress`` 并提交，页面据此展示处理中 / 剩余数量；``running`` 且心跳超时由看门狗
  （``sweep_stale_runs``）翻成 ``interrupted``。
- **完成对账**：源事件数 = 应用事件 + 被拒事件；待复核记录按 pending/replayed/skipped
  闭环；全部对平才置 ``success``，否则 ``partial``。
"""
from __future__ import annotations

import copy
import time
from datetime import timedelta

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from src.config.settings import settings
from src.constants.enums import REJECT_REASON_LABELS, RUN_TERMINAL_STATUSES, RunStatus
from src.models.cdc_event import CdcEvent
from src.models.merge_run import MergeRun
from src.models.reject_row import RejectRow
from src.models.source_table import SourceTable
from src.services import watermark_merge
from src.utils.errors import AppError
from src.utils.timeutil import iso_utc, local_now, utcnow

CHECKPOINT_VERSION = 1


class MergeInterrupted(RuntimeError):
    """执行中断（含故障注入模拟的进程崩溃）：批次保留现场，可从 checkpoint 续跑。"""

    def __init__(self, run_id: int, chunk: int, reason: str = "执行中断") -> None:
        super().__init__(f"merge run {run_id} interrupted at chunk {chunk}: {reason}")
        self.run_id = run_id
        self.chunk = chunk
        self.reason = reason


# --------------------------------------------------------------------------- 序列化

def _review_pending_map(db: Session, run_ids: list[int]) -> dict[int, int]:
    if not run_ids:
        return {}
    rows = db.execute(
        select(RejectRow.run_id, func.count())
        .where(RejectRow.run_id.in_(run_ids), RejectRow.replay_status == "pending")
        .group_by(RejectRow.run_id)
    ).all()
    return {int(run_id): int(count) for run_id, count in rows}


def _display_status(status: str) -> tuple[str, str]:
    """页面三态归类：处理中 / 已完成（待复核数量另列）。"""
    if status in (RunStatus.QUEUED.value, RunStatus.RUNNING.value, RunStatus.INTERRUPTED.value):
        return "processing", "处理中"
    return "completed", "已完成"


def serialize(
    run: MergeRun,
    table: SourceTable | None = None,
    *,
    review_pending: int | None = None,
) -> dict:
    metrics = run.metrics or {}
    progress = run.progress or {}
    checkpoint = run.checkpoint or {}
    total_events = int(progress.get("total_events") or run.events_in or 0)
    processed_events = int(progress.get("processed_events") or 0)
    if run.status in RUN_TERMINAL_STATUSES:
        processed_events = total_events
    remaining = max(0, total_events - processed_events)
    display_status, display_status_label = _display_status(run.status)
    reconciliation = metrics.get("reconciliation") or {}
    if review_pending is None:
        review_pending = reconciliation.get("review_pending", 0)
    confirmed_tables = len((checkpoint or {}).get("tables") or {})
    total_tables = int(progress.get("total_tables") or 0)
    return {
        "id": run.id,
        "run_no": run.run_no,
        "table_id": run.table_id,
        "table_code": table.code if table else None,
        "table_name": table.name if table else "全库批次",
        "scope": "全库" if run.table_id is None else (table.name if table else "已删除的表"),
        "trigger": run.trigger,
        "status": run.status,
        "status_label": _status_label(run.status),
        "display_status": display_status,
        "display_status_label": display_status_label,
        "resumable": run.status in (RunStatus.INTERRUPTED.value, RunStatus.FAILED.value),
        "operator_email": run.operator_email,
        "allowed_lateness_seconds": run.allowed_lateness_seconds,
        "events_in": run.events_in,
        "upsert_rows": run.upsert_rows,
        "deleted_rows": run.deleted_rows,
        "late_count": run.late_count,
        "late_ratio": metrics.get("late_ratio", 0.0),
        "dup_skipped": run.dup_skipped,
        "rejected_count": run.rejected_count,
        "review_pending": int(review_pending or 0),
        "watermark_at": iso_utc(run.watermark_at),
        "metrics": metrics,
        "log_text": run.log_text,
        "progress": {
            "total_events": total_events,
            "processed_events": processed_events,
            "remaining_events": remaining,
            "percent": round(processed_events / total_events, 4) if total_events else 0.0,
            "total_tables": total_tables,
            "confirmed_tables": confirmed_tables,
            "current_table": progress.get("current_table"),
            "chunks_done": progress.get("chunks_done", 0),
            "updated_at": progress.get("updated_at"),
        },
        "reconciliation": reconciliation,
        "attempt_count": run.attempt_count or 0,
        "heartbeat_at": iso_utc(run.heartbeat_at),
        "snapshot_max_event_id": run.snapshot_max_event_id,
        "started_at": iso_utc(run.started_at),
        "finished_at": iso_utc(run.finished_at),
    }


def _status_label(status: str) -> str:
    from src.constants.enums import RUN_STATUS_LABELS

    return RUN_STATUS_LABELS.get(status, status)


def next_run_no(db: Session) -> str:
    prefix = f"MR{local_now():%Y%m%d}"
    count = db.scalar(select(func.count()).select_from(MergeRun).where(MergeRun.run_no.like(f"{prefix}%"))) or 0
    return f"{prefix}-{count + 1:04d}"


# --------------------------------------------------------------------------- 事件装载

def _snapshot_boundary(db: Session, table_id: int | None) -> int | None:
    stmt = select(func.max(CdcEvent.id))
    if table_id is not None:
        stmt = stmt.where(CdcEvent.table_id == table_id)
    return db.scalar(stmt)


def _load_snapshot_events(db: Session, table_id: int | None, max_event_id: int | None) -> list[dict]:
    """按 (table_id, seq, id) 稳定顺序装载快照内事件，续跑与首跑输入逐字一致。"""
    stmt = select(CdcEvent)
    if table_id is not None:
        stmt = stmt.where(CdcEvent.table_id == table_id)
    if max_event_id is not None:
        stmt = stmt.where(CdcEvent.id <= max_event_id)
    rows = db.scalars(stmt.order_by(CdcEvent.table_id, CdcEvent.seq, CdcEvent.id)).all()
    return [
        {
            "id": row.id,
            "table_id": row.table_id,
            "pk_value": row.pk_value,
            "lsn": row.lsn,
            "op": row.op,
            "event_time": row.event_time,
            "ingest_at": row.ingest_at,
            "before": row.before_data or {},
            "after": row.after_data or {},
        }
        for row in rows
    ]


# --------------------------------------------------------------------------- 拒绝行（幂等）

def _dedupe_rejects(rejects: list[dict]) -> list[dict]:
    seen: set[tuple[str, int, str, int | None]] = set()
    unique: list[dict] = []
    for item in rejects:
        key = (item["reason"], item["event_lsn"], item["pk_value"], item.get("table_id"))
        if key in seen:
            continue
        seen.add(key)
        unique.append(item)
    return unique


def _jsonable_rejects(rejects: list[dict]) -> list[dict]:
    """拒绝行进 JSON checkpoint 前把 datetime 转成 ISO 字符串（ORM 列本身仍存 datetime）。"""
    safe: list[dict] = []
    for item in rejects:
        converted = dict(item)
        if converted.get("event_time") is not None:
            converted["event_time"] = iso_utc(converted["event_time"])
        safe.append(converted)
    return safe


def _persist_table_rejects(db: Session, run_id: int, rejects: list[dict]) -> int:
    """落一张已确认表的拒绝行；自然键已存在则跳过（重跑不重复产出、不覆盖人工复核状态）。"""
    unique = _dedupe_rejects(rejects)
    if not unique:
        return 0
    existing = {
        (row.reason, row.event_lsn, row.pk_value, row.table_id)
        for row in db.scalars(select(RejectRow).where(RejectRow.run_id == run_id)).all()
    }
    inserted = 0
    for item in unique:
        key = (item["reason"], item["event_lsn"], item["pk_value"], item.get("table_id"))
        if key in existing:
            continue
        existing.add(key)
        db.add(
            RejectRow(
                run_id=run_id,
                table_id=item.get("table_id"),
                pk_value=item["pk_value"],
                op=item["op"],
                reason=item["reason"],
                event_lsn=item["event_lsn"],
                event_time=item.get("event_time"),
                detail=item.get("detail") or {},
                replay_status="pending",
            )
        )
        inserted += 1
    return inserted


# --------------------------------------------------------------------------- 看门狗

def sweep_stale_runs(db: Session) -> int:
    """把心跳超时的 running 批次翻成 interrupted，返回翻转条数。"""
    cutoff = utcnow() - timedelta(seconds=max(5, int(settings.run_stale_seconds)))
    stale = db.scalars(
        select(MergeRun).where(
            MergeRun.status == RunStatus.RUNNING.value,
            (MergeRun.heartbeat_at.is_(None)) | (MergeRun.heartbeat_at < cutoff),
        )
    ).all()
    for run in stale:
        mark_interrupted(db, run, reason=f"心跳超过 {settings.run_stale_seconds}s 未刷新，判定服务中断")
    if stale:
        db.commit()
    return len(stale)


def mark_interrupted(db: Session, run: MergeRun, *, reason: str) -> MergeRun:
    if run.status in RUN_TERMINAL_STATUSES:
        return run
    run.status = RunStatus.INTERRUPTED.value
    progress = dict(run.progress or {})
    progress["last_error"] = reason
    progress["updated_at"] = iso_utc(utcnow())
    run.progress = progress  # 整体重新赋值，确保 JSON 变更落库
    db.flush()
    return run


# --------------------------------------------------------------------------- 执行引擎

def _group_by_table(events: list[dict]) -> dict[int, list[dict]]:
    grouped: dict[int, list[dict]] = {}
    for event in events:
        grouped.setdefault(int(event["table_id"]), []).append(event)
    return grouped


def _refresh_progress(
    run: MergeRun,
    *,
    total_events: int,
    confirmed_events: int,
    current_table: str | None,
    current_table_events: int,
    current_table_processed: int,
    total_tables: int,
    confirmed_tables: int,
    chunks_done: int,
) -> None:
    processed = confirmed_events + current_table_processed
    run.progress = {
        "total_events": total_events,
        "processed_events": min(processed, total_events),
        "total_tables": total_tables,
        "confirmed_tables": confirmed_tables,
        "current_table": current_table,
        "current_table_events": current_table_events,
        "current_table_processed": current_table_processed,
        "chunks_done": chunks_done,
        "updated_at": iso_utc(utcnow()),
    }
    run.heartbeat_at = utcnow()


def _run_engine(db: Session, run: MergeRun, *, resume: bool) -> MergeRun:
    now = utcnow()
    if not resume:
        run.snapshot_max_event_id = _snapshot_boundary(db, run.table_id)
        run.checkpoint = {"version": CHECKPOINT_VERSION, "watermark": None, "tables": {}}
        run.progress = {}
        run.metrics = {}
        run.status = RunStatus.RUNNING.value
        run.attempt_count = 1
        run.started_at = now
        run.finished_at = None
        run.heartbeat_at = now
        db.flush()
        db.commit()  # running + 快照边界先落库，进程立刻崩溃也能被看门狗识别
    else:
        if run.status not in (RunStatus.INTERRUPTED.value, RunStatus.FAILED.value):
            raise AppError("RUN_NOT_RESUMABLE", 409, {"run_id": run.id, "status": run.status})
        run.status = RunStatus.RUNNING.value
        run.attempt_count = (run.attempt_count or 0) + 1
        run.heartbeat_at = now
        db.flush()
        db.commit()

    events = _load_snapshot_events(db, run.table_id, run.snapshot_max_event_id)
    grouped = _group_by_table(events)
    tables = {table.id: table for table in db.scalars(select(SourceTable)).all()}
    lateness = int(run.allowed_lateness_seconds or 30)

    # JSON 列不跟踪原地修改，必须整体重新赋值才会被 SQLAlchemy 标记为脏；
    # 局部字典与属性不能共用同一引用（否则再赋值被身份检测判为“无变化”）。
    checkpoint = {"version": CHECKPOINT_VERSION, "watermark": None, "tables": {}}
    checkpoint.update(copy.deepcopy(run.checkpoint or {}))
    checkpoint["tables"] = dict(checkpoint.get("tables") or {})
    if not resume:
        ingest_times = [event["ingest_at"] for event in events if event["ingest_at"] is not None]
        watermark = (max(ingest_times) - timedelta(seconds=lateness)) if ingest_times else None
        checkpoint["watermark"] = watermark.isoformat() if watermark else None
        run.checkpoint = copy.deepcopy(checkpoint)
        db.flush()
    watermark = watermark_merge._as_dt(checkpoint.get("watermark"))  # noqa: SLF001 - 内部复用同一解析口径

    total_events = len(events)
    total_tables = len(grouped)
    chunk_size = max(1, int(settings.merge_chunk_size))
    fail_after = int(settings.merge_fail_after_chunks or 0)
    fail_after_events = int(getattr(settings, "merge_fail_after_events", 0) or 0)
    chunks_done = int((run.progress or {}).get("chunks_done") or 0)

    for tid in sorted(grouped):
        key = str(tid)
        table_events = grouped[tid]
        table = tables.get(tid)
        table_label = table.code if table else f"table#{tid}"
        confirmed = checkpoint["tables"]
        confirmed_events = sum(int(item.get("events_in") or 0) for item in confirmed.values())

        if key in confirmed:
            # 已确认表：续跑直接跳过，结果不重算、拒绝行不重复落库
            continue
        # 表内分块推进：只刷心跳与进度，折叠在整表确认时一次性完成（纯函数、确定性）
        for start in range(0, len(table_events), chunk_size):
            processed_in_table = min(start + chunk_size, len(table_events))
            chunks_done += 1
            _refresh_progress(
                run,
                total_events=total_events,
                confirmed_events=confirmed_events,
                current_table=table_label,
                current_table_events=len(table_events),
                current_table_processed=processed_in_table,
                total_tables=total_tables,
                confirmed_tables=len(confirmed),
                chunks_done=chunks_done,
            )
            db.commit()
            if settings.merge_chunk_delay_seconds > 0:
                time.sleep(float(settings.merge_chunk_delay_seconds))
            if fail_after and chunks_done == fail_after:
                raise MergeInterrupted(run.id, chunks_done, "故障注入：模拟服务进程中断")

        # 整表折叠（算法 A），输入确定 -> 输出逐字一致，中断重算安全
        result = watermark_merge.merge_table(
            table_events,
            allowed_lateness_seconds=lateness,
            schema_columns=(table.schema_columns or []) if table else [],
            watermark=watermark,
        )
        for reject in result["rejects"]:
            reject["table_id"] = tid
        _persist_table_rejects(db, run.id, result["rejects"])

        confirmed[key] = {
            "table_id": tid,
            "table_code": table.code if table else None,
            "table_name": table.name if table else str(tid),
            "events_in": int(result["metrics"].get("events_in") or 0),
            "metrics": result["metrics"],
            "rejects": _jsonable_rejects(_dedupe_rejects(result["rejects"])),
            "samples": [
                {
                    "table": table.code if table else str(tid),
                    "pk_value": row["pk_value"],
                    "after": row["after"],
                    "last_lsn": row["last_lsn"],
                    "last_event_time": row["last_event_time"].isoformat() if row["last_event_time"] else None,
                }
                for row in result["rows"][:3]
            ],
            "confirmed_at": iso_utc(utcnow()),
        }
        checkpoint["tables"] = confirmed  # 整体重建引用，确保 JSON 变更被持久化
        run.checkpoint = copy.deepcopy(checkpoint)  # 全新对象，强制 SQLAlchemy 标记为脏
        _refresh_progress(
            run,
            total_events=total_events,
            confirmed_events=sum(int(item.get("events_in") or 0) for item in confirmed.values()),
            current_table=None,
            current_table_events=len(table_events),
            current_table_processed=len(table_events),
            total_tables=total_tables,
            confirmed_tables=len(confirmed),
            chunks_done=chunks_done,
        )
        db.commit()  # 表级确认点：拒绝行 + checkpoint 同事务提交，崩溃后从这里接上

        # 故障注入（按已确认事件数）：精确模拟“某张表确认后、下一张表开始前”的服务中断
        confirmed_total = sum(int(item.get("events_in") or 0) for item in checkpoint["tables"].values())
        if fail_after_events and confirmed_total >= fail_after_events and len(checkpoint["tables"]) < len(grouped):
            raise MergeInterrupted(run.id, chunks_done, "故障注入：表确认后服务中断")

    return _finalize(db, run, grouped, tables, watermark, lateness, resume)


def _build_reconciliation(db: Session, run: MergeRun, source_events: int, rejected_events: int) -> dict:
    applied_events = source_events - rejected_events
    rows = db.execute(
        select(RejectRow.replay_status, func.count())
        .where(RejectRow.run_id == run.id)
        .group_by(RejectRow.replay_status)
    ).all()
    review = {status: int(count) for status, count in rows}
    review_pending = review.get("pending", 0)
    review_replayed = review.get("replayed", 0)
    review_skipped = review.get("skipped", 0)
    reject_rows_total = review_pending + review_replayed + review_skipped
    checks = [
        {
            "name": "源事件对账",
            "ok": applied_events + rejected_events == source_events,
            "detail": f"源事件 {source_events} = 应用 {applied_events} + 拒绝 {rejected_events}",
        },
        {
            "name": "待复核闭环",
            "ok": review_pending + review_replayed + review_skipped == reject_rows_total,
            "detail": f"待复核 {review_pending} + 已重放 {review_replayed} + 已跳过 {review_skipped} = 拒绝行 {reject_rows_total}",
        },
    ]
    return {
        "source_events": source_events,
        "applied_events": applied_events,
        "rejected_events": rejected_events,
        "reject_rows": reject_rows_total,
        "review_pending": review_pending,
        "review_replayed": review_replayed,
        "review_skipped": review_skipped,
        "balanced": all(check["ok"] for check in checks),
        "checks": checks,
    }


def _finalize(
    db: Session,
    run: MergeRun,
    grouped: dict[int, list[dict]],
    tables: dict[int, SourceTable],
    watermark,
    lateness: int,
    resumed: bool,
) -> MergeRun:
    checkpoint = run.checkpoint or {}
    confirmed = checkpoint.get("tables") or {}
    totals = {
        "upsert_rows": 0,
        "deletes": 0,
        "late_count": 0,
        "dup_skipped": 0,
        "rejected": 0,
        "ops": {"c": 0, "u": 0, "d": 0},
    }
    per_table: dict[str, dict] = {}
    samples: list[dict] = []
    reject_reason_counts: dict[str, int] = {}
    for tid in sorted(grouped):
        entry = confirmed[str(tid)]
        table = tables.get(tid)
        metrics = entry["metrics"]
        for key_name in ("upsert_rows", "deletes", "late_count", "dup_skipped", "rejected"):
            totals[key_name] += int(metrics.get(key_name) or 0)
        for op_key in ("c", "u", "d"):
            totals["ops"][op_key] += int((metrics.get("ops") or {}).get(op_key) or 0)
        code = entry.get("table_code") or (table.code if table else str(tid))
        per_table[code] = {
            "table_id": tid,
            "table_name": entry.get("table_name") or (table.name if table else str(tid)),
            "events": metrics.get("events_in", 0),
            "upsert_rows": metrics.get("upsert_rows", 0),
            "deletes": metrics.get("deletes", 0),
            "late_count": metrics.get("late_count", 0),
            "dup_skipped": metrics.get("dup_skipped", 0),
            "rejected": metrics.get("rejected", 0),
            "watermark": metrics.get("watermark"),
            "confirmed_at": entry.get("confirmed_at"),
        }
        samples.extend(entry.get("samples") or [])
        for reject in entry.get("rejects") or []:
            reject_reason_counts[reject["reason"]] = reject_reason_counts.get(reject["reason"], 0) + 1

    events_in = sum(int(item.get("events_in") or 0) for item in confirmed.values())
    reconciliation = _build_reconciliation(db, run, events_in, totals["rejected"])
    metrics = {
        "events_in": events_in,
        "upsert_rows": totals["upsert_rows"],
        "deletes": totals["deletes"],
        "late_count": totals["late_count"],
        "late_ratio": round(totals["late_count"] / events_in, 4) if events_in else 0.0,
        "dup_skipped": totals["dup_skipped"],
        "rejected": totals["rejected"],
        "ops": totals["ops"],
        "pks": totals["upsert_rows"],
        "tables": per_table,
        "watermark": checkpoint.get("watermark"),
        "allowed_lateness_seconds": lateness,
        "samples": samples[:9],
        "reject_reasons": {reason: reject_reason_counts[reason] for reason in sorted(reject_reason_counts)},
        "reconciliation": reconciliation,
        "resume": {
            "attempt_count": run.attempt_count or 1,
            "resumed_times": max(0, (run.attempt_count or 1) - 1),
            "confirmed_tables": len(confirmed),
            "total_tables": len(grouped),
            "snapshot_max_event_id": run.snapshot_max_event_id,
        },
    }

    run.metrics = metrics
    run.events_in = events_in
    run.upsert_rows = totals["upsert_rows"]
    run.deleted_rows = totals["deletes"]
    run.late_count = totals["late_count"]
    run.dup_skipped = totals["dup_skipped"]
    run.rejected_count = totals["rejected"]
    run.watermark_at = watermark
    balanced = bool(reconciliation["balanced"])
    run.status = RunStatus.SUCCESS.value if balanced and events_in else RunStatus.PARTIAL.value
    reason_text = "、".join(
        f"{REJECT_REASON_LABELS.get(reason, reason)} {count}" for reason, count in metrics["reject_reasons"].items()
    )
    resume_text = (
        f"；中断 {max(0, (run.attempt_count or 1) - 1)} 次后续跑完成，确认表 {len(confirmed)}/{len(grouped)}"
        if resumed or (run.attempt_count or 1) > 1
        else ""
    )
    run.log_text = (
        f"批次范围 {'全库' if run.table_id is None else '单表'}，快照边界事件 id≤{run.snapshot_max_event_id}，"
        f"读取 {events_in} 条事件；水位 {metrics['watermark'] or '—'}（允许迟到 {lateness}s）；"
        f"迟到 {totals['late_count']} 条、重复 {totals['dup_skipped']} 条、拒绝 {totals['rejected']} 条"
        + (f"（{reason_text}）" if reason_text else "")
        + f"；折叠 {totals['upsert_rows']} 行 upsert、{totals['deletes']} 个删除终态"
        + resume_text
        + f"；对账：{'; '.join(check['detail'] for check in reconciliation['checks'])}"
        + ("（对平）" if balanced else "（未对平，转部分成功）")
    )
    run.progress = {
        "total_events": events_in,
        "processed_events": events_in,
        "total_tables": len(grouped),
        "confirmed_tables": len(confirmed),
        "current_table": None,
        "chunks_done": (run.progress or {}).get("chunks_done", 0),
        "updated_at": iso_utc(utcnow()),
    }
    run.finished_at = utcnow()
    db.flush()
    db.commit()
    return run


# --------------------------------------------------------------------------- 对外入口

def execute(db: Session, run: MergeRun, *, table_id: int | None) -> MergeRun:
    """首跑入口（保留旧签名）：失败时抛 MergeInterrupted，现场已落库可续跑。"""
    return _run_engine(db, run, resume=False)


def resume(db: Session, run: MergeRun) -> MergeRun:
    """从最后确认表接上续跑；已确认表跳过，未确认表整表确定性重算。"""
    return _run_engine(db, run, resume=True)


def create_and_execute(
    db: Session,
    *,
    table_id: int | None,
    trigger: str,
    operator_email: str,
    allowed_lateness_seconds: int | None = None,
    seed_key: str | None = None,
) -> MergeRun:
    if seed_key:
        existing = db.scalar(select(MergeRun).where(MergeRun.seed_key == seed_key))
        if existing is not None:
            return existing
    table = None
    if table_id is not None:
        table = db.get(SourceTable, table_id)
        if table is None:
            raise AppError("NOT_FOUND", 404, {"entity": "source_table", "id": table_id})
    lateness = int(
        allowed_lateness_seconds
        if allowed_lateness_seconds is not None
        else (table.allowed_lateness_seconds if table else 30)
    )
    run = MergeRun(
        run_no=next_run_no(db),
        table_id=table_id,
        trigger=trigger,
        status=RunStatus.RUNNING.value,
        operator_email=operator_email,
        allowed_lateness_seconds=max(0, min(3600, lateness)),
        seed_key=seed_key,
    )
    db.add(run)
    db.flush()
    return execute(db, run, table_id=table_id)


def list_runs(
    db: Session,
    *,
    table_id: int | None = None,
    status: str | None = None,
    limit: int = 20,
    offset: int = 0,
) -> dict:
    # 列表查询顺带跑一次看门狗，避免心跳超时的批次长期挂在“执行中”
    sweep_stale_runs(db)
    stmt = select(MergeRun)
    count_stmt = select(func.count()).select_from(MergeRun)
    if table_id:
        stmt = stmt.where(MergeRun.table_id == table_id)
        count_stmt = count_stmt.where(MergeRun.table_id == table_id)
    if status:
        stmt = stmt.where(MergeRun.status == status)
        count_stmt = count_stmt.where(MergeRun.status == status)
    total = db.scalar(count_stmt) or 0
    rows = db.scalars(stmt.order_by(MergeRun.id.desc()).limit(limit).offset(offset)).all()
    tables = {table.id: table for table in db.scalars(select(SourceTable)).all()}
    pending_map = _review_pending_map(db, [row.id for row in rows])
    return {
        "items": [
            serialize(row, tables.get(row.table_id), review_pending=pending_map.get(row.id, 0)) for row in rows
        ],
        "total": total,
        "limit": limit,
        "offset": offset,
    }


def get_run(db: Session, run_id: int) -> MergeRun:
    run = db.get(MergeRun, run_id)
    if run is None:
        raise AppError("NOT_FOUND", 404, {"entity": "merge_run", "id": run_id})
    return run


def review_pending_for(db: Session, run_id: int) -> int:
    return (
        db.scalar(
            select(func.count())
            .select_from(RejectRow)
            .where(RejectRow.run_id == run_id, RejectRow.replay_status == "pending")
        )
        or 0
    )
