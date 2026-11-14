"""合并批次控制器：发起执行、中断识别、断点续跑。"""
from __future__ import annotations

from fastapi import Request
from sqlalchemy.orm import Session

from src.services import audit_service, merge_run_service, reject_service, table_service
from src.services.merge_run_service import MergeInterrupted


def _serialize(db: Session, run) -> dict:
    table = table_service.get_table(db, run.table_id) if run.table_id else None
    pending = merge_run_service.review_pending_for(db, run.id)
    return merge_run_service.serialize(run, table, review_pending=pending)


def create_run(db: Session, payload: dict, principal: dict, request: Request) -> dict:
    try:
        run = merge_run_service.create_and_execute(
            db,
            table_id=payload.get("table_id"),
            trigger=payload.get("trigger") or "manual",
            operator_email=principal["email"],
            allowed_lateness_seconds=payload.get("allowed_lateness_seconds"),
        )
    except MergeInterrupted as exc:
        # 执行期中断（含故障注入）：现场已随分块/确认点落库，标记 interrupted 让前端引导续跑，
        # 而不是返回 500；真实进程崩溃则由看门狗依据心跳超时兜底翻转。
        run = merge_run_service.get_run(db, exc.run_id)
        merge_run_service.mark_interrupted(db, run, reason=exc.reason)
        audit_service.record(
            db,
            actor_email=principal["email"],
            actor_role=principal["role"],
            action="run.create",
            entity="merge_run",
            entity_id=run.id if run else None,
            detail={"result": "interrupted", "reason": exc.reason},
            ip=request.client.host if request.client else None,
        )
        db.commit()
        return _serialize(db, run)
    audit_service.record(
        db,
        actor_email=principal["email"],
        actor_role=principal["role"],
        action="run.create",
        entity="merge_run",
        entity_id=run.id,
        detail={
            "run_no": run.run_no,
            "status": run.status,
            "events": run.events_in,
            "late": run.late_count,
            "snapshot_max_event_id": run.snapshot_max_event_id,
        },
        ip=request.client.host if request.client else None,
    )
    db.commit()
    return _serialize(db, run)


def resume_run(db: Session, run_id: int, principal: dict, request: Request) -> dict:
    run = merge_run_service.get_run(db, run_id)
    before_attempt = run.attempt_count or 0
    try:
        run = merge_run_service.resume(db, run)
    except MergeInterrupted as exc:
        merge_run_service.mark_interrupted(db, run, reason=exc.reason)
        audit_service.record(
            db,
            actor_email=principal["email"],
            actor_role=principal["role"],
            action="run.resume",
            entity="merge_run",
            entity_id=run.id,
            detail={"run_no": run.run_no, "result": "interrupted", "attempt": run.attempt_count, "reason": exc.reason},
            ip=request.client.host if request.client else None,
        )
        db.commit()
        return _serialize(db, run)
    audit_service.record(
        db,
        actor_email=principal["email"],
        actor_role=principal["role"],
        action="run.resume",
        entity="merge_run",
        entity_id=run.id,
        detail={
            "run_no": run.run_no,
            "result": run.status,
            "attempt": run.attempt_count,
            "resumed_from_attempt": before_attempt,
            "events": run.events_in,
            "review_pending": merge_run_service.review_pending_for(db, run.id),
        },
        ip=request.client.host if request.client else None,
    )
    db.commit()
    return _serialize(db, run)


def list_runs(db: Session, **filters) -> dict:
    return merge_run_service.list_runs(db, **filters)


def get_run(db: Session, run_id: int) -> dict:
    merge_run_service.sweep_stale_runs(db)
    run = merge_run_service.get_run(db, run_id)
    return _serialize(db, run)


def get_run_detail(db: Session, run_id: int) -> dict:
    merge_run_service.sweep_stale_runs(db)
    run = merge_run_service.get_run(db, run_id)
    rejects = reject_service.list_rejects(db, run_id=run.id, limit=100, offset=0)
    return {"run": _serialize(db, run), "rejects": rejects}
