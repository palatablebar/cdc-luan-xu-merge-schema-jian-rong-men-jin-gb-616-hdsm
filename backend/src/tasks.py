"""Celery 任务：心跳、中断看门狗、每日对启用的源表跑一次全库合并。"""
from __future__ import annotations

import time

from sqlalchemy import select

from src.config.celery_app import TASK_DAILY, TASK_HEARTBEAT, TASK_WATCHDOG, celery_app
from src.config.settings import settings
from src.utils.logger import get_logger

logger = get_logger("tasks")


def _redis():
    import redis

    return redis.Redis.from_url(settings.redis_url, decode_responses=True)


@celery_app.task(name=TASK_HEARTBEAT)
def heartbeat() -> str:
    try:
        _redis().set(f"{settings.app_name}:worker:heartbeat", time.time())
        return "ok"
    except Exception as exc:  # noqa: BLE001
        logger.warning("heartbeat failed: %s", exc)
        return "error"


@celery_app.task(name=TASK_WATCHDOG)
def run_watchdog() -> dict:
    """把心跳超时的 running 批次翻成 interrupted，避免页面永远停在“执行中”。"""
    from src.config.database import session_scope
    from src.services import merge_run_service

    with session_scope() as db:
        flipped = merge_run_service.sweep_stale_runs(db)
    if flipped:
        logger.warning("watchdog marked %s stale merge run(s) as interrupted", flipped)
    return {"interrupted": flipped}


@celery_app.task(name=TASK_DAILY)
def daily_refresh() -> dict:
    from src.config.database import session_scope
    from src.models.source_table import SourceTable
    from src.services import audit_service, merge_run_service
    from src.services.merge_run_service import MergeInterrupted

    with session_scope() as db:
        enabled = db.scalars(select(SourceTable).where(SourceTable.enabled.is_(True))).all()
        if not enabled:
            logger.info("daily merge skipped: no enabled source table")
            return {"tables": 0}
        try:
            run = merge_run_service.create_and_execute(
                db,
                table_id=None,
                trigger="schedule",
                operator_email=f"scheduler@{settings.app_name}.io",
            )
        except MergeInterrupted as exc:
            logger.warning("daily merge interrupted: %s", exc)
            return {"interrupted": True, "reason": exc.reason}
        audit_service.record(
            db,
            actor_email=f"scheduler@{settings.app_name}.io",
            actor_role="admin",
            action="run.create",
            entity="merge_run",
            entity_id=run.id,
            detail={"trigger": "schedule", "run_no": run.run_no, "events": run.events_in, "late": run.late_count},
            layer="worker",
        )
    logger.info("daily merge finished: %s events in run %s", run.events_in, run.run_no)
    return {"run_no": run.run_no, "events": run.events_in}
