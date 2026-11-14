"""断点续跑（checkpoint resume）集成测试：SQLite + 真实 ORM 表。

覆盖用户验收口径：
1. 中断后页面不再永远“运行中”——看门狗把心跳超时批次翻成 interrupted；
2. 续跑从已确认位置接上，同一批重跑不重复产出（拒绝行不翻倍）；
3. 中断期间新到事件按版本裁决且不冲掉已确认结果（快照边界隔离 + 下一批裁决）；
4. 完成时按源事件数 / 应用结果 / 待复核记录对账；
5. 页面三态（处理中 / 已完成 / 待复核）与剩余数量正确。
"""
from __future__ import annotations

from datetime import timedelta

import pytest

from src.config.settings import settings
from src.constants.enums import RunStatus
from src.models.merge_run import MergeRun
from src.models.reject_row import RejectRow
from src.services import merge_run_service
from src.services.merge_run_service import MergeInterrupted
from src.utils.timeutil import iso_utc, utcnow

BASE = utcnow() - timedelta(hours=1)


def _events(count: int, *, pk_prefix: str = "POL") -> list[dict]:
    """造 count 条正常 u 事件（pk 唯一，互不重复）。"""
    return [
        {
            "pk": f"{pk_prefix}-{index:04d}",
            "lsn": index,
            "op": "u",
            "event_time": BASE + timedelta(seconds=index),
            "ingest_at": BASE + timedelta(seconds=index + 1),
            "after": {"status": "已承保"},
        }
        for index in range(1, count + 1)
    ]


def _start_run(db, table_id=None):
    run = MergeRun(
        run_no=merge_run_service.next_run_no(db),
        table_id=table_id,
        trigger="manual",
        status=RunStatus.RUNNING.value,
        operator_email="admin@cdcmerge.io",
        allowed_lateness_seconds=30,
    )
    db.add(run)
    db.flush()
    return run


def _count_rejects(db, run_id: int) -> int:
    from sqlalchemy import func, select

    return db.scalar(select(func.count()).select_from(RejectRow).where(RejectRow.run_id == run_id)) or 0


@pytest.fixture(autouse=True)
def _reset_fail_injection():
    original = settings.merge_fail_after_chunks
    original_events = settings.merge_fail_after_events
    original_delay = settings.merge_chunk_delay_seconds
    settings.merge_fail_after_chunks = 0
    settings.merge_fail_after_events = 0
    settings.merge_chunk_delay_seconds = 0
    yield
    settings.merge_fail_after_chunks = original
    settings.merge_fail_after_events = original_events
    settings.merge_chunk_delay_seconds = original_delay


def test_full_run_writes_checkpoint_and_balances(db, source_factory):
    table = source_factory(events=_events(450))
    run = merge_run_service.create_and_execute(
        db, table_id=table.id, trigger="manual", operator_email="admin@cdcmerge.io"
    )
    assert run.status == RunStatus.SUCCESS.value
    assert run.attempt_count == 1
    checkpoint = run.checkpoint["tables"]
    assert set(checkpoint) == {str(table.id)}
    assert run.progress["processed_events"] == 450
    reconciliation = run.metrics["reconciliation"]
    assert reconciliation["balanced"] is True
    assert reconciliation["source_events"] == 450
    assert reconciliation["applied_events"] == 450
    assert reconciliation["rejected_events"] == 0


def test_interrupted_run_is_resumable_and_skips_confirmed_table(db, source_factory):
    # 两张表、每表 450 条。注入点 = 确认满 450 条事件（表 A 完整确认提交）后、进入表 B 前崩溃。
    table_a = source_factory(code="T_A", events=_events(450, pk_prefix="A"))
    table_b = source_factory(code="T_B", events=_events(450, pk_prefix="B"))
    settings.merge_fail_after_events = 450
    run = _start_run(db)

    with pytest.raises(MergeInterrupted):
        merge_run_service.execute(db, run, table_id=None)
    db.refresh(run)
    assert run.status == RunStatus.RUNNING.value  # 异常路径由调用方标记 interrupted
    merge_run_service.mark_interrupted(db, run, reason="模拟崩溃")
    db.refresh(run)
    assert run.status == RunStatus.INTERRUPTED.value
    confirmed = set(run.checkpoint["tables"])
    assert confirmed == {str(table_a.id)}  # 第一张表已确认，第二张未确认

    # 关闭故障注入后续跑
    settings.merge_fail_after_chunks = 0
    resumed = merge_run_service.resume(db, run)
    assert resumed.status == RunStatus.SUCCESS.value
    assert resumed.attempt_count == 2
    assert set(resumed.checkpoint["tables"]) == {str(table_a.id), str(table_b.id)}
    # 已确认表只跑一次：A 表指标来自首跑确认点，B 表来自续跑
    assert resumed.events_in == 900
    assert resumed.upsert_rows == 900


def test_resume_does_not_duplicate_reject_rows(db, source_factory):
    # 含重复 (pk, lsn) 的事件：制造 duplicate 拒绝行
    events = _events(120)
    duplicate = dict(events[0])
    events.append(duplicate)  # 同 pk 同 lsn -> duplicate
    table = source_factory(events=events)

    settings.merge_fail_after_chunks = 1  # 第一个分块后崩溃（表未确认）
    run = _start_run(db, table_id=table.id)
    with pytest.raises(MergeInterrupted):
        merge_run_service.execute(db, run, table_id=table.id)
    merge_run_service.mark_interrupted(db, run, reason="模拟崩溃")
    assert _count_rejects(db, run.id) == 0  # 表未确认 -> 没有任何拒绝行落库

    settings.merge_fail_after_chunks = 0
    resumed = merge_run_service.resume(db, run)
    assert resumed.status == RunStatus.SUCCESS.value
    assert _count_rejects(db, run.id) == 1  # 重跑整表只产生 1 条拒绝行，没有翻倍
    reject = db.query(RejectRow).filter_by(run_id=run.id).one()
    assert reject.reason == "duplicate"

    # 再续跑一次（幂等）：拒绝行仍只有 1 条
    merge_run_service._persist_table_rejects(db, run.id, resumed.checkpoint["tables"][str(table.id)]["rejects"])
    assert _count_rejects(db, run.id) == 1


def test_watchdog_marks_heartbeat_timeout_as_interrupted(db, source_factory):
    table = source_factory(events=_events(10))
    run = _start_run(db, table_id=table.id)
    run.heartbeat_at = utcnow() - timedelta(seconds=settings.run_stale_seconds + 30)
    db.commit()

    flipped = merge_run_service.sweep_stale_runs(db)
    db.refresh(run)
    assert flipped == 1
    assert run.status == RunStatus.INTERRUPTED.value
    assert "心跳" in run.progress["last_error"]

    # 看门狗幂等：已中断的批次不会被再次翻转
    assert merge_run_service.sweep_stale_runs(db) == 0


def test_fresh_heartbeat_is_not_marked_interrupted(db, source_factory):
    table = source_factory(events=_events(10))
    run = _start_run(db, table_id=table.id)
    run.heartbeat_at = utcnow()
    db.commit()
    assert merge_run_service.sweep_stale_runs(db) == 0
    db.refresh(run)
    assert run.status == RunStatus.RUNNING.value


def test_new_events_during_outage_are_isolated_and_adjudicated_next_batch(db, source_factory):
    events = _events(20)
    table = source_factory(events=events)

    # 首跑中途崩溃
    settings.merge_fail_after_chunks = 1
    run = _start_run(db, table_id=table.id)
    with pytest.raises(MergeInterrupted):
        merge_run_service.execute(db, run, table_id=table.id)
    merge_run_service.mark_interrupted(db, run, reason="模拟崩溃")
    boundary = run.snapshot_max_event_id
    assert boundary is not None

    # 中断期间持续到达的新事件（包括同 pk 的更高 lsn 版本）
    from src.models.cdc_event import CdcEvent

    new_version = {
        "pk": events[0]["pk"],
        "lsn": 9999,
        "op": "u",
        "event_time": BASE + timedelta(seconds=999),
        "ingest_at": utcnow(),
        "after": {"status": "已退保"},
    }
    db.add(
        CdcEvent(
            table_id=table.id,
            pk_value=new_version["pk"],
            lsn=new_version["lsn"],
            seq=999,
            op="u",
            event_time=new_version["event_time"],
            ingest_at=new_version["ingest_at"],
            before_data={},
            after_data=new_version["after"],
        )
    )
    db.commit()

    # 续跑：新事件被快照边界挡在本批之外，已确认结果不被冲掉
    settings.merge_fail_after_chunks = 0
    resumed = merge_run_service.resume(db, run)
    assert resumed.snapshot_max_event_id == boundary
    assert resumed.events_in == 20  # 仍是快照内的 20 条，不含中断期新事件
    first_pk_result = next(
        sample for sample in resumed.metrics["samples"] if sample["pk_value"] == events[0]["pk"]
    )
    assert first_pk_result["after"]["status"] == "已承保"  # 没被 9999 版本冲掉

    # 下一批（新发起）把新事件按 event_time/lsn 版本规则纳入，新版本胜出
    next_run = merge_run_service.create_and_execute(
        db, table_id=table.id, trigger="manual", operator_email="admin@cdcmerge.io"
    )
    assert next_run.snapshot_max_event_id > boundary
    first_pk_next = next(
        sample for sample in next_run.metrics["samples"] if sample["pk_value"] == events[0]["pk"]
    )
    assert first_pk_next["after"]["status"] == "已退保"


def test_resume_only_allowed_for_interrupted(db, source_factory):
    table = source_factory(events=_events(5))
    run = merge_run_service.create_and_execute(
        db, table_id=table.id, trigger="manual", operator_email="admin@cdcmerge.io"
    )
    from src.utils.errors import AppError

    with pytest.raises(AppError) as exc:
        merge_run_service.resume(db, run)
    assert exc.value.code == "RUN_NOT_RESUMABLE"


def test_serialize_exposes_display_status_remaining_and_review_pending(db, source_factory):
    events = _events(50)
    events.append(dict(events[0]))  # duplicate -> 1 条待复核
    table = source_factory(events=events)

    settings.merge_fail_after_chunks = 1
    run = _start_run(db, table_id=table.id)
    with pytest.raises(MergeInterrupted):
        merge_run_service.execute(db, run, table_id=table.id)
    merge_run_service.mark_interrupted(db, run, reason="模拟崩溃")

    payload = merge_run_service.serialize(run, table)
    assert payload["display_status"] == "processing"
    assert payload["display_status_label"] == "处理中"
    assert payload["resumable"] is True
    assert payload["status"] == RunStatus.INTERRUPTED.value
    assert payload["progress"]["remaining_events"] >= 0
    assert payload["progress"]["total_events"] == 51

    settings.merge_fail_after_chunks = 0
    done = merge_run_service.resume(db, run)
    done_payload = merge_run_service.serialize(
        done, table, review_pending=merge_run_service.review_pending_for(db, done.id)
    )
    assert done_payload["display_status"] == "completed"
    assert done_payload["review_pending"] == 1
    assert done_payload["progress"]["remaining_events"] == 0
    assert done_payload["reconciliation"]["review_pending"] == 1


def test_resumed_run_metrics_equal_uninterrupted_run(db, source_factory):
    """续跑完成的指标/拒绝行必须与一次性跑完逐字一致（确定性 + 不重复产出）。"""
    import copy

    events_a = _events(300, pk_prefix="A")
    events_a.append(dict(events_a[0]))  # 一条 duplicate
    events_b = _events(300, pk_prefix="B")
    table_a = source_factory(code="T_DET_A", events=events_a)
    table_b = source_factory(code="T_DET_B", events=events_b)

    # 基准：一次性跑完
    baseline = merge_run_service.create_and_execute(
        db, table_id=None, trigger="manual", operator_email="admin@cdcmerge.io"
    )
    baseline_rejects = sorted(
        (r.reason, r.pk_value, r.event_lsn) for r in db.query(RejectRow).filter_by(run_id=baseline.id).all()
    )

    # 中断后续跑（在第一张表确认后崩）
    settings.merge_fail_after_events = 301
    interrupted = _start_run(db)
    with pytest.raises(MergeInterrupted):
        merge_run_service.execute(db, interrupted, table_id=None)
    merge_run_service.mark_interrupted(db, interrupted, reason="模拟崩溃")
    settings.merge_fail_after_events = 0
    resumed = merge_run_service.resume(db, interrupted)

    # 指标逐字一致（剔除 resume 元数据与确认时间戳这类非确定性字段）
    def _comparable(metrics: dict) -> dict:
        cleaned = copy.deepcopy(metrics)
        cleaned.pop("resume", None)
        for table in (cleaned.get("tables") or {}).values():
            table.pop("confirmed_at", None)
        return cleaned

    assert _comparable(resumed.metrics) == _comparable(baseline.metrics)
    resumed_rejects = sorted(
        (r.reason, r.pk_value, r.event_lsn) for r in db.query(RejectRow).filter_by(run_id=resumed.id).all()
    )
    assert resumed_rejects == baseline_rejects  # 拒绝行集合完全相同，没有重复
    assert resumed.status == RunStatus.SUCCESS.value


def test_resume_after_all_tables_confirmed_but_before_finalize(db, source_factory):
    """边界：所有表都已确认、仅收尾（finalize）前崩溃。续跑不应重算任何表，直接成功。"""
    table_a = source_factory(code="T_FIN_A", events=_events(120, pk_prefix="A"))
    table_b = source_factory(code="T_FIN_B", events=_events(120, pk_prefix="B"))
    run = _start_run(db)
    # 手动初始化快照边界与 checkpoint 骨架（模拟首跑已完成初始化提交）
    run.snapshot_max_event_id = merge_run_service._snapshot_boundary(db, None)
    run.checkpoint = {"version": 1, "watermark": None, "tables": {}}
    db.commit()

    # 直接构造“全部确认但未 finalize”的现场
    from src.services import watermark_merge

    all_events = merge_run_service._load_snapshot_events(db, None, run.snapshot_max_event_id)
    for tid in (table_a.id, table_b.id):
        table = db.get(type(table_a), tid)
        rows = [e for e in all_events if e["table_id"] == tid]
        result = watermark_merge.merge_table(
            rows,
            allowed_lateness_seconds=30,
            schema_columns=table.schema_columns or [],
            watermark=None,
        )
        for reject in result["rejects"]:
            reject["table_id"] = tid
        merge_run_service._persist_table_rejects(db, run.id, result["rejects"])
        cp = dict(run.checkpoint or {"version": 1, "watermark": None, "tables": {}})
        cp.setdefault("tables", {})
        cp["tables"][str(tid)] = {
            "table_id": tid,
            "table_code": table.code,
            "table_name": table.name,
            "events_in": result["metrics"]["events_in"],
            "metrics": result["metrics"],
            "rejects": merge_run_service._jsonable_rejects(
                merge_run_service._dedupe_rejects(result["rejects"])
            ),
            "samples": [],
            "confirmed_at": iso_utc(utcnow()),
        }
        run.checkpoint = cp
    run.status = RunStatus.INTERRUPTED.value
    run.attempt_count = 1  # 首跑已尝试一次，续跑应累加到 2
    db.commit()

    # 续跑：所有表已确认 -> 全部跳过 -> 直接 finalize 成功
    resumed = merge_run_service.resume(db, run)
    assert resumed.status == RunStatus.SUCCESS.value
    assert resumed.events_in == 240
    assert resumed.attempt_count == 2
    assert set(resumed.checkpoint["tables"]) == {str(table_a.id), str(table_b.id)}
    assert resumed.metrics["reconciliation"]["balanced"] is True


def test_reconciliation_balances_with_reject_lifecycle(db, source_factory):
    events = _events(30)
    events.append(dict(events[0]))  # duplicate
    table = source_factory(events=events)
    run = merge_run_service.create_and_execute(
        db, table_id=table.id, trigger="manual", operator_email="admin@cdcmerge.io"
    )
    rec = run.metrics["reconciliation"]
    assert rec["source_events"] == 31
    assert rec["rejected_events"] == 1
    assert rec["applied_events"] == 30
    assert rec["review_pending"] == 1
    assert rec["balanced"] is True
    assert run.status == RunStatus.SUCCESS.value

    # 人工处置（重放）后对账口径仍闭环
    reject = db.query(RejectRow).filter_by(run_id=run.id).one()
    from src.services import reject_service

    reject_service.replay(db, reject, replay_status="replayed", note="重放验证", reviewer="analyst@cdcmerge.io")
    db.commit()
    rec2 = merge_run_service._build_reconciliation(db, run, run.events_in, run.rejected_count)
    assert rec2["review_replayed"] == 1
    assert rec2["review_pending"] == 0
    assert rec2["balanced"] is True
