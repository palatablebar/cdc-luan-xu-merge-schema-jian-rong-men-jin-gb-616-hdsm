"""控制器层测试：首跑中断优雅落 interrupted、续跑与权限前置条件。

不依赖 httpx/TestClient，直接以最小 stub 调用 controller，保证在仅装 requirements.txt
的环境（Docker 镜像）里也能跑。
"""
from __future__ import annotations

from datetime import timedelta
from types import SimpleNamespace

import pytest

from src.config.settings import settings
from src.constants.enums import RunStatus
from src.controllers import run_controller
from src.services import merge_run_service
from src.utils.errors import AppError
from src.utils.timeutil import utcnow

BASE = utcnow() - timedelta(hours=1)


class _FakeRequest:
    client = SimpleNamespace(host="127.0.0.1")


_PRINCIPAL = {"email": "admin@cdcmerge.io", "role": "admin"}


def _seed_table(db, source_factory, count: int = 400):
    events = [
        {
            "pk": f"P-{i}",
            "lsn": i,
            "op": "u",
            "event_time": BASE + timedelta(seconds=i),
            "ingest_at": BASE + timedelta(seconds=i + 1),
            "after": {"status": "ok"},
        }
        for i in range(1, count + 1)
    ]
    return source_factory(code="T_HTTP", events=events)


@pytest.fixture(autouse=True)
def _reset_fail_injection():
    original = settings.merge_fail_after_chunks
    settings.merge_fail_after_chunks = 0
    yield
    settings.merge_fail_after_chunks = original


def test_create_run_returns_interrupted_instead_of_500(db, source_factory):
    table = _seed_table(db, source_factory)
    settings.merge_fail_after_chunks = 1  # 首个分块后中断，表未确认

    payload = run_controller.create_run(
        db, {"table_id": table.id, "trigger": "manual"}, _PRINCIPAL, _FakeRequest()
    )
    assert payload["status"] == RunStatus.INTERRUPTED.value
    assert payload["resumable"] is True
    assert payload["display_status"] == "processing"
    assert payload["progress"]["remaining_events"] > 0

    # 关闭注入后续跑成功
    settings.merge_fail_after_chunks = 0
    run = merge_run_service.get_run(db, payload["id"])
    resumed = run_controller.resume_run(db, payload["id"], _PRINCIPAL, _FakeRequest())
    assert resumed["status"] == RunStatus.SUCCESS.value
    assert resumed["progress"]["remaining_events"] == 0
    assert resumed["events_in"] == 400


def test_resume_terminal_run_raises_conflict(db, source_factory):
    table = _seed_table(db, source_factory, count=20)
    created = run_controller.create_run(
        db, {"table_id": table.id, "trigger": "manual"}, _PRINCIPAL, _FakeRequest()
    )
    run = merge_run_service.get_run(db, created["id"])
    with pytest.raises(AppError) as exc:
        merge_run_service.resume(db, run)
    assert exc.value.code == "RUN_NOT_RESUMABLE"
    assert exc.value.status_code == 409
