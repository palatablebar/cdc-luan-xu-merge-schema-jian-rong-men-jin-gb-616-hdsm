"""测试夹具：用 SQLite 文件库跑完整 ORM 流程（无需外部数据库）。"""
from __future__ import annotations

import os
import tempfile

import pytest

# 必须在导入应用配置前指定 sqlite，且在每个测试进程内只初始化一次
_TMP_DB = os.path.join(tempfile.gettempdir(), "cdcmerge_pytest.db")
if os.path.exists(_TMP_DB):
    os.remove(_TMP_DB)
os.environ.setdefault("DB_KIND", "sqlite")
os.environ.setdefault("DB_NAME", _TMP_DB)
os.environ.setdefault("SEED_DEMO", "false")
os.environ.setdefault("MERGE_CHUNK_DELAY_SECONDS", "0")


@pytest.fixture()
def db():
    from src.config.database import engine, session_scope
    from src.models import Base, CdcEvent, MergeRun, RejectRow, SourceTable  # noqa: F401

    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
    with session_scope() as session:
        yield session
    Base.metadata.drop_all(bind=engine)


@pytest.fixture()
def source_factory(db):
    """造源表 + CDC 事件的工厂。"""
    from src.models.cdc_event import CdcEvent
    from src.models.source_table import SourceTable

    def make(
        code: str = "T_X",
        *,
        events: list[dict] | None = None,
        schema_columns: list[dict] | None = None,
        lateness: int = 30,
    ):
        table = SourceTable(
            code=code,
            name=f"表 {code}",
            database_name="biz_prod",
            table_name=code.lower(),
            pk_column="id",
            schema_columns=schema_columns or [{"name": "status", "type": "varchar(16)", "nullable": True}],
            allowed_lateness_seconds=lateness,
            owner="test",
            enabled=True,
        )
        db.add(table)
        db.flush()
        for index, item in enumerate(events or [], start=1):
            db.add(
                CdcEvent(
                    table_id=table.id,
                    pk_value=str(item["pk"]),
                    lsn=int(item["lsn"]),
                    seq=index,
                    op=item.get("op", "u"),
                    event_time=item["event_time"],
                    ingest_at=item["ingest_at"],
                    before_data=item.get("before") or {},
                    after_data=item.get("after") or {},
                )
            )
        db.flush()
        return table

    return make
