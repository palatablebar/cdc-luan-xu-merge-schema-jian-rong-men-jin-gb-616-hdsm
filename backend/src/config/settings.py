"""集中配置：全部来自环境变量，默认值与 .env.example 保持一致。"""
from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", case_sensitive=False)

    app_name: str = "cdcmerge"
    app_title: str = "CDC 乱序合并与 Schema 兼容门禁"

    db_kind: str = "postgres"  # postgres | mysql
    db_host: str = "localhost"
    db_port: int = 5432
    db_name: str = "cdcmerge"
    db_user: str = "cdcmerge"
    db_password: str = "cdcmerge_pw_2026"

    jwt_secret: str = "cdcmerge-jwt-secret-change-me-2026"
    jwt_algorithm: str = "HS256"
    access_token_minutes: int = 480

    redis_url: str = "redis://localhost:6379/0"
    seed_demo: bool = True
    rate_limit_per_minute: int = 300
    rate_limit_write_per_minute: int = 60
    app_timezone_offset_minutes: int = 480

    # —— 断点续跑 ——
    # 每个分块处理的事件数：处理完一个分块刷新心跳与进度并落一次库。
    merge_chunk_size: int = 200
    # 分块之间的停顿秒数（演示用，让“处理中/剩余数量”可见；生产置 0）。
    merge_chunk_delay_seconds: float = 0.05
    # running 心跳超过该秒数没有刷新即判定为中断（interrupted）。
    run_stale_seconds: int = 60
    # 故障注入：在第 N 个事件分块处理完后主动抛出异常（仅测试/演示，0 表示关闭）。
    merge_fail_after_chunks: int = 0
    # 故障注入：累计确认事件数达到该值、且还有未确认表时中断（精确模拟表间中断，0 表示关闭）。
    merge_fail_after_events: int = 0

    @property
    def database_url(self) -> str:
        if self.db_kind == "sqlite":
            # 仅用于本地无依赖测试/CI：db_name 直接作为 sqlite 文件路径（:memory: 亦可）
            return f"sqlite:///{self.db_name}"
        if self.db_kind == "mysql":
            return (
                f"mysql+pymysql://{self.db_user}:{self.db_password}"
                f"@{self.db_host}:{self.db_port}/{self.db_name}?charset=utf8mb4"
            )
        return (
            f"postgresql+psycopg2://{self.db_user}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
