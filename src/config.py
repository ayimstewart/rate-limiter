"""Settings, read from environment variables (and a ``.env`` file if present)."""

from __future__ import annotations

from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from src.models import Algorithm


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    storage_backend: Literal["memory", "redis"] = "memory"
    redis_url: str = "redis://localhost:6379/0"
    default_algorithm: Algorithm = Algorithm.TOKEN_BUCKET
    log_level: str = "INFO"
    fail_open: bool = True
    redis_socket_timeout: float = Field(default=0.25, gt=0)
    redis_max_connections: int = Field(default=100, ge=1)

    @field_validator("log_level")
    @classmethod
    def _normalise_level(cls, value: str) -> str:
        level = value.upper()
        if level not in {"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"}:
            raise ValueError(f"unknown log level {value!r}")
        return level
