"""Request/response schemas for the HTTP API."""

from __future__ import annotations

import enum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class Algorithm(enum.StrEnum):
    TOKEN_BUCKET = "token_bucket"
    SLIDING_WINDOW = "sliding_window"
    FIXED_WINDOW = "fixed_window"
    LEAKY_BUCKET = "leaky_bucket"


class CheckRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")  # a typo like "windows" should be a 422, not silently ignored

    key: str = Field(min_length=1, max_length=256, examples=["user:123"])
    algorithm: Algorithm | None = Field(
        default=None, description="Falls back to the service's DEFAULT_ALGORITHM when omitted."
    )
    limit: int = Field(gt=0, le=10_000_000, examples=[100], description="Requests allowed per window.")
    window: int = Field(gt=0, le=2_592_000, examples=[60], description="Window length in seconds (max 30 days).")


class CheckResponse(BaseModel):
    allowed: bool
    remaining: int = Field(ge=0)
    reset_at: int = Field(description="Unix time (seconds) at which this key's quota is fully restored.")
    limit: int
    algorithm: Algorithm
    retry_after: float = Field(description="Seconds until a request would be admitted; 0 when allowed.")
    delay: float | None = Field(
        default=None,
        description="leaky_bucket only: seconds to hold an admitted request to keep output at the drain rate.",
    )
    degraded: bool = Field(
        default=False,
        description="True when the storage backend failed and this answer is the fail-open default.",
    )


class HealthResponse(BaseModel):
    status: Literal["ok", "degraded"]
    storage: Literal["memory", "redis"]
    redis: Literal["connected", "disconnected", "not_configured"]
