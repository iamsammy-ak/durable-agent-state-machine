"""Retry policies: exponential backoff with full jitter, attempt limits and error classification."""

from __future__ import annotations

import hashlib
import math
import random
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .errors import NonRetryableError


class RetryPolicy(BaseModel):
    """How a failed activity attempt is retried.

    The delay after failed attempt ``n`` (1-based) is drawn uniformly from ``[0, cap]`` where
    ``cap = min(max_interval, initial_interval * backoff_coefficient ** (n - 1))`` ("full jitter").
    Randomising the whole interval spreads retries from many runs apart, so a recovering
    dependency is not hit by synchronised waves of retries.
    """

    model_config = ConfigDict(frozen=True)

    max_attempts: int = Field(default=5, ge=1, description="Attempts in total, including the first one.")
    initial_interval: float = Field(default=0.5, ge=0, description="Backoff ceiling after the first failure (s).")
    backoff_coefficient: float = Field(default=2.0, ge=1.0, description="Growth factor of the ceiling per attempt.")
    max_interval: float = Field(default=30.0, ge=0, description="Upper bound on any single delay (s).")
    jitter: Literal["full", "none"] = "full"
    non_retryable: tuple[str, ...] = Field(
        default=(), description="Exception class names that fail the step at once (subclasses match too)."
    )

    @model_validator(mode="after")
    def _interval_order(self) -> RetryPolicy:
        if self.max_interval < self.initial_interval:
            raise ValueError("max_interval must be >= initial_interval")
        return self

    def ceiling(self, attempt: int) -> float:
        """Largest delay that may follow failed attempt ``attempt``."""
        if attempt < 1:
            raise ValueError("attempt numbers start at 1")
        if self.initial_interval == 0:
            return 0.0
        exponent = attempt - 1
        # Compare in log space so huge attempt numbers cannot overflow a float.
        if exponent * math.log(self.backoff_coefficient) + math.log(self.initial_interval) >= math.log(
            max(self.max_interval, 1e-300)
        ):
            return self.max_interval
        return min(self.max_interval, self.initial_interval * self.backoff_coefficient**exponent)

    def delay(self, attempt: int, rng: random.Random) -> float:
        """Delay before the attempt that follows failed attempt ``attempt``."""
        cap = self.ceiling(attempt)
        return rng.uniform(0.0, cap) if self.jitter == "full" else cap

    def is_retryable(self, error: BaseException) -> bool:
        """False for :class:`NonRetryableError` and for any class (or base class) listed in ``non_retryable``."""
        if isinstance(error, NonRetryableError):
            return False
        return self.is_retryable_name(*(cls.__name__ for cls in type(error).__mro__))

    def is_retryable_name(self, *type_names: str) -> bool:
        """Classify a failure by type name (used for timeouts, which have no exception instance)."""
        return not set(type_names) & set(self.non_retryable)


def stable_rng(*parts: object) -> random.Random:
    """A random generator seeded from ``parts``: identical across processes (unlike ``hash()``)."""
    digest = hashlib.sha256("\x1f".join(str(p) for p in parts).encode("utf-8")).digest()
    return random.Random(int.from_bytes(digest[:8], "big"))
