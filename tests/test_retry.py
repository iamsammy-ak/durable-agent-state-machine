from __future__ import annotations

import random

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from pydantic import ValidationError

from durable_agent import NonRetryableError, RetryPolicy
from durable_agent.retry import stable_rng

policies = st.builds(
    lambda initial, coef, extra, attempts: RetryPolicy(
        initial_interval=initial, backoff_coefficient=coef, max_interval=initial + extra, max_attempts=attempts
    ),
    initial=st.floats(0, 10, allow_nan=False),
    coef=st.floats(1, 10, allow_nan=False),
    extra=st.floats(0, 100, allow_nan=False),
    attempts=st.integers(1, 20),
)


def test_ceiling_doubles_then_caps() -> None:
    policy = RetryPolicy(initial_interval=0.5, backoff_coefficient=2, max_interval=3)
    assert [policy.ceiling(n) for n in range(1, 6)] == [0.5, 1.0, 2.0, 3.0, 3.0]


@settings(max_examples=60)
@given(policy=policies, attempt=st.integers(1, 10_000), seed=st.integers(0, 2**32))
def test_full_jitter_delay_stays_within_bounds(policy: RetryPolicy, attempt: int, seed: int) -> None:
    delay = policy.delay(attempt, random.Random(seed))
    assert 0.0 <= delay <= policy.ceiling(attempt) <= policy.max_interval


@settings(max_examples=60)
@given(policy=policies, attempt=st.integers(1, 500))
def test_ceiling_never_decreases(policy: RetryPolicy, attempt: int) -> None:
    assert policy.ceiling(attempt) <= policy.ceiling(attempt + 1)


def test_no_jitter_returns_the_ceiling_and_huge_attempts_do_not_overflow() -> None:
    policy = RetryPolicy(initial_interval=1, backoff_coefficient=10, max_interval=60, jitter="none")
    assert policy.delay(3, random.Random(0)) == 60
    assert policy.ceiling(10**9) == 60


def test_attempt_numbers_start_at_one() -> None:
    with pytest.raises(ValueError):
        RetryPolicy().ceiling(0)


def test_max_interval_must_not_be_below_initial_interval() -> None:
    with pytest.raises(ValidationError):
        RetryPolicy(initial_interval=5, max_interval=1)


class TransientError(ConnectionError):
    pass


def test_error_classification_by_type_name_and_base_class() -> None:
    policy = RetryPolicy(non_retryable=("PermissionError", "ConnectionError"))
    assert policy.is_retryable(TimeoutError("slow"))
    assert not policy.is_retryable(PermissionError("denied"))
    assert not policy.is_retryable(TransientError("subclass of a listed base"))
    assert not RetryPolicy().is_retryable(NonRetryableError("always permanent"))
    assert RetryPolicy().is_retryable_name("ActivityTimeout")
    assert not RetryPolicy(non_retryable=("ActivityTimeout",)).is_retryable_name("ActivityTimeout")


def test_stable_rng_is_reproducible_and_keyed() -> None:
    assert stable_rng("run", "step#1", 1).random() == stable_rng("run", "step#1", 1).random()
    assert stable_rng("run", "step#1", 1).random() != stable_rng("run", "step#1", 2).random()
