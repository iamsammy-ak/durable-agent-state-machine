"""Exception types shared by the runtime."""

from __future__ import annotations


class DurableAgentError(Exception):
    """Base class for runtime errors."""


class WorkflowDefinitionError(DurableAgentError):
    """The workflow definition is inconsistent (unknown states, missing transitions, ...)."""


class TransitionError(DurableAgentError):
    """No transition out of the current state matched at run time."""


class LeaseLost(DurableAgentError):
    """The worker no longer holds the run's lease: its fencing token is stale, so its writes are refused."""


class RunNotFound(DurableAgentError):
    """No run with this id exists."""


class RunFinished(DurableAgentError):
    """The run already reached a terminal state."""


class SignalRejected(DurableAgentError):
    """The signal name is unknown to the workflow or its payload is invalid."""


class ReplayError(DurableAgentError):
    """The journal cannot be folded into a consistent run state."""


class NonRetryableError(Exception):
    """Raise from an activity to fail the step immediately, without further attempts."""


class InvalidResult(NonRetryableError):
    """An activity returned a value that does not fit the workflow's state model."""


class ActivityTimeout(Exception):
    """An attempt ran longer than its per-attempt timeout (recorded as the failure type)."""
