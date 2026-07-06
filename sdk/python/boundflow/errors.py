"""Typed exceptions for the BoundFlow SDK.

The SDK talks to the control plane over gRPC, but callers shouldn't have to import
`grpc` or reason about status codes. Every control-plane call raises a
`BoundflowError` subclass instead of a raw `grpc.aio.AioRpcError`, so you can write
`except boundflow.NotFoundError` without knowing the wire protocol.
"""
from __future__ import annotations

import grpc


class BoundflowError(Exception):
    """Base class for all BoundFlow SDK errors.

    `message` is the server-supplied detail; `code` is the underlying gRPC status
    name (e.g. "FAILED_PRECONDITION"), kept for reference. The original transport
    exception is chained as `__cause__`.
    """

    def __init__(self, message: str, *, code: str = "", cause: Exception | None = None):
        super().__init__(message)
        self.message = message
        self.code = code
        if cause is not None:
            self.__cause__ = cause


class PlatformError(Exception):
    """A platform-level failure raised from inside a worker's handler or LLM client.

    Unlike a customer callback that raises (a customer-domain failure that completes
    the run and keeps the workflow active), this signals the run couldn't be governed
    at all — e.g. the LLM provider reported no token usage, so cost caps can't be
    enforced. The worker lets it propagate so the operation is reported as *failed*,
    interrupting the workflow; its message becomes the request's `failure_reason`.
    """


class NotFoundError(BoundflowError):
    """The referenced resource does not exist (or isn't visible to this API key)."""


class AlreadyExistsError(BoundflowError):
    """A resource with the same identity already exists."""


class InvalidArgumentError(BoundflowError):
    """The request was malformed or violated a validation rule."""


class FailedPreconditionError(BoundflowError):
    """The resource isn't in a state that permits the operation — e.g. resolving a
    workflow that isn't interrupted."""


class PermissionDeniedError(BoundflowError):
    """The API key isn't allowed to perform this operation."""


class UnauthenticatedError(BoundflowError):
    """Missing or invalid API key."""


class UnavailableError(BoundflowError):
    """The control plane is unreachable or not ready."""


class DeadlineExceededError(BoundflowError):
    """The call exceeded its deadline."""


_STATUS_MAP = {
    grpc.StatusCode.NOT_FOUND: NotFoundError,
    grpc.StatusCode.ALREADY_EXISTS: AlreadyExistsError,
    grpc.StatusCode.INVALID_ARGUMENT: InvalidArgumentError,
    grpc.StatusCode.FAILED_PRECONDITION: FailedPreconditionError,
    grpc.StatusCode.PERMISSION_DENIED: PermissionDeniedError,
    grpc.StatusCode.UNAUTHENTICATED: UnauthenticatedError,
    grpc.StatusCode.UNAVAILABLE: UnavailableError,
    grpc.StatusCode.DEADLINE_EXCEEDED: DeadlineExceededError,
}


def from_rpc_error(exc: grpc.aio.AioRpcError) -> BoundflowError:
    """Translate a gRPC error into the matching BoundflowError subclass."""
    code = exc.code()
    cls = _STATUS_MAP.get(code, BoundflowError)
    message = exc.details() or (code.name if code else "unknown error")
    return cls(message, code=code.name if code else "", cause=exc)
