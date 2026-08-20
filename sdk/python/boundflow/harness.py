"""Wire a durable harness to an operation, without hand-rolling the keys.

A harness only survives the operation ending if two keys are right: the checkpointer's
`thread_id`, which continues the conversation, and the store namespace, which is the
agent's filesystem. Both are per *task*, and both are easy to get wrong in a way nothing
reports — reuse a namespace and two tasks quietly share files; vary a `thread_id` between
rounds and the agent starts over with no error anywhere.

So they aren't the caller's to choose. `durable_harness` derives both from the operation,
opens the Postgres store and checkpointer, and hands back everything a harness needs:

    async with durable_harness(ctx, "operator", STORE_URL) as h:
        result = await ctx.run_governed(
            "operator",
            lambda model, tools: create_deep_agent(
                model=model, tools=tools, system_prompt=SYSTEM, **h.wiring
            ).ainvoke(h.first({"messages": [...]}), h.config),
            chat_model=ChatAnthropic(model=MODEL),
            tools=[...])

`h.wiring` is backend, checkpointer, and the policy translated into the harness's own
mechanisms — permissions and middleware. `h.config` carries the thread and the metering
callbacks. `h.first(payload)` is the payload for a fresh round, or the resume command if
the task is parked, so the same call serves both.

Everything here is deepagents-shaped and imports langgraph, which is why it's a separate
module: the rest of the SDK stays free of both.
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

from .capabilities import file_permissions
from .harness_callbacks import governed_tool_callbacks
from .harness_metering import metered
from .harness_middleware import harness_middleware


@dataclass
class DurableHarness:
    """The wiring for one governed, durable round. Built by `durable_harness`."""

    thread_id: str
    wiring: dict
    config: dict
    _resume: Any = None

    def first(self, payload: dict) -> Any:
        """The payload to invoke with: `payload` on a fresh task, or the parked
        interrupt's resume command when the operation is continuing one.

        Lets a handler open with the same line whether it's starting or resuming, which
        is the difference the caller most often forgets.
        """
        return self._resume if self._resume is not None else payload


@asynccontextmanager
async def durable_harness(ctx, agent_name: str, store_url: str, *, resume: Any = None):
    """Open the durable stores for this task and yield its wiring.

    `resume` is the decision from a gate — see `harness_gates` — and is what makes
    `h.first()` return a `Command` instead of a fresh message.
    """
    from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
    from langgraph.store.postgres.aio import AsyncPostgresStore
    from langgraph.types import Command
    from deepagents.backends import StoreBackend

    governor = ctx.agent_governor(agent_name)
    governor.register_harness_observer()

    # The task, not the operation: a resumed operation must land on the same thread and
    # the same filesystem as the one that parked.
    task_id = ctx.context.get("task_id") or ctx._op.request_id
    # The workflow *id*, not its type. Several workflows share a type — they're
    # instances of the same agent, each an entity with its own state — so keying on
    # type would interleave their namespaces under one prefix. Nothing collides
    # today, because task_id is unique, but deleting one instance could then only
    # be done by walking every task id and working out which belonged to whom.
    # Keyed on the id, an instance's state is a subtree you can drop.
    namespace = (ctx._op.workflow_id, agent_name, task_id)

    async with (
        AsyncPostgresStore.from_conn_string(store_url) as store,
        AsyncPostgresSaver.from_conn_string(store_url) as saver,
    ):
        await store.setup()
        await saver.setup()
        yield DurableHarness(
            thread_id=task_id,
            wiring={
                "backend": StoreBackend(namespace=lambda _rt: namespace, store=store),
                # Metered on the way through: the harness's own numbers are the
                # truth about spend, and they cover calls the governor never saw.
                "checkpointer": metered(saver, governor, ctx.report_metrics),
                # Policy, translated. Ours to declare and version, theirs to enforce.
                "permissions": file_permissions(governor.policy),
                "middleware": harness_middleware(governor),
            },
            config={
                "configurable": {"thread_id": task_id},
                # Metering rides callbacks so it reaches subagents, which a parent's
                # middleware never sees.
                "callbacks": [governed_tool_callbacks(governor)],
            },
            _resume=Command(resume=resume) if resume is not None else None,
        )


class UngovernedModel(ValueError):
    """A subagent was configured with a model BoundFlow can't govern."""


def validate_subagents(specs) -> list:
    """Reject subagents that name their model as a string. Returns the specs.

        subagents=validate_subagents([RESEARCHER, SCRIBE])

    A spec inherits the parent's model *object* — the governed one — unless it names a
    model itself, in which case the harness builds its own client. That client never
    reaches the governor: its calls aren't capped, aren't priced, and don't exist in
    any metric until the checkpoint is read afterwards. Invisible money, and the only
    symptom is a cost limit that quietly doesn't apply.

    So it raises rather than warns. Omit `model` to inherit, or pass a model object if
    the subagent genuinely needs a different one — `ctx.agent_model()` returns a
    governed one.
    """
    for spec in specs:
        model = spec.get("model") if isinstance(spec, dict) else getattr(spec, "model", None)
        if isinstance(model, str):
            name = (spec.get("name") if isinstance(spec, dict) else None) or "<unnamed>"
            raise UngovernedModel(
                f"subagent {name!r} names its model as a string ({model!r}), which builds "
                "a client BoundFlow can't see: its calls are uncapped, unpriced and "
                "unmetered. Omit 'model' to inherit the governed one, or pass a model "
                "object.")
    return list(specs)


def task_context(ctx, extra: dict | None = None) -> dict:
    """Context for the next operation, carrying the task identity forward.

        return AwaitApproval(on_approve=Next("resume", context=task_context(ctx, {...})))

    Without this the resumed operation gets a new `request_id`, derives a different
    thread and namespace, and the agent silently starts from nothing.
    """
    return {"task_id": ctx.context.get("task_id") or ctx._op.request_id, **(extra or {})}
