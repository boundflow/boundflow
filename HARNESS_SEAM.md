# Harness seam — BoundFlow work orders

Handoff note. Read against **deepagents 0.7.7** and **`feat/one-enforcement-core`**.
Source of the analysis: deepagents `graph.py` / `middleware/` / `backends/`, BoundFlow
`governed.py` / `langchain_client.py` / `worker.py`, and
`langchain/agents/middleware/human_in_the_loop.py`.

Context: Charter is moving to run someone else's agent harness (deepagents first) inside
a BoundFlow operation, via `ctx.run_governed`. This lists what the **control plane** needs
to build. Charter-side work is at the bottom for context only.

## The rule

Settled, and it decides every ambiguous case:

| Ours — substrate and lifetime | Theirs — what the agent does |
| --- | --- |
| Where files live, and whether they survive | Prompt assembly |
| Persistence across rounds and machines | Context management and compaction |
| Identity, versioning, rollback | Memory |
| Budget and enforcement | Skills and progressive disclosure |
| Durable human gates | Tool choice and planning |
| Scheduling and waiting | Subagent decisions |

Test for anything unclear: **would a customer using the harness directly get the same
behavior?** If yes, we're transport and it's fine. If we'd change what the agent does,
it's theirs.

## What already works, and needs nothing

- **Model calls.** `GovernedChatModel` enforces model / `max_tokens_per_call` /
  `max_call_seconds`, meters cost and calls, and records the full prompt per call.
- **Declared tools.** `ctx.agent_tools()` makes `tool_call_limits` and failure counts
  enforceable.
- **Lifecycle policy, entirely.** `_flush_governors()` writes each governor's snapshot to
  `ctx.agent_state_updates`, and `internal/metrics/metricshandler.go:60` builds the
  workflow snapshot as the sum across all agents. `cost`, `num_llm_calls`, `latency` and
  `approval_rejections` roll up from a governed harness run exactly as they do today.
- **Durable waits.** `Next.delay_seconds` writes `dispatch_at`
  (`internal/storage/postgres/job.go:497`) and both the claim query and the scheduler
  honour it. Charter just doesn't use it yet.

~~`max_llm_calls` / `max_cost_usd` let the crossing call complete.~~ **Stale — fixed.**
That was issue #66, closed as completed 2026-08-13. Main now reads
`llm_calls >= max_llm_calls - 1`, so the forced `submit_result` lands inside the budget.

## BF-0 — Govern harness-injected tools — **built, and smaller than it looked**

**Status: done on `exp/deepagents-harness`, but the shape changed.**

The problem was real: `tool_failures` and `tool_call_limits` read state populated only for
tools BoundFlow dispatched, so a rule targeting a harness-injected tool silently never
fired — and under this architecture the harness injects *more* tools, so the gap widens.

What changed is the answer. The original plan was one BoundFlow middleware doing
everything — counting, capping, gating. Building it revealed that deepagents already ships
three of the four, better than we would:

| Concern | Owner | Mechanism |
|---|---|---|
| Which tools exist | us | declaration |
| Per-action allow / deny / interrupt | deepagents | `permissions=` (filesystem only) |
| Counting and capping tool calls | deepagents | `ToolCallLimitMiddleware` |
| Observing calls and how they ended | LangChain | tool callbacks |
| **Default-deny allowlist** | **us** | `tool_allowlist_middleware` |
| Where policy comes from (versioned, central) | us | `RuntimePolicy` |
| Durable waiting on an interrupt | us | `harness_gates.py`, below |
| Metrics across runs → pause / cooldown / rollback | us | lifecycle rules |

So the built thing is three small pieces, not one big one:

- **`harness_callbacks.py`** — metering, via LangChain's `on_tool_start` / `on_tool_end` /
  `on_tool_error`. This is the surface built for the purpose (`AgentMiddleware` has no
  read-only tool hook; `wrap_tool_call` is a control hook), and it is the only one that
  **reaches subagents**: deepagents compiles a `task` subagent with its own middleware
  list, so parent middleware never sees its tool calls, while callbacks ride the runtime
  config down — deepagents says so itself in `middleware/subagents.py`. For a metering
  product, a `task` call spending an unmetered budget is the failure that matters.
- **`harness_middleware.py`** — `tool_allowlist_middleware`, and nothing else that
  enforces. Default-deny is the one real gap: `permissions=` covers filesystem actions
  only, `interrupt_on` can pause a call but not refuse it. Also `harness_call_limits`,
  which builds *their* `ToolCallLimitMiddleware` from *our* policy — ours to decide,
  theirs to enforce.
- **`governor.register_harness_observer()`** — stands the request-side inference down so
  the same call isn't counted twice.

**The hole worth knowing.** Caps and allowlists name *tools*, and a harness ships several
tools per capability. Capping `write_file` at 1 was enforced correctly and the agent
promptly used `edit_file` to achieve the same thing. That's not a bug in the cap; it's
what naming tools instead of capabilities buys. Either enumerate the whole capability or
sell this as metering, not containment.

**Verified**: caller middleware composes outside deepagents' own stack (first in list is
outermost); a cap on `write_file`, a tool BoundFlow never declared, produced
`REFUSAL "Call limit reached for 'write_file' (max 1)"`; `calls_per_tool` after a live
run showed `{'restart_database': 1, 'write_file': 1}` — the declared tool counted once by
its own wrapper, the harness tool once by the callback, no double count.

---

## BF-0b — Durable approval gates for harness interrupts — **built**

**Status: done on `exp/deepagents-harness`. The cleanest example of the rule.**

deepagents already decides *what* needs a human: `interrupt_on={"tool": True}` for any
tool, `permissions(mode="interrupt")` for files. What it cannot do is *wait*. Its
`HumanInTheLoopMiddleware` raises a LangGraph interrupt, so the process holds the pause —
close the laptop and it's gone.

That's the whole seam in one sentence: **the harness names the moment, BoundFlow owns the
waiting.** We add nothing to the decision and everything to its lifetime.

`harness_gates.py` is the bridge, and it's tiny — `pending_action(result)` pulls the
parked request out of `__interrupt__`, and `approve()` / `reject()` / `respond()` /
`edit()` build the resume payloads. The workflow returns `AwaitApproval(on_approve=Next(
"resume", context={"decision": approve()}))`; the resume operation feeds it back with
`Command(resume=...)`.

**Verified** end-to-end against the local stack: the agent asked to run
`restart_database({'name': 'db-prod-1'})`, the operation parked, a human approved through
the control plane, and a **separate operation** resumed it to completion — sharing only
the checkpointer, on a freshly built agent instance. Two rounds, $0.0115 then $0.0111, and
`/notes.md` survived in Postgres across both.

The correct resume shape is `Command(resume={"decisions": [{"type": "approve"}]})` — a
bare list raises `TypeError: list indices must be integers`.

**Known limit:** one gate at a time. `jobs.workflow_id` is a primary key, so a turn
proposing several actions surfaces only the first; the rest are decided on later rounds.

---

## BF-1 — Land the governed-model branches on main

**Status: ready. Blocks everything else.**

`feat/one-enforcement-core` is exactly one commit on top of `feat/governed-model`, and
sits 2 commits behind main.

1. Merge `feat/governed-model`.
2. Rebase `feat/one-enforcement-core` on main, merge that.

**Done when**

- `ctx.run_governed`, `ctx.agent_model`, `ctx.agent_tools` importable from a released SDK
- `sdk/python/tests/test_governed_model.py` passes on main

---

## BF-2 — Durable harness state, owned by the customer, governed by us

**Status: much smaller than first written. Not on the critical path.**

> **Revised.** The original version of this item said "a durable artifact store,
> addressable over RPC," built and hosted by BoundFlow. That was wrong on two counts and
> is superseded by what follows.

### What the harness actually needs

Two different things, often conflated:

- **Checkpointer** (`BaseCheckpointSaver`) — LangGraph graph state. The message list.
  This is the conversation.
- **Store** (`BaseStore`, surfaced as a filesystem by `StoreBackend`) — the agent's files:
  scratch, whatever compaction evicted from context, skills, and per-agent memory.

Today the filesystem defaults to `StateBackend`, which lives inside LangGraph state and
dies at the end of each invocation — every Charter round. Nothing survives a gate.

### Why we don't host it

**It would break the central claim.** BoundFlow's pitch is that the backend never sees a
prompt and never pays for a token. The bytes in that store are prompts, agent reasoning,
tool outputs, and whatever customer data the agent touched. Holding them makes the
governance product a reader of the data it governs.

**And it puts the control plane in the filesystem hot path.** Every `read`/`write`/`ls`
becomes a gRPC round trip. Latency and scale both suffer, for data we shouldn't have.

### The split

Operational state is ours. Application state is theirs.

| BoundFlow owns | Customer owns |
| --- | --- |
| Which namespace a task's state lives in | The bytes in it |
| When that state may be discarded | Where it's stored |
| That state exists and is reachable on resume | Access to it |
| Version, budget, gates, scheduling | Files, conversation, memory |

The agent config declares a store connection; the **worker** connects to it directly. We
track namespace and lifetime and never hold a byte. Workers already carry
`ANTHROPIC_API_KEY` and every MCP credential, so a store connection is the same kind of
data-plane configuration and belongs in the same place.

The self-hosting default is the Postgres they already run for BoundFlow — one connection
string, no new infrastructure. A customer who wants their own store points at it instead.

### Why this is small

`BaseStore` has exactly two abstract methods, `batch` and `abatch`. Everything else is
built on them in the base class. And `BackendProtocol` implements `ls`, `glob` and `grep`
itself, in pure Python — verified: `als`/`aglob`/`agrep` resolve to `BackendProtocol`,
while `StoreBackend` supplies only `aread`/`awrite`/`aedit`/`adelete`. So the filesystem
comes free from a store, and grep and glob never touch anything we build.

`langgraph-checkpoint-postgres` (3.1.2) already ships `AsyncPostgresStore` and
`AsyncPostgresSaver`. For the default path there may be nothing to implement at all.

Per-task versus per-agent is a **namespace prefix**, not two systems: `(tenant, agent,
task)` for scratch, `(tenant, agent)` for memory. Per-agent stays **opt-in** —
`MemoryMiddleware` isn't installed unless `memory=` is passed (`graph.py:861`), and both
`checkpointer` and `store` default to `None`. An agent that doesn't ask for memory keeps
clean rollback, because there's nothing else to restore.

### Quotas without seeing the data

Storage is the one runaway dimension currently ungoverned — cost, calls, tokens, latency
and tool calls are all capped, but an agent looping on file writes trips none of them.

Enforce it the way cost is already enforced: the **worker meters and reports usage
metadata**, the **control plane holds the policy and decides**. Same shape as
`max_cost_usd`, and no bytes cross the boundary. Add `max_storage_mb` to the runtime
policy family.

### Done when

- an agent declares a store; the worker connects to it and a task resumed on a different
  worker sees identical contents
- namespaces are `(tenant, agent, task)` / `(tenant, agent)`, and BoundFlow tells the
  worker when a task namespace may be dropped
- `max_storage_mb` is enforced from reported usage, with a distinguishable error
- deleting an agent instructs cleanup of its namespaces

**Note:** `execute` is the only harness tool needing `SandboxBackendProtocol`. Plain
`BackendProtocol` covers `ls`/`read`/`write`/`edit`/`grep`/`glob`/`delete`, and without a
sandbox `execute` returns "not available" — today's default, so nothing regresses. Shell
is a separate, later decision. Never `LocalShellBackend`; it documents itself as "NO
sandboxing."

**Not blocking.** `StateBackend` is the default and works now. The whole integration —
governed model, budgets, lifecycle policy, approval gates — can be proven end to end
before any of this exists. This is what makes context survive a round, not what makes the
harness run.

---

## BF-3 — Decide where graph checkpoints live, and bound them

**Status: policy + plumbing.**

Charter will implement `BaseCheckpointSaver`, buffering LangGraph's per-super-step `aput`
calls and flushing the latest at operation end — the operation is BoundFlow's unit of
recovery, so finer granularity would never be read.

The BoundFlow question is where the bytes go. Job context is the obvious home, but
`InvokeWorkflowRequest.initial_context` is documented as *"Persisted on the request; keep
it small (coordination/input data, not a datastore)"* and a checkpoint carrying a
compacted message list will test that. Either raise and enforce an explicit ceiling on job
context, or route checkpoints to BF-2 and keep job context for coordination only. The
second is probably right.

**Done when**

- a documented size limit on job context, enforced server-side with a clear error
- a decision recorded on checkpoint storage, with a path that doesn't grow a Postgres row
  unboundedly

---

## BF-4 — Bound and document `delay_seconds`

**Status: small, but promoted — BF-5 now rests on it.**

Durable waits already work. Nothing in the paths reviewed validates an upper bound. Before
Charter starts generating multi-day sleeps, confirm whether one exists and add it if not.

This is no longer a nice-to-have. With BF-5 withdrawn, `delay_seconds` is the primitive
that carries every wait Charter generates: the single-wait case, the parent multiplexing
concurrent timers to the nearest deadline, and the poll loop over waiters. It is the most
load-bearing thing on this list that nobody has looked at closely.

**Done when**

- a maximum is defined and rejected at the API boundary, not silently accepted
- the relationship to `timeout_seconds` is documented — they're independent columns
  (`dispatch_at` vs `timeout_seconds`), so a 7-day delay with a 20-minute operation
  timeout is legitimate, and that should be stated rather than inferred

---

## BF-5 — Durable spawn — **withdrawn**

**Status: not a BoundFlow work order. Moved to Charter, and shrunk to almost nothing.**

> **Revised.** The original item asked for a spawn primitive on `OperationContext` plus a
> parent/child relationship in the data model, and estimated weeks. Both the requirement
> and the estimate were wrong. Nothing here needs building in BoundFlow.

### Subagents already work

The `task` tool calls `await subagent.ainvoke(...)`, in-process, and the subagent inherits
the model and tools passed to `create_deep_agent`. Hand it a `GovernedChatModel`
*instance* and every subagent call is metered, capped and traced by the same governor as
the parent. Reasoning, tool use, concurrency and budget are all covered today.

### Durability is only ever about waiting

If a subagent runs for thirty seconds and the worker dies, the operation retries and
nothing is lost. Process death only costs you something **if you were waiting**. So
durability isn't a separate benefit that child workflows unlock — it is the one thing
waiting requires, and it's the only thing.

That collapses the design. A durable "subagent" doesn't need the harness, a model, tools,
or state. State lives customer-side (BF-2), so there's nothing to hand it anyway. It needs
to be a **waiter**:

```
op: wait  →  AwaitApproval(on_approve=Complete(result={...}),
                           on_reject =Complete(result={...}))
```

```
op: wait   →  Next("finish", delay_seconds=600)
op: finish →  Complete(result={...})
```

One generic `charter-waiter` workflow type, parameterised by `initial_context`. Roughly a
hundred lines, no budget story, no state story, no harness inside it. And because it makes
no LLM calls, the question of how to split a budget across parallel children doesn't
arise.

### Children only buy *concurrent* waits

A workflow has one job row (`workflow_id TEXT PRIMARY KEY` on `jobs`) and can hold one
gate at a time. That is the entire reason to spawn anything.

- **One wait** → the parent does it itself with `AwaitApproval` or `delay_seconds`. No
  children.
- **Concurrent timers** → still no children. The parent computes the nearest deadline,
  sleeps to it, wakes, handles what's ready, sleeps to the next. Arithmetic, not
  orchestration.
- **Concurrent gates** — two approvals sitting with two different people — → one waiter
  each, because there's no deadline to compute and the parent can't hold both.

Only the third case justifies a child.

### Charter-side, using RPCs that already exist

Per waiter: `create_workflow(type="charter-waiter")` → `activate_workflow` →
`set_workflow_lifecycle_policy` (pre-arm a ceiling so an orphan can't run away) →
`invoke_workflow(initial_context=…, runtime_overrides=…)`. The parent then polls
`get_request_info(request_id).result` on a `delay_seconds` loop and calls
`delete_workflow` when done.

Two details that make this work:

- **Children share one `workflow_type`.** Worker routing is by `(workflow_type,
  workflow_version)` in the claim query, so every Charter worker serves `charter-waiter`
  and a dynamically created instance is immediately schedulable. Instances are dynamic;
  routing stays static. No deploy per child.
- **`list_workflows` pollution isn't a problem.** Customers use Charter's CLI, so
  `charter agents` filters waiters out and `charter describe` can nest them under the
  parent.

Charter owns two obligations that come with this: **spawn draws from a pre-declared
pool or a fixed type** — an agent able to call `create_workflow` freely can mint unbounded
workflows, which is authority nobody granted it — and **orphan GC**, since a parent that
dies between create and delete leaves a workflow nobody will clean up. A pre-armed
lifecycle policy bounds an orphan's spend but not its existence, so a TTL sweeper belongs
in the design from day one.

### What BoundFlow could add later, if it earns it

Not spawn. **Run lineage** — letting an invoke declare a parent request id — would buy two
things:

1. Roll child metrics into the parent's workflow snapshot. Today metrics are per-workflow,
   so a parent's `cost` and `num_llm_calls` exclude its children and lifecycle rules on
   the parent go blind to them.
2. Cascade cancel and pause down the tree.

Both have Charter-side workarounds. A pre-armed lifecycle policy on each child enforces a
ceiling without the parent being alive, which is better than cascade. And for aggregation,
`metricshandler.go` creates an `AgentState` for any agent name it hasn't seen without
validating it, so a parent can read `get_workflow_metrics` on its children and write their
cost into its own `ctx.agent_state_updates` under e.g. `waiter:approval-3`. Cost then
double-counts at tenant level — wrong for billing, right for governance. *Unverified:
inferred from the aggregation loop, worth a spike before relying on it.*

So lineage is an accuracy and ergonomics improvement, not a correctness hole. Let real
usage ask for it.

---

## BF-6 — Scope machine callers for `SubmitInput`

**Status: optional.**

Not needed for the harness. It's the cheapest possible event primitive: `AwaitInput`
already parks until `SubmitInput` is called, and `pending_input.metadata` is published to
external readers, so a watcher can correlate and resolve the right gate. What's missing is
naming and an auth scope narrower than a human's.

**Done when**

- a credential can resolve input gates without carrying full tenant authority
- audit distinguishes a machine-submitted answer from a human one

---

## Surprises worth knowing

- **A string model silently escapes governance.** A subagent spec naming its model as a
  string makes `resolve_model()` build a fresh client — never touches the governor, the
  budget, or cost. Charter will reject this at validate time.
- **The backend is a privilege escalation switch.** If `backend` is ever exposed in agent
  config it needs treating as one.
- **The harness version becomes part of the agent version.** Once state persists across
  rounds, a checkpoint written by 0.7.7 may not deserialize under 0.8 — so a library
  upgrade can break in-flight tasks sitting mid-approval, where today it's just a worker
  deploy.
- **Compaction spends the task's budget.** Summarization is in deepagents' *default*
  middleware stack and makes model calls. Governed, which is correct, but context
  management now competes with the work for `max_llm_calls` and `max_cost_usd`.
- **Per-agent memory weakens rollback** — but only for agents that opted into it.
- **Middleware doesn't reach subagents; callbacks do.** deepagents compiles each `task`
  subagent with its own middleware list. Anything that must see the whole run — metering
  above all — has to ride the callback config instead.
- **We nearly rebuilt four things deepagents already had**: a permissioned filesystem
  backend, tool-call capping, interrupt logic, and tool observation. Read the harness
  before writing the adapter; the seam is narrower than it first looks, which is the
  point.

## Charter-side, for context only

Not BoundFlow work. Listed so the dependencies are clear.

Mostly deletion: `memory:` and `AuditMemory` come out of the config, `instructions:`
becomes `skills:` (files we version and ship, not prose we assemble), `add_context`
staging comes out of `loop.py`, and once BF-3 lands so does `_history`. Rejection feedback
becomes a `ToolMessage` matching the harness's own shape — see
`human_in_the_loop.py:338`, `"User rejected the tool call for ..."`.

`_history` and `memory.from_audit` are both workarounds for the missing checkpointer.
Land persistence and they delete themselves.

New, from BF-5's withdrawal: a `charter-waiter` workflow type, the spawn/poll/reap loop
around it, and a TTL sweeper for orphans. Plus the rule that only concurrent *gates*
justify a waiter — single waits and concurrent timers are handled by the parent with
`delay_seconds`.

## Unverified

- ~~Whether re-invoking a checkpointed LangGraph thread seeds cleanly across our round
  boundary.~~ **Verified.** 12 messages spanning two operations, reconstructed with
  `aget_state`. Note checkpoints are stored as *deltas*, not snapshots — querying the
  table directly shows an empty conversation and means nothing; use the graph API.
- Whether anything currently bounds `delay_seconds` — hence BF-4.
- Whether a parent can attribute a child's cost to itself by writing an unrecognised agent
  name into `ctx.agent_state_updates`. Inferred from the aggregation loop in
  `metricshandler.go`, not tested. BF-5's fallback for metric roll-up depends on it.
- deepagents middleware defaults move fast; re-verify before building against them.
