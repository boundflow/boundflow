"""The operator console against the live stack.

The sibling suite (tests/test_ui_console.py) drives the console against a fake
control plane — fast, and right for the console's own routing and rendering. These
prove the same actions against the real server the CLI tests use, so a fake that has
drifted from the real client can't hide a break.

Scope mirrors the CLI suite for every command the console exposes: approve, reject,
submit-input, suspend, resume, resolve, delete, plus the read views. The CLI's
create / set-config / policy / pricing / tenant / audit commands have no counterpart
here on purpose — the console doesn't do authoring.
"""
from __future__ import annotations

import asyncio

import pytest

from boundflow import (
    AwaitApproval,
    AwaitInput,
    BoundFlowWorker,
    Complete,
    InvokeMode,
    LifecycleState,
    Next,
    RunStatus,
    WorkflowConfig,
    WorkflowState,
)

from .conftest import (
    WORKER_ADDRESS,
    console_client,
    control_plane,
    dummy_mock,
    run_worker,
    unique,
    wait_for,
    wait_lifecycle,
)

pytestmark = pytest.mark.asyncio


async def _tenant_and_workflow(cp, wtype: str, *, activate: bool = True):
    tenant = await cp.create_tenant(unique("ui"))
    wf = await cp.create_workflow(wtype, tenant.id, config=WorkflowConfig(version=1))
    if activate:
        await cp.activate_workflow(wf.id)
    return wf


# ── Read views ───────────────────────────────────────────────────────────────

async def test_fleet_lists_a_real_workflow(console, boundflow_api_key):
    async with control_plane(boundflow_api_key) as cp:
        wf = await _tenant_and_workflow(cp, unique("ui_fleet"))
        async with console_client(console) as c:
            body = (await c.get("/")).text
    assert wf.id in body
    assert "Fleet" in body


async def test_fleet_reflects_activation(console, boundflow_api_key):
    """The CLI proves this via `workflow list`; the fleet table is the same read."""
    async with control_plane(boundflow_api_key) as cp:
        wf = await _tenant_and_workflow(cp, unique("ui_activate"), activate=False)
        async with console_client(console) as c:
            before = (await c.get(f"/workflows/{wf.id}")).text
            assert "Not activated." in before

            await cp.activate_workflow(wf.id)
            after = (await c.get(f"/workflows/{wf.id}")).text
            assert "Not activated." not in after


async def test_detail_shows_lifecycle_and_workflow_state(console, boundflow_api_key):
    async with control_plane(boundflow_api_key) as cp:
        wf = await _tenant_and_workflow(cp, unique("ui_detail"))
        info = await cp.get_workflow(wf.id)
        async with console_client(console) as c:
            body = (await c.get(f"/workflows/{wf.id}")).text
    assert info.lifecycle_state.value in body
    assert info.workflow_state.value in body


async def test_a_missing_workflow_surfaces_the_server_error(console, boundflow_api_key):
    async with console_client(console) as c:
        r = await c.get("/workflows/does-not-exist")
    assert r.status_code == 200          # an error page, not a crash
    assert "not found" in r.text.lower()


async def test_metrics_are_zero_before_any_run(console, boundflow_api_key):
    async with control_plane(boundflow_api_key) as cp:
        wf = await _tenant_and_workflow(cp, unique("ui_metrics"))
        async with console_client(console) as c:
            body = (await c.get(f"/workflows/{wf.id}")).text
    assert "No runs yet." in body


# ── Gates ────────────────────────────────────────────────────────────────────

async def test_approving_in_the_console_runs_the_approve_branch(console,
                                                                boundflow_api_key):
    """The CLI's test_approve_via_cli_runs_approve_branch, through the console."""
    approved = []
    wtype = unique("ui_approve")
    worker = BoundFlowWorker(WORKER_ADDRESS, dummy_mock())

    @worker.workflow(wtype, version=1)
    async def _entry(ctx):
        return AwaitApproval(
            on_approve=Next(operation="approved_step", context=ctx.context, timeout=30),
            on_reject=Complete(),
            timeout=120,
            justification="ship it?",
        )

    @worker.operation(wtype, "approved_step")
    async def _approved(ctx):
        approved.append(True)
        return Complete()

    async with run_worker(worker), control_plane(boundflow_api_key) as cp:
        wf = await _tenant_and_workflow(cp, wtype)
        await cp.invoke_workflow(wf.id, operation_timeout_seconds=120)
        await wait_lifecycle(cp, wf.id, LifecycleState.AWAITING_APPROVAL)

        async with console_client(console) as c:
            # The console discovers the approval_id itself — the operator never sees it.
            page = (await c.get("/inbox")).text
            assert "ship it?" in page
            approval_id = (await cp.get_workflow(wf.id)).pending_approval.approval_id

            r = await c.post(f"/workflows/{wf.id}/approval",
                             data={"decision": "approve", "approval_id": approval_id,
                                   "actor": "ui@test", "reason": "looks fine"},
                             follow_redirects=False)
            assert r.status_code == 303

        await wait_for(lambda: _truthy(approved), "the approve branch to run")

        audit = await cp.get_approval_audit(wf.id)
        assert audit[0].decision.value == "approved"
        assert audit[0].actor == "ui@test"
        assert audit[0].reason == "looks fine"


async def test_rejecting_in_the_console_runs_the_reject_branch(console,
                                                               boundflow_api_key):
    rejected = []
    wtype = unique("ui_reject")
    worker = BoundFlowWorker(WORKER_ADDRESS, dummy_mock())

    @worker.workflow(wtype, version=1)
    async def _entry(ctx):
        return AwaitApproval(
            on_approve=Next(operation="never", context=ctx.context, timeout=30),
            on_reject=Complete(result={"rejected": True}),
            timeout=120,
        )

    async with run_worker(worker), control_plane(boundflow_api_key) as cp:
        wf = await _tenant_and_workflow(cp, wtype)
        await cp.invoke_workflow(wf.id, operation_timeout_seconds=120)
        await wait_lifecycle(cp, wf.id, LifecycleState.AWAITING_APPROVAL)
        approval_id = (await cp.get_workflow(wf.id)).pending_approval.approval_id

        async with console_client(console) as c:
            await c.post(f"/workflows/{wf.id}/approval",
                         data={"decision": "reject", "approval_id": approval_id,
                               "actor": "ui@test", "reason": "too expensive"},
                         follow_redirects=False)

        await wait_lifecycle(cp, wf.id, LifecycleState.ACTIVE)
        audit = await cp.get_approval_audit(wf.id)
        assert audit[0].decision.value == "rejected"
        assert audit[0].reason == "too expensive"
        rejected.append(True)


async def test_answering_an_input_gate_in_the_console(console, boundflow_api_key):
    answers = []
    wtype = unique("ui_input")
    worker = BoundFlowWorker(WORKER_ADDRESS, dummy_mock())

    @worker.workflow(wtype, version=1)
    async def _entry(ctx):
        return AwaitInput(
            on_answer=Next(operation="answered", context=ctx.context, timeout=30),
            on_timeout=Complete(),
            timeout=120,
            prompt="which region?",
        )

    @worker.operation(wtype, "answered")
    async def _answered(ctx):
        answers.append(ctx.input_answer)
        return Complete()

    async with run_worker(worker), control_plane(boundflow_api_key) as cp:
        wf = await _tenant_and_workflow(cp, wtype)
        await cp.invoke_workflow(wf.id, operation_timeout_seconds=120)
        await wait_lifecycle(cp, wf.id, LifecycleState.AWAITING_INPUT)
        input_id = (await cp.get_workflow(wf.id)).pending_input.input_id

        async with console_client(console) as c:
            assert "which region?" in (await c.get("/inbox")).text
            await c.post(f"/workflows/{wf.id}/input",
                         data={"input_id": input_id, "answer": '{"region": "us-east"}',
                               "actor": "ui@test"},
                         follow_redirects=False)

        await wait_for(lambda: _truthy(answers), "the answer branch to run")
    # A JSON object reaches the handler unwrapped; plain text would be {"answer": ...}.
    assert answers[0] == {"region": "us-east"}


# ── Holds ────────────────────────────────────────────────────────────────────

async def test_suspend_then_resume_through_the_console(console, boundflow_api_key):
    """The CLI's suspend/resume pair, driven entirely from the console — including
    that it finds the suspension_id itself rather than making anyone paste it."""
    async with control_plane(boundflow_api_key) as cp:
        wf = await _tenant_and_workflow(cp, unique("ui_hold"))

        async with console_client(console) as c:
            r = await c.post(f"/workflows/{wf.id}/suspend",
                             data={"reason": "cost spike", "stop_current": "1"},
                             follow_redirects=False)
            assert r.status_code == 303

            info = await cp.get_workflow(wf.id)
            assert info.workflow_state == WorkflowState.SUSPENDED
            assert info.suspension.reason == "cost spike"
            assert info.suspension.stop_current is True

            held = (await c.get("/holds")).text
            assert wf.id in held and "cost spike" in held

            await wait_for(
                lambda: _finalized(cp, wf.id), "the suspension to finish draining")

            detail = (await c.get(f"/workflows/{wf.id}")).text
            assert "Held by an operator" in detail
            assert "Resume" in detail

            sid = (await cp.get_workflow(wf.id)).suspension.suspension_id
            await c.post(f"/workflows/{wf.id}/resume",
                         data={"suspension_id": sid}, follow_redirects=False)

        info = await cp.get_workflow(wf.id)
        assert info.workflow_state == WorkflowState.ACTIVE
        assert info.suspension is None


async def test_resume_with_a_stale_id_shows_the_refusal(console, boundflow_api_key):
    """The server refuses it; the console has to surface that rather than swallow it."""
    async with control_plane(boundflow_api_key) as cp:
        wf = await _tenant_and_workflow(cp, unique("ui_badresume"))
        async with console_client(console) as c:
            await c.post(f"/workflows/{wf.id}/suspend", data={"reason": "x"},
                         follow_redirects=False)
            r = await c.post(f"/workflows/{wf.id}/resume",
                             data={"suspension_id": "not-the-right-id"},
                             follow_redirects=False)
            assert r.status_code == 303
            assert "error=" in r.headers["location"]


async def test_the_holds_view_only_lists_held_workflows(console, boundflow_api_key):
    async with control_plane(boundflow_api_key) as cp:
        held = await _tenant_and_workflow(cp, unique("ui_held"))
        free = await _tenant_and_workflow(cp, unique("ui_free"))
        async with console_client(console) as c:
            await c.post(f"/workflows/{held.id}/suspend", data={"reason": "held"},
                         follow_redirects=False)
            body = (await c.get("/holds")).text
    assert held.id in body
    assert free.id not in body


# ── Delete ───────────────────────────────────────────────────────────────────

async def test_delete_needs_the_typed_id_and_then_removes_the_workflow(
        console, boundflow_api_key):
    async with control_plane(boundflow_api_key) as cp:
        wf = await _tenant_and_workflow(cp, unique("ui_delete"))

        async with console_client(console) as c:
            wrong = await c.post(f"/workflows/{wf.id}/delete",
                                 data={"confirm": "not-the-id"}, follow_redirects=False)
            assert "error=" in wrong.headers["location"]
            assert (await cp.get_workflow(wf.id)) is not None   # still there

            ok = await c.post(f"/workflows/{wf.id}/delete",
                              data={"confirm": wf.id}, follow_redirects=False)
            assert ok.headers["location"] == "/"

            # Deletion is soft plus an async purge, so it stays listed as `deleted`
            # rather than disappearing — same as `boundflow workflow list`.
            await wait_for(lambda: _deleted(cp, wf.id), "the workflow to be deleted")
            assert "Deleted." in (await c.get(f"/workflows/{wf.id}")).text

            # It leaves the fleet for its own view rather than sitting among the live
            # workflows until the purge reconciler gets to it.
            assert wf.id not in (await c.get("/")).text
            assert wf.id in (await c.get("/deleted")).text


# ── helpers ──────────────────────────────────────────────────────────────────

async def _truthy(lst):
    return bool(lst)


async def _finalized(cp, wid):
    s = (await cp.get_workflow(wid)).suspension
    return s is not None and s.finalized_at is not None


async def _deleted(cp, wid):
    return (await cp.get_workflow(wid)).lifecycle_state == LifecycleState.DELETED


async def test_resolving_an_interruption_reactivates_the_workflow(console,
                                                                  boundflow_api_key):
    """A worker that dies mid-run interrupts the workflow; the console clears it with
    one button, filling in the run id the CLI makes you look up and paste."""
    wtype = unique("ui_resolve")
    worker = BoundFlowWorker(WORKER_ADDRESS, dummy_mock())
    started = []

    @worker.workflow(wtype, version=1)
    async def _entry(ctx):
        started.append(True)
        await asyncio.sleep(3600)        # never returns; the worker dies under it
        return Complete()

    async with control_plane(boundflow_api_key) as cp:
        wf = await _tenant_and_workflow(cp, wtype)
        async with run_worker(worker):
            await cp.invoke_workflow(wf.id, operation_timeout_seconds=5)
            await wait_for(lambda: _truthy(started), "the run to start")
        # Worker gone mid-run: the scheduler interrupts the workflow.
        await wait_lifecycle(cp, wf.id, LifecycleState.INTERRUPTED, timeout=90)

        async with console_client(console) as c:
            detail = (await c.get(f"/workflows/{wf.id}")).text
            assert "platform failure" in detail
            rid = (await cp.get_workflow(wf.id)).last_interrupted_request_id
            assert rid and f'value="{rid}"' in detail    # prefilled, nothing to paste

            r = await c.post(f"/workflows/{wf.id}/resolve",
                             data={"request_id": rid}, follow_redirects=False)
            assert r.status_code == 303

        await wait_lifecycle(cp, wf.id, LifecycleState.ACTIVE, timeout=60)


async def test_activating_releases_a_workflow_the_console_shows_as_paused(
        console, boundflow_api_key):
    """A freshly created workflow is paused, and the console has to be able to
    release it — the callout named the cause but offered nothing before this."""
    async with control_plane(boundflow_api_key) as cp:
        wf = await _tenant_and_workflow(cp, unique("ui_activate2"), activate=False)
        assert (await cp.get_workflow(wf.id)).workflow_state == WorkflowState.PAUSED

        async with console_client(console) as c:
            detail = (await c.get(f"/workflows/{wf.id}")).text
            assert "Not activated." in detail
            assert "Activate" in detail

            r = await c.post(f"/workflows/{wf.id}/activate", data={"request_id": ""},
                             follow_redirects=False)
            assert r.status_code == 303

        info = await cp.get_workflow(wf.id)
        assert info.workflow_state == WorkflowState.ACTIVE


async def test_activating_with_a_stale_policy_decision_is_refused(console,
                                                                  boundflow_api_key):
    """The guard is what makes a one-click override safe: it names the decision being
    overridden, so a newer one can't be discarded by a stale page."""
    async with control_plane(boundflow_api_key) as cp:
        wf = await _tenant_and_workflow(cp, unique("ui_stale"), activate=False)
        async with console_client(console) as c:
            r = await c.post(f"/workflows/{wf.id}/activate",
                             data={"request_id": "some-other-decision"},
                             follow_redirects=False)
            assert "error=" in r.headers["location"]
        assert (await cp.get_workflow(wf.id)).workflow_state == WorkflowState.PAUSED


async def test_the_audit_log_on_the_detail_page_is_the_real_one(console,
                                                                boundflow_api_key):
    """A decision made in the console has to come back out of the audit log there
    too — the console reads the same records the CLI's `audit` commands do."""
    wtype = unique("ui_audit")
    worker = BoundFlowWorker(WORKER_ADDRESS, dummy_mock())

    @worker.workflow(wtype, version=1)
    async def _entry(ctx):
        return AwaitApproval(
            on_approve=Next(operation="never", context=ctx.context, timeout=30),
            on_reject=Complete(),
            timeout=120,
            justification="spend $5,000?",
        )

    async with run_worker(worker), control_plane(boundflow_api_key) as cp:
        wf = await _tenant_and_workflow(cp, wtype)
        await cp.invoke_workflow(wf.id, operation_timeout_seconds=120)
        await wait_lifecycle(cp, wf.id, LifecycleState.AWAITING_APPROVAL)
        approval_id = (await cp.get_workflow(wf.id)).pending_approval.approval_id

        async with console_client(console) as c:
            await c.post(f"/workflows/{wf.id}/approval",
                         data={"decision": "reject", "approval_id": approval_id,
                               "actor": "ops@example.com", "reason": "over the limit"},
                         follow_redirects=False)
            await wait_lifecycle(cp, wf.id, LifecycleState.ACTIVE)

            body = (await c.get(f"/workflows/{wf.id}")).text

    assert "Audit" in body
    assert "ops@example.com" in body
    assert "over the limit" in body
    assert "rejected" in body
    assert "spend $5,000?" in body


async def test_abandoning_queued_runs_from_the_console(console, boundflow_api_key):
    """Queue mode lets runs pile up; the console has to be able to drop them.

    Only queued runs are affected — one already scheduled or in progress is
    untouched — so this asserts what actually came back, not what was asked for.
    """
    async with control_plane(boundflow_api_key) as cp:
        tenant = await cp.create_tenant(unique("ui"))
        wf = await cp.create_workflow(
            unique("ui_abandon"), tenant.id,
            config=WorkflowConfig(version=1, invoke_mode=InvokeMode.QUEUE,
                                  max_queue_depth=10))
        await cp.activate_workflow(wf.id)
        # No worker is running, so these queue up rather than being picked up.
        for _ in range(3):
            await cp.invoke_workflow(wf.id, operation_timeout_seconds=60)

        async with console_client(console) as c:
            assert "Abandon queued runs" in (await c.get(f"/workflows/{wf.id}")).text
            r = await c.post(f"/workflows/{wf.id}/abandon", data={"all": "1"},
                             follow_redirects=False)
            assert r.status_code == 303
            assert "error=" not in r.headers["location"]

            # Ambiguity is refused the same way the CLI refuses it.
            bad = await c.post(f"/workflows/{wf.id}/abandon",
                               data={"request_ids": "r1", "all": "1"},
                               follow_redirects=False)
            assert "error=" in bad.headers["location"]

        runs = await cp.list_workflow_runs(wf.id)
        assert any(r.status == RunStatus.ABANDONED for r in runs), \
            [r.status.value for r in runs]


async def test_the_fleet_filters_by_tenant_against_real_data(console,
                                                             boundflow_api_key):
    async with control_plane(boundflow_api_key) as cp:
        t1 = await cp.create_tenant(unique("ui-t1"))
        t2 = await cp.create_tenant(unique("ui-t2"))
        a = await cp.create_workflow(unique("ui_ta"), t1.id,
                                     config=WorkflowConfig(version=1))
        b = await cp.create_workflow(unique("ui_tb"), t2.id,
                                     config=WorkflowConfig(version=1))

        async with console_client(console) as c:
            both = (await c.get("/")).text
            assert a.id in both and b.id in both

            only_t1 = (await c.get(f"/?tenant={t1.id}")).text
            assert a.id in only_t1
            assert b.id not in only_t1
            assert "Filtered to tenant" in only_t1

            # The poll fetches with the same filter, so a refresh doesn't drop it.
            frag = (await c.get(f"/fragment/fleet?tenant={t1.id}")).text
            assert a.id in frag and b.id not in frag


async def test_cooldown_until_reaches_the_console(console, boundflow_api_key):
    """The column was loaded on every read but dropped in WorkflowToProto, so a
    cooldown reached the customer explained and untimed. This drives a real policy
    cooldown and checks the deadline arrives and is rendered.

    Fires on failures rather than LLM calls because this worker never runs an agent.
    """
    from boundflow import Complete, WorkflowMetric, WorkflowRule
    from boundflow.policies import Cooldown

    wtype = unique("ui_cooldown")
    worker = BoundFlowWorker(WORKER_ADDRESS, dummy_mock())

    @worker.workflow(wtype, version=1)
    async def _entry(ctx):
        raise RuntimeError("failing on purpose, to trip the cooldown rule")

    async with run_worker(worker), control_plane(boundflow_api_key) as cp:
        wf = await _tenant_and_workflow(cp, wtype, activate=False)
        await cp.set_workflow_lifecycle_policy(wf.id, [
            WorkflowRule(metric=WorkflowMetric.NUM_FAILURES, threshold=1,
                         action=Cooldown(window=1, seconds=120)),
        ])
        await cp.activate_workflow(wf.id)
        await cp.invoke_workflow(wf.id, operation_timeout_seconds=60)

        async def cooling():
            return (await cp.get_workflow(wf.id)).workflow_state == WorkflowState.COOLDOWN

        await wait_for(cooling, "the policy to put the workflow in cooldown", 90)

        info = await cp.get_workflow(wf.id)
        assert info.cooldown_until is not None, "cooldown_until never reached the SDK"

        async with console_client(console) as c:
            body = (await c.get(f"/workflows/{wf.id}")).text
        assert "Cooling down" in body
        assert "Scheduling resumes" in body


async def test_list_agents_discovers_agents_and_their_armed_policies(
        console, boundflow_api_key):
    """Every other agent RPC takes a name the caller is assumed to know, which only
    works for whoever wrote the workflow. Setting a policy creates the agent_state
    row, so this needs no worker.
    """
    from boundflow import RuntimePolicy

    async with control_plane(boundflow_api_key) as cp:
        wf = await _tenant_and_workflow(cp, unique("ui_agents"))
        assert await cp.list_agents(wf.id) == []      # nothing has run or been armed

        await cp.set_agent_runtime_policy(wf.id, "responder",
                                          RuntimePolicy(max_llm_calls=4))
        await cp.set_agent_runtime_policy(wf.id, "summarizer",
                                          RuntimePolicy(max_cost_usd=1.5))

        agents = await cp.list_agents(wf.id)
        assert [a.agent_name for a in agents] == ["responder", "summarizer"]  # sorted
        # snake_case: MessageToDict camelCases proto field names, but a Struct's
        # contents are literal keys and the server stores the SDK's own JSON.
        assert agents[0].runtime_policy["max_llm_calls"] == 4
        assert agents[1].runtime_policy["max_cost_usd"] == 1.5
        assert agents[0].lifecycle_policy == {}      # none armed, not absent

        async with console_client(console) as c:
            body = (await c.get(f"/workflows/{wf.id}")).text
        assert "Policies" in body
        assert "responder" in body and "summarizer" in body


async def test_the_workflow_lifecycle_policy_is_shown_in_the_console(
        console, boundflow_api_key):
    """The console could say a policy had paused a workflow without showing the rule."""
    from boundflow import WorkflowMetric, WorkflowRule
    from boundflow.policies import Cooldown

    async with control_plane(boundflow_api_key) as cp:
        wf = await _tenant_and_workflow(cp, unique("ui_wfpolicy"))
        await cp.set_workflow_lifecycle_policy(wf.id, [
            WorkflowRule(metric=WorkflowMetric.COST, threshold=5.0,
                         action=Cooldown(window=10, seconds=120)),
        ])
        async with console_client(console) as c:
            body = (await c.get(f"/workflows/{wf.id}")).text
    assert "Workflow lifecycle" in body
    assert "cost" in body
