"""The console's HTTP layer.

One long-lived ControlPlaneClient for the process, since the fleet fragment polls and
reconnecting per request would be wasteful. The API key stays here — it is read from
the environment into this process and never rendered into the page, so the browser
holds no credential.

Binds 127.0.0.1 by default and there is deliberately no flag to change it: the console
has no identity of its own, so anyone who can reach it can act with the operator's key.
Reach a remote control plane by pointing --server at it, not by exposing this.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from ..control_plane import ControlPlaneClient, DEFAULT_SERVER_ADDRESS
from . import views
from .labels import DEFAULT as DEFAULT_LABELS, Labels
from .render import nav_links, page

log = logging.getLogger("boundflow.ui")

# A gated workflow needs a second call for its gate detail: list_workflows returns a
# light WorkflowInfo with pending_approval/pending_input unset. Bounded so a fleet
# that is entirely parked can't open hundreds of concurrent calls.
_GATE_FANOUT = 8


def _require_starlette():
    try:
        from starlette.applications import Starlette  # noqa: F401
    except ModuleNotFoundError as exc:  # pragma: no cover - import-guard
        raise SystemExit(
            "The operator console needs its optional dependencies:\n"
            "    pip install 'boundflow[ui]'"
        ) from exc


def _parse_answer(raw: str) -> dict:
    """Turn the answer textbox into the dict submit_input expects.

    A JSON object is passed through. Anything else is wrapped as {"answer": text},
    so an operator answering a free-form prompt doesn't have to hand-write JSON —
    the field's hint says so, since it changes the shape the handler receives.
    """
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return {"answer": raw}
    return parsed if isinstance(parsed, dict) else {"answer": parsed}


class Console:
    """Holds the control-plane connection and renders the screens."""

    def __init__(self, server: str, api_key: str,
                 labels: Labels = DEFAULT_LABELS) -> None:
        self.server = server
        self.labels = labels
        self._api_key = api_key
        self._cp: ControlPlaneClient | None = None

    async def start(self) -> None:
        self._cp = ControlPlaneClient(self.server, api_key=self._api_key)
        await self._cp.__aenter__()

    async def stop(self) -> None:
        if self._cp is not None:
            await self._cp.__aexit__(None, None, None)
            self._cp = None

    @property
    def cp(self) -> ControlPlaneClient:
        if self._cp is None:  # pragma: no cover - lifespan guarantees this
            raise RuntimeError("console used before start()")
        return self._cp

    async def gated(self, workflows: list) -> list:
        """Fetch full detail for the workflows parked on a person, in fleet order.

        A workflow that moves off its gate between the list and the get simply drops
        out — it is no longer waiting on anyone, which is the right answer.
        """
        waiting = [w for w in workflows if views.is_gated(w)]
        if not waiting:
            return []
        sem = asyncio.Semaphore(_GATE_FANOUT)

        async def one(w):
            async with sem:
                try:
                    return await self.cp.get_workflow(w.id)
                except Exception:
                    log.warning("could not load gate detail for %s", w.id, exc_info=True)
                    return None

        full = await asyncio.gather(*(one(w) for w in waiting))
        return [w for w in full if w is not None and views.is_gated(w)]

    async def held(self, workflows: list) -> list:
        """Workflows under an operator hold, with the suspension detail the release
        control needs — which, like the gates, only get_workflow carries."""
        suspended = [w for w in workflows if views.is_suspended(w)]
        if not suspended:
            return []
        sem = asyncio.Semaphore(_GATE_FANOUT)

        async def one(w):
            async with sem:
                try:
                    return await self.cp.get_workflow(w.id)
                except Exception:
                    log.warning("could not load hold detail for %s", w.id, exc_info=True)
                    return None

        full = await asyncio.gather(*(one(w) for w in suspended))
        return [w for w in full if w is not None and views.is_suspended(w)]


def build_app(console: Console):
    """Construct the Starlette app. Imported lazily so `boundflow --help` works
    without the ui extra installed."""
    import contextlib

    from starlette.applications import Starlette
    from starlette.responses import HTMLResponse, RedirectResponse
    from starlette.routing import Route

    @contextlib.asynccontextmanager
    async def lifespan(_app):
        await console.start()
        try:
            yield
        finally:
            await console.stop()

    def nav(current: str, workflows: list | None = None) -> str:
        """Sidebar entries. Counts need the fleet, so a page that hasn't loaded it
        renders the labels bare rather than paying for an extra call."""
        lb = console.labels
        if workflows is None:
            counts = (None, None, None, None)
        else:
            gone = [w for w in workflows if views.is_deleted(w)]
            counts = (
                len(workflows) - len(gone),      # the fleet excludes tombstones
                sum(1 for w in workflows if views.is_gated(w)),
                sum(1 for w in workflows if views.is_suspended(w)),
                len(gone),
            )
        return nav_links(
            [("/", lb.fleet, counts[0]),
             ("/inbox", lb.inbox, counts[1]),
             ("/holds", lb.holds, counts[2]),
             ("/deleted", lb.deleted, counts[3])],
            current,
        )

    # Every screen is a live view of control-plane state, so a cached copy is always
    # wrong — and a stale gate is worse than no gate, since it invites a decision on
    # an approval that has already been resolved.
    NO_STORE = {"Cache-Control": "no-store"}

    def render(title: str, body: str, request, *, current: str = "/",
               workflows: list | None = None, filterable: bool = True) -> HTMLResponse:
        return HTMLResponse(page(
            title, body, server=console.server, labels=console.labels,
            nav=nav(current, workflows), filterable=filterable,
            error=request.query_params.get("error", ""),
        ), headers=NO_STORE)

    def failed(request, title: str, exc: Exception) -> HTMLResponse:
        return HTMLResponse(page(
            title, "", server=console.server, labels=console.labels,
            nav=nav(request.url.path), error=str(exc),
        ), headers=NO_STORE)

    def back(workflow_id: str, error: str = "") -> RedirectResponse:
        # 303 so a refresh after acting doesn't repost the decision.
        suffix = f"?error={error}" if error else ""
        return RedirectResponse(f"/workflows/{workflow_id}{suffix}", status_code=303)

    async def home(request):
        try:
            workflows = await console.cp.list_workflows()
        except Exception as exc:
            return failed(request, console.labels.fleet, exc)
        gated = await console.gated(workflows)
        q = request.query_params
        return render(console.labels.fleet,
                      views.home(workflows, gated, console.labels,
                                 sort=q.get("sort", ""), desc=bool(q.get("desc")),
                                 tenant=q.get("tenant", "")),
                      request, current="/", workflows=workflows)

    async def inbox(request):
        try:
            workflows = await console.cp.list_workflows()
        except Exception as exc:
            return failed(request, console.labels.inbox, exc)
        gated = await console.gated(workflows)
        return render(console.labels.inbox,
                      views.inbox_page(gated, console.labels), request,
                      current="/inbox", workflows=workflows, filterable=False)

    async def deleted(request):
        try:
            workflows = await console.cp.list_workflows()
        except Exception as exc:
            return failed(request, console.labels.deleted, exc)
        gone = [w for w in workflows if views.is_deleted(w)]
        return render(console.labels.deleted,
                      views.deleted_page(gone, console.labels), request,
                      current="/deleted", workflows=workflows)

    async def holds(request):
        try:
            workflows = await console.cp.list_workflows()
        except Exception as exc:
            return failed(request, console.labels.holds, exc)
        held = await console.held(workflows)
        return render(console.labels.holds,
                      views.holds_page(held, console.labels), request,
                      current="/holds", workflows=workflows, filterable=False)

    async def fleet_fragment(request):
        """Polled by the page. Errors return 502 so the poller leaves the last good
        table on screen instead of blanking it."""
        try:
            workflows = await console.cp.list_workflows()
        except Exception as exc:
            return HTMLResponse(str(exc), status_code=502)
        q = request.query_params
        return HTMLResponse(
            views.fleet_table(workflows, console.labels, sort=q.get("sort", ""),
                              desc=bool(q.get("desc")), tenant=q.get("tenant", "")),
            headers=NO_STORE)

    async def detail(request):
        wid = request.path_params["workflow_id"]
        try:
            workflow = await console.cp.get_workflow(wid)
        except Exception as exc:
            return failed(request, console.labels.workflow.capitalize(), exc)
        # Every panel is optional: a workflow with a broken metrics read still has
        # to render, because the page is how an operator finds out what is wrong.
        runs, metrics, audit, policies = await asyncio.gather(
            console.cp.list_workflow_runs(wid),
            console.cp.get_workflow_metrics(wid),
            console.cp.get_audit_log(wid),
            console.cp.get_workflow_policy_audit(wid),
            return_exceptions=True,
        )
        for name, value in (("runs", runs), ("metrics", metrics),
                            ("audit", audit), ("policy audit", policies)):
            if isinstance(value, BaseException):
                log.warning("could not load %s for %s", name, wid, exc_info=value)
        runs = [] if isinstance(runs, BaseException) else runs
        metrics = None if isinstance(metrics, BaseException) else metrics
        audit = [] if isinstance(audit, BaseException) else audit
        policies = [] if isinstance(policies, BaseException) else policies

        # The decision that put the workflow in its current state, so the callout can
        # say what crossed rather than only that something did.
        decision = next(
            (p for p in policies
             if p.request_id == workflow.last_policy_decision_request_id), None)
        return render(wid, views.workflow_detail(workflow, runs, metrics,
                                                 console.labels, audit, decision),
                      request, current="", filterable=False)

    async def act(request, fn):
        """Run one control-plane mutation and redirect back to the workflow."""
        wid = request.path_params["workflow_id"]
        form = await request.form()
        try:
            await fn(wid, form)
        except Exception as exc:
            return back(wid, str(exc))
        return back(wid)

    async def approval(request):
        async def go(wid: str, form: Any):
            approve = form.get("decision") == "approve"
            call = console.cp.approve_workflow if approve else console.cp.reject_workflow
            await call(
                wid, form.get("approval_id", ""),
                actor=form.get("actor", ""), reason=form.get("reason", ""),
            )
        return await act(request, go)

    async def submit_input(request):
        async def go(wid: str, form: Any):
            await console.cp.submit_input(
                wid, form.get("input_id", ""),
                _parse_answer(form.get("answer", "")), actor=form.get("actor", ""),
            )
        return await act(request, go)

    async def suspend(request):
        async def go(wid: str, form: Any):
            await console.cp.suspend_workflow(
                wid, reason=form.get("reason", ""),
                stop_current_run=bool(form.get("stop_current")),
                suspension_id=form.get("suspension_id", ""),
            )
        return await act(request, go)

    async def resume(request):
        async def go(wid: str, form: Any):
            await console.cp.resume_workflow(wid, form.get("suspension_id", ""))
        return await act(request, go)

    async def abandon(request):
        async def go(wid: str, form: Any):
            ids = [i.strip() for i in form.get("request_ids", "").split(",") if i.strip()]
            everything = bool(form.get("all"))
            # The RPC takes exactly one of the two, the same as `abandon-queued`.
            if everything and ids:
                raise ValueError("choose run ids or all queued runs, not both")
            if not everything and not ids:
                raise ValueError("give run ids, or tick all queued runs")
            await console.cp.abandon_queued_requests(
                wid, request_ids=ids or None, all=everything)
        return await act(request, go)

    async def activate(request):
        async def go(wid: str, form: Any):
            await console.cp.activate_workflow(wid, form.get("request_id", ""))
        return await act(request, go)

    async def resolve(request):
        async def go(wid: str, form: Any):
            await console.cp.resolve_interrupted_workflow(wid, form.get("request_id", ""))
        return await act(request, go)

    async def delete(request):
        """Deleting leaves nothing to redirect back to, so this one goes to the fleet.

        The typed id is checked here rather than in the browser: a confirmation that
        only exists in JavaScript isn't one.
        """
        wid = request.path_params["workflow_id"]
        form = await request.form()
        if form.get("confirm", "").strip() != wid:
            return back(wid, "confirmation does not match this workflow id")
        try:
            await console.cp.delete_workflow(wid)
        except Exception as exc:
            return back(wid, str(exc))
        return RedirectResponse("/", status_code=303)

    return Starlette(
        routes=[
            Route("/", home),
            Route("/inbox", inbox),
            Route("/holds", holds),
            Route("/deleted", deleted),
            Route("/fragment/fleet", fleet_fragment),
            Route("/workflows/{workflow_id}", detail),
            Route("/workflows/{workflow_id}/approval", approval, methods=["POST"]),
            Route("/workflows/{workflow_id}/input", submit_input, methods=["POST"]),
            Route("/workflows/{workflow_id}/suspend", suspend, methods=["POST"]),
            Route("/workflows/{workflow_id}/resume", resume, methods=["POST"]),
            Route("/workflows/{workflow_id}/abandon", abandon, methods=["POST"]),
            Route("/workflows/{workflow_id}/activate", activate, methods=["POST"]),
            Route("/workflows/{workflow_id}/resolve", resolve, methods=["POST"]),
            Route("/workflows/{workflow_id}/delete", delete, methods=["POST"]),
        ],
        lifespan=lifespan,
    )


def serve(server: str = DEFAULT_SERVER_ADDRESS, api_key: str = "",
          port: int = 8787, open_browser: bool = True,
          labels: Labels = DEFAULT_LABELS) -> None:
    """Run the console until interrupted. Blocks."""
    _require_starlette()
    import uvicorn

    app = build_app(Console(server, api_key, labels))
    url = f"http://127.0.0.1:{port}"
    if open_browser:
        import threading
        import webbrowser
        threading.Timer(0.5, lambda: webbrowser.open(url)).start()
    print(f"{labels.brand} console on {url}  (control plane: {server})")
    uvicorn.run(app, host="127.0.0.1", port=port, log_level="warning")
