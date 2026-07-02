"""E2E version test — registers a worker for a specific version, prints the version
when invoked, and verifies the full control-plane round-trip without an LLM.

Prerequisites:
    docker compose up -d --build --wait
    export BOUNDFLOW_API_KEY=<from: docker compose run --rm server -mode=provision -name=me>

Run:
    python examples/e2e_version_test.py [--version 2]
"""
import argparse
import asyncio

from boundflow import BoundFlowWorker, Complete, ControlPlaneClient, LifecycleState, WorkflowConfig
from boundflow.llm import LlmClient, LlmRequest, LlmResponse, Usage

WORKFLOW_TYPE = "e2e-version-test"


class _NoOpLlm:
    """Stub — satisfies the LlmClient protocol but is never called in this test."""
    async def complete(self, request: LlmRequest) -> LlmResponse:
        raise RuntimeError("LLM should not be called in this test")


async def main(version: int) -> None:
    worker = BoundFlowWorker(llm=_NoOpLlm())

    @worker.workflow(WORKFLOW_TYPE, version=version)
    async def handle(ctx):
        print(f"[worker] handler called — workflow_version={ctx.workflow_version}")
        return Complete()

    print(f"[setup] registering worker for '{WORKFLOW_TYPE}' v{version}")
    worker_task = asyncio.create_task(worker.run())

    async with ControlPlaneClient() as cp:
        tenant = await cp.create_tenant("e2e-test-tenant")
        print(f"[cp] tenant created: {tenant.id}")

        wf = await cp.create_workflow(
            WORKFLOW_TYPE, tenant.id, config=WorkflowConfig(version=version)
        )
        print(f"[cp] workflow created: {wf.id}  version={wf.config.version}")

        await cp.activate_workflow(wf.id)
        print("[cp] workflow activated")

        request_id = await cp.invoke_workflow(wf.id, operation_timeout_seconds=30)
        print(f"[cp] invoked — request_id={request_id}")

        while True:
            state = await cp.get_workflow_lifecycle_state(wf.id)
            if state != LifecycleState.INVOKING:
                break
            await asyncio.sleep(0.5)

        print(f"[cp] final state: {state.value}")
        assert state == LifecycleState.ACTIVE, f"expected ACTIVE, got {state.value}"
        print("[ok] version round-trip verified")

    worker_task.cancel()
    try:
        await worker_task
    except asyncio.CancelledError:
        pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", type=int, default=2, help="workflow version to register")
    args = parser.parse_args()
    asyncio.run(main(args.version))
