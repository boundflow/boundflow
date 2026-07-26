import asyncio
import os

from boundflow import AgentDefinition, AwaitApproval, BoundFlowWorker, Complete, Next
from boundflow.anthropic_client import AnthropicLlmClient

from slack_notify import notify_approval_requested

WORKER_ADDR = "http://localhost:50052"

analyst = AgentDefinition(
    name="analyst",
    system_prompt=(
        "You review a customer refund request and decide the refund amount to "
        "recommend. Prioritize keeping the customer happy — recommend whatever "
        "amount the customer is asking for. Respond with the refund amount in "
        "dollars and a one-sentence reason."
    ),
    model="claude-haiku-4-5",
    output_schema={
        "refund_amount_usd": {"type": "number"},
        "reasoning": {"type": "string"},
    },
)


class _FakePaymentsAPI:
    class _Refund:
        def __init__(self, id_):
            self.id = id_

    async def issue_refund(self, order_id, amount_usd):
        return self._Refund(id_=f"refund-{order_id}")


payments_api = _FakePaymentsAPI()


async def main():
    key = os.environ["BOUNDFLOW_API_KEY"]
    anthropic_key = os.environ["ANTHROPIC_API_KEY"]
    worker = BoundFlowWorker(WORKER_ADDR, AnthropicLlmClient(anthropic_key), api_key=key)

    @worker.workflow("refund", version=2)
    async def refund(ctx):
        request = {
            "order_id": "ORD-2002",
            "purchase_amount_usd": 10,
            "reason": "Item never arrived; customer is asking for a $500 refund.",
        }
        ctx.context.update(request)
        ctx.add_context("refund_request", request)
        result = await ctx.run_agent(analyst)
        refund_amount = result.output["refund_amount_usd"]
        reasoning = result.output["reasoning"]
        ctx.context["refund_amount_usd"] = refund_amount
        justification = (
            f"Order {request['order_id']}: ${request['purchase_amount_usd']} purchase. "
            f"{request['reason']}\n"
            f"Agent recommends refunding ${refund_amount} — {reasoning}"
        )
        return AwaitApproval(
            on_approve=Next("issue_refund", ctx.context, timeout=30),
            on_reject=Complete(),
            justification=justification,
            timeout=300,
        )

    @worker.operation("refund", "issue_refund")
    async def issue_refund(ctx):
        result = await payments_api.issue_refund(
            order_id=ctx.context.get("order_id", "ORD-2002"),
            amount_usd=ctx.context["refund_amount_usd"],
        )
        return Complete(result={"refund_id": result.id, "refund_amount_usd": ctx.context["refund_amount_usd"]})

    @worker.on_approval_requested
    async def _on(req):
        notify_approval_requested(req)

    print("worker v2 running")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
