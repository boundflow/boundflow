"""Against a real Ollama server, the provider the output-token cap used to break.

Every Ollama call failed before reaching the model: the cap was bound as a
`max_tokens` call argument, and Ollama's client rejects it. Fakes that accept any
argument can't see that, so this talks to a real server.

Skipped unless OLLAMA_TEST_MODEL names a model the server at localhost:11434 has
pulled. CI uses a tiny one: what's under test is that a call goes through with the
cap applied and usage reported, not what the model says.
"""

from __future__ import annotations

import os

import pytest

pytest.importorskip("langchain_ollama")

from boundflow import AgentGovernor, RuntimePolicy  # noqa: E402
from boundflow.llm import LlmRequest, Message, TextBlock, ToolSpec  # noqa: E402

MODEL = os.environ.get("OLLAMA_TEST_MODEL")
pytestmark = pytest.mark.skipif(not MODEL, reason="OLLAMA_TEST_MODEL not set")

CAP = 16
PROMPT = "Count from 1 to 200, separated by spaces."


async def test_a_governed_ollama_call_goes_through_capped_and_metered():
    """The path Charter's agent loop takes."""
    from langchain_core.messages import HumanMessage
    from langchain_ollama import ChatOllama

    from boundflow.langchain_client import GovernedChatModel

    gov = AgentGovernor("responder", RuntimePolicy(max_tokens_per_call=CAP), MODEL, {})
    msg = await GovernedChatModel(governor=gov, chat_model=ChatOllama(model=MODEL)).ainvoke(
        [HumanMessage(content=PROMPT)])

    assert gov.llm_calls == 1
    assert gov.tokens_used > 0, "no usage reported, so the call couldn't be metered"
    assert msg.usage_metadata["output_tokens"] <= CAP


async def test_the_llm_client_caps_an_ollama_call_with_tools_bound():
    """The other path, where the cap has to go on before `bind_tools` wraps the model."""
    from langchain_ollama import ChatOllama

    from boundflow.langchain_client import LangChainLlmClient

    response = await LangChainLlmClient(ChatOllama(model=MODEL)).complete(LlmRequest(
        model=MODEL, max_tokens=CAP, system="Answer briefly.",
        messages=[Message("user", [TextBlock(PROMPT)])],
        tools=[ToolSpec("submit_result", "finish", {"type": "object"})],
    ))

    assert response.usage.output_tokens <= CAP
