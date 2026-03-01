"""
Analyst methodology chat service — streams responses from Ollama /api/chat.
This is a teaching/guidance assistant for junior analysts; it does NOT produce
threat intelligence or attribution claims.
"""
import json
import os
from collections.abc import AsyncGenerator

import httpx

_SYSTEM_PROMPT = """You are an analyst methodology assistant built into ActorWatch, a \
threat actor tracking tool. Your role is to guide analysts — especially juniors — \
through the process of investigating threat actors and infrastructure.

You help analysts understand:
- What steps to take when starting an investigation
- How to pivot from one indicator to related infrastructure
- What MITRE ATT&CK techniques mean in plain language
- How to structure and document their findings in ActorWatch
- Which external tools to use for specific tasks

ActorWatch has the following tabs per actor: Overview, Timeline, IOCs, Notebook, Visuals. \
It also has a Resources section with curated external tools organized by task type \
(Infrastructure Pivoting, Historical DNS/WHOIS, Passive DNS/Pivot Chaining, Malware Analysis, \
Threat Actor/Campaign Context, IOC Feeds & Reputation, Vulnerability Context, \
Visualization & Graphing, Reporting & Standards).

When directing analysts to external tools, reference the Resources section by category name \
so they know where to find them in the UI.

You do not:
- Make attribution claims ("this is definitely group X")
- Invent or guess threat intelligence data
- Answer questions unrelated to threat analysis methodology

If asked something outside your scope, redirect the analyst back to the investigation process.

Keep responses concise and actionable. Analysts are working — they need direction, not essays."""

_MAX_HISTORY_TURNS = 10
_MAX_MESSAGE_LEN = 4000
_MAX_CONTENT_LEN = 2000
_TIMEOUT_SECONDS = 45.0


def _get_config() -> dict:
    return {
        "base_url": os.getenv("OLLAMA_BASE_URL", "http://localhost:11434").rstrip("/"),
        "model": os.getenv("OLLAMA_MODEL", "llama3.1:8b"),
    }


async def stream_chat_response_core(
    message: str,
    history: list[dict],
) -> AsyncGenerator[str, None]:
    """
    Streams response tokens from Ollama /api/chat.
    Yields plain text token strings. On error yields an error message string.
    """
    config = _get_config()

    messages: list[dict] = [{"role": "system", "content": _SYSTEM_PROMPT}]
    for turn in history[-_MAX_HISTORY_TURNS:]:
        role = str(turn.get("role", ""))
        content = str(turn.get("content", ""))[:_MAX_CONTENT_LEN]
        if role in {"user", "assistant"} and content:
            messages.append({"role": role, "content": content})
    messages.append({"role": "user", "content": message[:_MAX_MESSAGE_LEN]})

    payload = {
        "model": config["model"],
        "messages": messages,
        "stream": True,
        "options": {"temperature": 0.7, "num_predict": 500},
    }

    try:
        async with httpx.AsyncClient() as client:
            async with client.stream(
                "POST",
                f"{config['base_url']}/api/chat",
                json=payload,
                timeout=_TIMEOUT_SECONDS,
            ) as resp:
                resp.raise_for_status()
                async for line in resp.aiter_lines():
                    if not line:
                        continue
                    try:
                        chunk = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    token: str = chunk.get("message", {}).get("content", "")
                    if token:
                        yield token
                    if chunk.get("done"):
                        break
    except httpx.HTTPStatusError as exc:
        yield f"\n\n[Error: Ollama returned HTTP {exc.response.status_code}]"
    except httpx.ConnectError:
        yield "\n\n[Error: Could not connect to the local AI. Check that Ollama is running.]"
    except Exception as exc:  # noqa: BLE001
        yield f"\n\n[Error: {exc}]"
