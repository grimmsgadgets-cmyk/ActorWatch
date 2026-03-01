"""
Chat route — proxies analyst methodology assistant messages to Ollama.
Thin handler: validates input, delegates streaming to chat_service.
"""
from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

import route_paths


class _HistoryItem(BaseModel):
    role: str
    content: str = Field(..., max_length=2000)


class ChatRequest(BaseModel):
    message: str = Field(..., min_length=1, max_length=4000)
    history: list[_HistoryItem] = Field(default_factory=list, max_length=20)


def create_chat_router(*, deps: dict) -> APIRouter:
    router = APIRouter()
    stream_chat_response = deps["stream_chat_response"]

    @router.post(route_paths.CHAT_MESSAGE)
    async def chat_message(req: ChatRequest):
        history = [h.model_dump() for h in req.history]

        async def _generate():
            async for token in stream_chat_response(req.message, history):
                yield token.encode("utf-8")

        return StreamingResponse(_generate(), media_type="text/plain; charset=utf-8")

    return router
