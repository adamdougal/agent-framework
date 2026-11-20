# Copyright (c) Microsoft. All rights reserved.

"""The background reader the live realtime tests share; each live module keeps its own route and opt-in."""

import asyncio
import contextlib
from collections.abc import Callable
from typing import Any


def core(event: Any) -> dict[str, Any]:
    return (event.additional_properties or {}).get("core", {})


class Reader:
    """Reads a session's events in the background, as the conversation's receive loop does."""

    def __init__(self, session: Any) -> None:
        self.events: list[Any] = []
        self._task = asyncio.create_task(self._read(session))

    async def _read(self, session: Any) -> None:
        async for event in session.receive():
            self.events.append(event)

    def of_response(self, response_id: str) -> list[Any]:
        return [event for event in self.events if core(event).get("response_id") == response_id]

    def response_done(self, response_id: str) -> Any | None:
        return next((event for event in self.of_response(response_id) if event.kind == "response_done"), None)

    def of_type(self, service_event_type: str) -> list[Any]:
        return [event for event in self.events if event.service_event_type == service_event_type]

    def audio_bytes(self, response_id: str) -> int:
        audio = [event for event in self.of_response(response_id) if event.kind == "output_audio"]
        return sum(len(event.content.get_data_bytes()) for event in audio)

    def final_text(self, response_id: str) -> str:
        finals = [
            event
            for event in self.of_response(response_id)
            if event.kind == "output_text" and event.content.status == "completed"
        ]
        return " ".join(event.content.text for event in finals)

    async def until(self, condition: Callable[[], Any]) -> None:
        while not condition():
            if self._task.done():
                self._task.result()
                raise AssertionError("The session ended before the condition held.")
            await asyncio.sleep(0.02)

    async def stop(self) -> None:
        self._task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._task
