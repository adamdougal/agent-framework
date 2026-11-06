# PROTOTYPE, never merged (adamdougal/agent-framework#10).
"""A scripted caller: a mic that streams clips in real time (silence otherwise), and a speaker that plays agent audio
at real time, so barge-in reports a realistic ``played_ms``."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .accelerator import VoiceLoop

CLIPS_DIR = Path(__file__).resolve().parent.parent / "clips"
FRAME_MS = 20
FRAME_BYTES = 24 * 2 * FRAME_MS


def load_clip(name: str) -> bytes:
    return (CLIPS_DIR / f"{name}.pcm").read_bytes()


@dataclass
class _Playback:
    start: float
    queued_ms: float = 0.0
    stopped_at: float | None = None

    def played_ms(self, now: float) -> float:
        end = self.stopped_at if self.stopped_at is not None else now
        return max(0.0, min(self.queued_ms, (end - self.start) * 1000))


class Speaker:
    """Plays each response's audio in order, at real time, and stops on barge-in."""

    def __init__(self) -> None:
        self.play_end = 0.0
        self.responses: dict[str, _Playback] = {}
        self.last: str | None = None

    def on_audio(self, response_id: str, ms: float) -> None:
        now = time.monotonic()
        playback = self.responses.get(response_id)
        if playback is None:
            playback = self.responses[response_id] = _Playback(start=max(now, self.play_end))
            self.last = response_id
        playback.queued_ms += ms
        self.play_end = max(now, self.play_end) + ms / 1000

    def playing(self) -> bool:
        return time.monotonic() < self.play_end

    def stop(self) -> tuple[str | None, float | None]:
        now = time.monotonic()
        self.play_end = now
        if self.last is None:
            return None, None
        playback = self.responses[self.last]
        if playback.stopped_at is None:
            playback.stopped_at = now
        return self.last, playback.played_ms(now)


@dataclass
class SimCall:
    """Drives one conversation with clips. ``events`` holds every debugger event with its time since start."""

    conversation: Any
    barge_in: bool = True
    log: Callable[[str], None] | None = None
    on_event: Callable[[dict[str, Any]], Awaitable[None]] | None = None
    events: list[dict[str, Any]] = field(default_factory=list)
    interrupts: list[dict[str, Any]] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.speaker = Speaker()
        self.loop = VoiceLoop(self.conversation, self._sink)
        self._mic: asyncio.Queue[bytes] = asyncio.Queue()
        self._mic_task: asyncio.Task[None] | None = None
        self._loop_task: asyncio.Task[None] | None = None
        self._t0 = time.monotonic()
        self._waiters: list[tuple[Callable[[dict[str, Any]], bool], asyncio.Future[dict[str, Any]]]] = []
        self.clip_done = asyncio.Event()
        self.clip_done.set()
        self._mark = 0

    def t(self) -> float:
        return round(time.monotonic() - self._t0, 3)

    async def start(self) -> None:
        self._mic_task = asyncio.create_task(self._run_mic())
        self._loop_task = asyncio.create_task(self._run_loop())

    async def stop(self) -> None:
        for task in (self._mic_task,):
            if task:
                task.cancel()
        await self.conversation.close()
        if self._loop_task:
            try:
                await asyncio.wait_for(self._loop_task, 15)
            except Exception as exc:  # noqa: BLE001
                self._note(f"loop ended with {exc!r}")

    async def _run_loop(self) -> None:
        try:
            await self.loop.run()
        except Exception as exc:  # noqa: BLE001
            self._note(f"update loop raised {exc!r}")
            await self._sink({"kind": "LoopError", "message": repr(exc)})

    async def _run_mic(self) -> None:
        next_send = time.monotonic()
        silent = b"\x00" * FRAME_BYTES
        pending = b""
        while True:
            if not pending and not self._mic.empty():
                pending = self._mic.get_nowait()
            if pending:
                frame, pending = pending[:FRAME_BYTES], pending[FRAME_BYTES:]
                if not pending and self._mic.empty():
                    self.clip_done.set()
            else:
                frame = silent
            try:
                await self.conversation.send(frame)
            except Exception:  # noqa: BLE001
                return
            next_send += FRAME_MS / 1000
            await asyncio.sleep(max(0, next_send - time.monotonic()))

    async def say(self, clip: str, *, wait: bool = True) -> None:
        self._note(f"caller says {clip!r}")
        self.mark()
        self.clip_done.clear()
        await self._mic.put(load_clip(clip))
        if wait:
            await self.clip_done.wait()

    async def _sink(self, event: dict[str, Any]) -> None:
        event = {"t": self.t(), **event}
        kind = event["kind"]
        if kind == "AudioData":
            self.speaker.on_audio(event["responseId"], event["ms"])
        elif kind == "StopAudio":
            was_playing = self.speaker.playing()
            response_id, played = self.speaker.stop()
            if self.barge_in and response_id is not None:
                record = {
                    "t": self.t(),
                    "response_id": response_id,
                    "played_ms": int(played or 0),
                    "was_playing": was_playing,
                }
                self.interrupts.append(record)
                label = "barge-in" if was_playing else "caller spoke after playback"
                self._note(f"{label}: interrupt({response_id}, played_ms={record['played_ms']})")
                asyncio.get_running_loop().create_task(self._interrupt(record))
        if kind != "AudioData" and kind != "Update":
            self._note(f"{kind}: {event.get('data') or event.get('message') or ''}")
        if kind != "AudioData":
            self.events.append(event)
        for predicate, future in list(self._waiters):
            if not future.done() and predicate(event):
                future.set_result(event)
        if self.on_event is not None:
            await self.on_event(event)

    async def _interrupt(self, record: dict[str, Any]) -> None:
        try:
            await self.conversation.interrupt(played_ms=record["played_ms"])
        except Exception as exc:  # noqa: BLE001
            record["error"] = repr(exc)
            self._note(f"interrupt raised {exc!r}")

    def mark(self) -> None:
        """Waits after this only match events from here on."""
        self._mark = len(self.events)

    async def wait_for(self, predicate: Callable[[dict[str, Any]], bool], timeout: float = 30.0) -> dict[str, Any]:
        for index in range(self._mark, len(self.events)):
            if predicate(self.events[index]):
                self._mark = index + 1
                return self.events[index]
        future: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        self._waiters.append((predicate, future))
        try:
            event = await asyncio.wait_for(future, timeout)
            self._mark = self.events.index(event) + 1
            return event
        finally:
            self._waiters.remove((predicate, future))

    async def wait_kind(self, kind: str, timeout: float = 30.0, **match: Any) -> dict[str, Any]:
        def predicate(event: dict[str, Any]) -> bool:
            if event["kind"] != kind:
                return False
            data = event.get("data") or {}
            return all(data.get(key) == value for key, value in match.items())

        return await self.wait_for(predicate, timeout)

    async def wait_reply_done(self, timeout: float = 30.0) -> dict[str, Any]:
        """Wait for a response that finished and was spoken, then for its audio to finish playing."""
        event = await self.wait_kind("ResponseDone", timeout)
        while self.speaker.playing():
            await asyncio.sleep(0.05)
        return event

    async def settle(self, seconds: float) -> None:
        await asyncio.sleep(seconds)

    def _note(self, message: str) -> None:
        if self.log:
            self.log(f"[{self.t():7.2f}] {message}")
