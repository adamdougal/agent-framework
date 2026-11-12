# Copyright (c) Microsoft. All rights reserved.

"""Voice history: the call's authored turns as text, in the provider's conversation order."""

from __future__ import annotations

import copy
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, TypeVar

from .._types import Content, FinishReasonLiteral, Message, UsageDetails, add_usage_details
from ._events import RealtimeEvent
from ._updates import core_attributes

if TYPE_CHECKING:
    from .._agents import BaseAgent


@dataclass
class _CallerTurn:
    item_id: str
    text: str | None = None
    status: str = "completed"
    partial: str = ""
    text_less: str = "transcript_pending"

    @property
    def settled(self) -> bool:
        return self.text is not None or self.text_less != "transcript_pending"

    def stop_waiting(self, reason: str) -> None:
        """Never invent the caller's words: keep what was transcribed so far, else say why there's no text."""
        if self.partial:
            self.text, self.status = self.partial, "incomplete"
        else:
            self.text_less = reason

    def messages(self) -> list[Message]:
        if self.text is not None:
            return [Message("user", [Content.from_text(self.text, status=self.status)], message_id=self.item_id)]
        return [
            Message("user", [], message_id=self.item_id, additional_properties={"core": {"text_less": self.text_less}})
        ]


@dataclass
class _AgentTurn:
    item_id: str
    text: str | None = None
    status: str = "completed"
    partial: str = ""
    response_id: str | None = None
    author_name: str | None = None
    heard: bool = True
    heard_fraction: float | None = None

    @property
    def settled(self) -> bool:
        """Its response settles it: the exchange waits for that response to finish."""
        return True

    def response_ended(self, status: str) -> None:
        """A response that didn't complete was cut short, so its text is only what was generated before that."""
        if status == "completed":
            return
        if self.text is None:
            self.text = self.partial
        self.status = "incomplete"

    def messages(self) -> list[Message]:
        """A cut applies to whatever text the turn has when it renders, since its final transcript can come later."""
        text = self.text
        if text is not None and self.heard_fraction is not None:
            text = _heard_words(text, self.heard_fraction)
        if not text or not self.heard:
            return []
        status = "incomplete" if self.heard_fraction is not None else self.status
        return [
            Message(
                "assistant",
                [Content.from_text(text, status=status)],
                author_name=self.author_name,
                message_id=self.item_id,
            )
        ]


@dataclass
class _CallItem:
    item_id: str
    call: Content | None = None
    result: Content | None = None
    author_name: str | None = None

    @property
    def settled(self) -> bool:
        return self.call is None or self.result is not None

    def messages(self) -> list[Message]:
        if self.call is None:
            return []
        messages = [Message("assistant", [self.call], author_name=self.author_name, message_id=self.item_id)]
        if self.result is not None:
            messages.append(Message("tool", [self.result], author_name=self.author_name))
        return messages


_Item = _CallerTurn | _AgentTurn | _CallItem
_ItemT = TypeVar("_ItemT", _CallerTurn, _AgentTurn, _CallItem)


@dataclass
class _Response:
    """A response belongs to the exchange that was latest when it was created, wherever its items land."""

    response_id: str
    finished: bool = False
    usage: UsageDetails | None = None
    finish_reason: FinishReasonLiteral | None = None

    @property
    def settled(self) -> bool:
        return self.finished

    def messages(self) -> list[Message]:
        return []


@dataclass
class Exchange:
    inputs: list[Message]
    outputs: list[Message]
    settled: bool = True
    usage: UsageDetails | None = None
    response_id: str | None = None
    finish_reason: FinishReasonLiteral | None = None
    end: int = 0

    def include(self, response: _Response) -> None:
        """As a run reports its last model call, the exchange reports its last response, with every response's usage."""
        self.usage = add_usage_details(self.usage, response.usage)
        self.response_id = response.response_id
        self.finish_reason = response.finish_reason


class CallHistory:
    def __init__(self) -> None:
        self._entries: list[_Item | _Response | Message] = []
        self._items: dict[str, _Item] = {}
        self._responses: dict[str, _Response] = {}
        self._frozen: list[Exchange] = []
        self._frozen_end = 0

    def on_item_added(self, event: RealtimeEvent, *, transcribing: bool) -> None:
        if (item := _item_for(core_attributes(event), transcribing=transcribing)) is not None:
            self._items[item.item_id] = item
            self._entries.append(item)

    def unheard(self, item_id: str) -> None:
        if isinstance(turn := self._items.get(item_id), _AgentTurn):
            turn.heard = False

    def heard_messages_after(self, response_id: str, item_id: str) -> list[str]:
        """Messages already counted as unheard are left out, so none is ever deleted twice."""
        later: list[str] = []
        found = False
        for entry in self._entries:
            if isinstance(entry, _AgentTurn | _CallerTurn | _CallItem) and entry.item_id == item_id:
                found = True
            elif found and isinstance(entry, _AgentTurn) and entry.response_id == response_id and entry.heard:
                later.append(entry.item_id)
        return later

    @property
    def waiting_for_caller_transcripts(self) -> bool:
        return bool(self._waiting_caller_turns())

    def stop_waiting_for_caller_transcripts(self) -> None:
        for turn in self._waiting_caller_turns():
            turn.stop_waiting("transcript_missing")

    def _waiting_caller_turns(self) -> list[_CallerTurn]:
        return [item for item in self._items.values() if isinstance(item, _CallerTurn) and not item.settled]

    def on_app_input(self, message: Message) -> None:
        text_only = [content for content in message.contents if not is_audio(content)]
        if not text_only:
            return
        recorded = copy.copy(message)
        recorded.contents = text_only
        self._entries.append(recorded)

    def on_input_transcription(self, event: RealtimeEvent) -> None:
        if (turn := self._item(event, _CallerTurn)) is not None:
            _take_transcript(turn, event)

    def on_input_transcription_failed(self, event: RealtimeEvent) -> None:
        if (turn := self._item(event, _CallerTurn)) is not None:
            turn.stop_waiting("transcription_failed")

    def on_output_text(self, event: RealtimeEvent, author: BaseAgent) -> None:
        if (turn := self._item(event, _AgentTurn)) is None:
            return
        turn.author_name = author.name
        turn.response_id = core_attributes(event)["response_id"]
        _take_transcript(turn, event)

    def on_response_created(self, response_id: str) -> None:
        response = _Response(response_id)
        self._responses[response_id] = response
        self._entries.append(response)

    def on_usage(self, event: RealtimeEvent) -> None:
        response = self._responses.get(core_attributes(event)["response_id"])
        if response is not None and event.content is not None:
            response.usage = event.content.usage_details

    def end_unfinished_responses(self) -> None:
        """A closed session finishes no more responses, so one still running was cut short by the close."""
        for response in self._responses.values():
            if not response.finished:
                self.on_response_done(response.response_id, "cancelled")

    def on_response_done(
        self,
        response_id: str,
        status: str,
        reason: str | None = None,
        finish_reason: FinishReasonLiteral | None = None,
    ) -> None:
        if (response := self._responses.get(response_id)) is not None:
            response.finished = True
            response.finish_reason = finish_reason
            if status == "incomplete" and reason == "content_filter":
                # The provider never finishes transcribing a turn it filtered, so waiting would hold every later
                # exchange until close.
                for turn in self._exchange_inputs(response):
                    if isinstance(turn, _CallerTurn) and not turn.settled:
                        turn.stop_waiting("transcript_missing")
        for item in self._items.values():
            if isinstance(item, _AgentTurn) and item.response_id == response_id:
                item.response_ended(status)

    def _exchange_inputs(self, response: _Response) -> list[_CallerTurn | Message]:
        inputs: list[_CallerTurn | Message] = []
        for entry in reversed(self._entries[: self._entries.index(response)]):
            if isinstance(entry, (_CallerTurn, Message)):
                inputs.append(entry)
            elif _is_reply(entry):
                break
        return inputs

    def on_item_truncated(self, event: RealtimeEvent, generated_ms: float | None) -> None:
        if (turn := self._item(event, _AgentTurn)) is not None and generated_ms:
            turn.heard_fraction = core_attributes(event)["audio_end_ms"] / generated_ms

    def on_function_call(self, event: RealtimeEvent, author: BaseAgent) -> None:
        if (call := self._item(event, _CallItem)) is not None:
            call.call = event.content
            call.author_name = author.name

    def on_tool_result(self, result: Content) -> None:
        for item in self._items.values():
            if isinstance(item, _CallItem) and item.call is not None and item.call.id == result.id:
                item.result = result

    def messages(self) -> list[Message]:
        return [message for exchange in self.exchanges() for message in (*exchange.inputs, *exchange.outputs)]

    def freeze(self, exchange: Exchange) -> None:
        """Once providers have an exchange, later events mustn't make history disagree with what they recorded."""
        if exchange.end > self._frozen_end:
            self._frozen.append(exchange)
            self._frozen_end = exchange.end

    def exchanges(self) -> list[Exchange]:
        """An input that follows the agent's reply starts the next exchange."""
        exchanges: list[Exchange] = []
        replied = False
        for position, entry in enumerate(self._entries[self._frozen_end :], self._frozen_end):
            is_input = isinstance(entry, (Message, _CallerTurn))
            if not exchanges or (is_input and replied):
                exchanges.append(Exchange([], []))
                replied = False
            messages = [entry] if isinstance(entry, Message) else entry.messages()
            (exchanges[-1].inputs if is_input else exchanges[-1].outputs).extend(messages)
            replied = replied or (not is_input and _is_reply(entry))
            if not isinstance(entry, Message) and not entry.settled:
                exchanges[-1].settled = False
            if isinstance(entry, _Response):
                exchanges[-1].include(entry)
            exchanges[-1].end = position + 1
        return [*self._frozen, *exchanges]

    def _item(self, event: RealtimeEvent, item_type: type[_ItemT]) -> _ItemT | None:
        item = self._items.get(core_attributes(event)["item_id"])
        return item if isinstance(item, item_type) else None


def _item_for(item: dict[str, Any], *, transcribing: bool) -> _Item | None:
    """The caller's audio, the agent's messages and calls are the call's own turns.

    Other user and system items are the echoes of app input, which is recorded as it's sent, or of replayed history.
    """
    role = item.get("role")
    if role == "user" and item.get("input_audio"):
        return _CallerTurn(item["item_id"], text_less="transcript_pending" if transcribing else "transcription_off")
    if role == "assistant":
        return _AgentTurn(item["item_id"])
    if role is None:
        return _CallItem(item["item_id"])
    return None


def _is_reply(entry: _Item | _Response) -> bool:
    """A response is a reply even when it says nothing, such as one a content filter stopped."""
    return isinstance(entry, _Response) or bool(entry.messages())


def _take_transcript(turn: _CallerTurn | _AgentTurn, transcript: RealtimeEvent) -> None:
    """Only the completed transcript is the turn's text; deltas build the partial text a cut-short turn keeps."""
    if transcript.content is None:
        return
    if transcript.content.status == "completed":
        turn.text = transcript.content.text
    else:
        turn.partial += transcript.content.text or ""


def is_audio(content: Content) -> bool:
    return content.type in ("data", "uri") and content.has_top_level_media_type("audio")


def _heard_words(text: str, heard_fraction: float) -> str:
    """Speech isn't word-timed, so the heard share of the audio is taken as the same share of the text."""
    target = len(text) * heard_fraction
    heard = ""
    for word in text.split(" "):
        longer = f"{heard} {word}" if heard else word
        if abs(len(longer) - target) > abs(len(heard) - target):
            break
        heard = longer
    return heard
