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
    waiting: bool
    text: str | None = None
    status: str = "completed"
    partial: str = ""

    @property
    def pending(self) -> bool:
        return self.waiting and self.text is None

    def stop_waiting(self) -> None:
        if self.partial:
            self.text, self.status = self.partial, "incomplete"
        self.waiting = False

    def messages(self) -> list[Message]:
        if self.text is not None:
            return [Message("user", [Content.from_text(self.text, status=self.status)], message_id=self.item_id)]
        return [Message("user", [], message_id=self.item_id, additional_properties={"core": {"text_less": True}})]


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

    def response_ended(self, status: str) -> None:
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

    def messages(self) -> list[Message]:
        return []


@dataclass
class Exchange:
    inputs: list[Message]
    outputs: list[Message]
    usage: UsageDetails | None = None
    response_id: str | None = None
    finish_reason: FinishReasonLiteral | None = None

    def include(self, response: _Response) -> None:
        self.usage = add_usage_details(self.usage, response.usage)
        self.response_id = response.response_id
        self.finish_reason = response.finish_reason


class CallHistory:
    def __init__(self) -> None:
        self._entries: list[_Item | _Response | Message] = []
        self._items: dict[str, _Item] = {}
        self._responses: dict[str, _Response] = {}

    def on_item_added(self, event: RealtimeEvent, *, transcribing: bool) -> None:
        if (item := _item_for(core_attributes(event), transcribing=transcribing)) is not None:
            self._items[item.item_id] = item
            self._entries.append(item)

    def unheard(self, item_id: str) -> None:
        if isinstance(turn := self._items.get(item_id), _AgentTurn):
            turn.heard = False

    def heard_messages_after(self, response_id: str, item_id: str) -> list[str]:
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
            turn.stop_waiting()

    def _waiting_caller_turns(self) -> list[_CallerTurn]:
        return [item for item in self._items.values() if isinstance(item, _CallerTurn) and item.pending]

    def on_app_input(self, message: Message) -> None:
        recorded = copy.copy(message)
        recorded.contents = [content for content in message.contents if content.type == "text"]
        if recorded.contents:
            self._entries.append(copy.deepcopy(recorded))

    def on_input_transcription(self, event: RealtimeEvent) -> None:
        if (turn := self._item(event, _CallerTurn)) is not None:
            _take_transcript(turn, event)

    def on_input_transcription_failed(self, event: RealtimeEvent) -> None:
        if (turn := self._item(event, _CallerTurn)) is not None:
            turn.stop_waiting()

    def on_output_text(self, event: RealtimeEvent, author: BaseAgent) -> None:
        if (turn := self._item(event, _AgentTurn)) is None:
            return
        turn.author_name = author.name
        turn.response_id = core_attributes(event)["response_id"]
        _take_transcript(turn, event)

    def on_response_created(self, event: RealtimeEvent) -> None:
        core = core_attributes(event)
        if core.get("out_of_band"):
            return
        response_id = core["response_id"]
        response = _Response(response_id)
        self._responses[response_id] = response
        self._entries.append(response)

    def on_usage(self, event: RealtimeEvent) -> None:
        response = self._responses.get(core_attributes(event)["response_id"])
        if response is not None and event.content is not None:
            response.usage = event.content.usage_details

    def end_unfinished_responses(self) -> None:
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
                # The provider never finishes transcribing a turn it filtered, so close would wait for nothing.
                for turn in self._exchange_inputs(response):
                    if isinstance(turn, _CallerTurn) and turn.pending:
                        turn.stop_waiting()
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
        recorded = copy.copy(result)
        recorded.items = [part for part in result.items or [] if part.type == "text"]
        for item in self._items.values():
            if isinstance(item, _CallItem) and item.call is not None and item.call.id == result.id:
                item.result = recorded

    def messages(self) -> list[Message]:
        return [message for exchange in self.exchanges() for message in (*exchange.inputs, *exchange.outputs)]

    def exchanges(self) -> list[Exchange]:
        exchanges: list[Exchange] = []
        replied = False
        for entry in self._entries:
            is_input = isinstance(entry, (Message, _CallerTurn))
            if not exchanges or (is_input and replied):
                exchanges.append(Exchange([], []))
                replied = False
            messages = [entry] if isinstance(entry, Message) else entry.messages()
            (exchanges[-1].inputs if is_input else exchanges[-1].outputs).extend(messages)
            replied = replied or (not is_input and _is_reply(entry))
            if isinstance(entry, _Response):
                exchanges[-1].include(entry)
        # Echoes of replayed history render nothing, and on their own they're no exchange of this call.
        return [exchange for exchange in exchanges if exchange.inputs or exchange.outputs or exchange.response_id]

    def _item(self, event: RealtimeEvent, item_type: type[_ItemT]) -> _ItemT | None:
        item = self._items.get(core_attributes(event)["item_id"])
        return item if isinstance(item, item_type) else None


def _item_for(item: dict[str, Any], *, transcribing: bool) -> _Item | None:
    """Other user and system items echo app input or replay, which mustn't be recorded twice."""
    role = item.get("role")
    if role == "user" and item.get("input_audio"):
        return _CallerTurn(item["item_id"], waiting=transcribing)
    if role == "assistant":
        return _AgentTurn(item["item_id"])
    if role is None:
        return _CallItem(item["item_id"])
    return None


def _is_reply(entry: _Item | _Response) -> bool:
    """A response is a reply even when it says nothing, such as one a content filter stopped."""
    return isinstance(entry, _Response) or bool(entry.messages())


def _take_transcript(turn: _CallerTurn | _AgentTurn, transcript: RealtimeEvent) -> None:
    if transcript.content is None:
        return
    if transcript.content.status == "completed":
        turn.text = transcript.content.text
    else:
        turn.partial += transcript.content.text or ""


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
