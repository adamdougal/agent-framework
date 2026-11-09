# Copyright (c) Microsoft. All rights reserved.

"""Tests for the realtime contract types and the additive core changes they need (spec 006)."""

from __future__ import annotations

from collections.abc import AsyncIterable, Sequence
from types import TracebackType
from typing import TYPE_CHECKING, Any, Self

import pytest

if TYPE_CHECKING:
    from agent_framework import Message, RealtimeEvent

# region Driving tests


def test_experimental_feature_has_realtime() -> None:
    from agent_framework import ExperimentalFeature

    assert ExperimentalFeature.REALTIME.value == "REALTIME"


def test_experimental_feature_has_foundry_voice_agents() -> None:
    from agent_framework import ExperimentalFeature

    assert ExperimentalFeature.FOUNDRY_VOICE_AGENTS.value == "FOUNDRY_VOICE_AGENTS"


def test_from_text_sets_status() -> None:
    from agent_framework import Content

    content = Content.from_text("Hello", status="completed")

    assert content.status == "completed"


def test_text_addition_keeps_the_final_status() -> None:
    from agent_framework import Content

    delta = Content.from_text("Hel", merge_mode="append")
    final = Content.from_text("Hello", merge_mode="replace", status="completed")

    merged = delta + final

    assert merged.text == "Hello"
    assert merged.status == "completed"


def test_text_addition_keeps_an_earlier_status_when_the_later_has_none() -> None:
    from agent_framework import Content

    final = Content.from_text("Hello", merge_mode="replace", status="completed")
    later = Content.from_text("!", merge_mode="append")

    merged = final + later

    assert merged.text == "Hello!"
    assert merged.status == "completed"


def test_from_updates_keeps_the_final_text_status() -> None:
    from agent_framework import AgentResponse, AgentResponseUpdate, Content

    updates = [
        AgentResponseUpdate(
            contents=[Content.from_text("Hel", merge_mode="append")], role="assistant", message_id="item_1"
        ),
        AgentResponseUpdate(
            contents=[Content.from_text("lo", merge_mode="append")], role="assistant", message_id="item_1"
        ),
        AgentResponseUpdate(
            contents=[Content.from_text("Hello", merge_mode="replace", status="completed")],
            role="assistant",
            message_id="item_1",
        ),
    ]

    response = AgentResponse.from_updates(updates)

    [message] = response.messages
    [content] = message.contents
    assert content.text == "Hello"
    assert content.status == "completed"


def test_from_updates_keeps_an_earlier_text_status_when_the_later_has_none() -> None:
    from agent_framework import AgentResponse, AgentResponseUpdate, Content

    updates = [
        AgentResponseUpdate(
            contents=[Content.from_text("Hello", merge_mode="replace", status="completed")],
            role="assistant",
            message_id="item_1",
        ),
        AgentResponseUpdate(
            contents=[Content.from_text("!", merge_mode="append")], role="assistant", message_id="item_1"
        ),
    ]

    response = AgentResponse.from_updates(updates)

    [message] = response.messages
    [content] = message.contents
    assert content.text == "Hello!"
    assert content.status == "completed"


def test_usage_details_declares_the_modality_keys() -> None:
    from agent_framework import UsageDetails

    modality_keys = {
        f"{modality}_{count}"
        for modality in ("text", "audio", "image")
        for count in ("input_token_count", "output_token_count", "cache_read_input_token_count")
    }

    assert modality_keys <= UsageDetails.__optional_keys__
    assert not modality_keys & UsageDetails.__required_keys__


def test_realtime_kind_literal_lists_the_known_kinds() -> None:
    from typing import get_args

    from agent_framework import RealtimeKindLiteral

    kinds = get_args(RealtimeKindLiteral)

    assert len(kinds) == len(set(kinds))
    assert set(kinds) == {
        "session_created",
        "session_updated",
        "speech_started",
        "speech_stopped",
        "response_created",
        "response_done",
        "item_added",
        "item_truncated",
        "input_transcription",
        "input_transcription_failed",
        "output_text",
        "output_audio",
        "function_call",
        "function_result",
        "function_approval_request",
        "handoff_completed",
        "usage",
        "error",
        "passthrough",
    }


def test_realtime_kind_is_an_open_string_type() -> None:
    from agent_framework import RealtimeKind

    kind = RealtimeKind("future_kind")

    assert kind == "future_kind"
    assert isinstance(kind, str)
    assert RealtimeKind.__name__ == "RealtimeKind"


def test_realtime_event_defaults() -> None:
    from agent_framework import RealtimeEvent

    event = RealtimeEvent("speech_started")

    assert event.kind == "speech_started"
    assert event.content is None
    assert event.service_event_type is None
    assert event.additional_properties == {}
    assert event.raw_representation is None


def test_realtime_event_keeps_its_fields() -> None:
    from agent_framework import Content, RealtimeEvent

    content = Content.from_text("Hel", merge_mode="append")
    wire = {"type": "response.output_audio_transcript.delta", "delta": "Hel"}

    event = RealtimeEvent(
        "output_text",
        content=content,
        service_event_type="response.output_audio_transcript.delta",
        additional_properties={"core": {"response_id": "resp_1", "item_id": "item_1", "content_index": 0}},
        raw_representation=wire,
    )

    assert event.kind == "output_text"
    assert event.content is content
    assert event.service_event_type == "response.output_audio_transcript.delta"
    assert event.additional_properties == {"core": {"response_id": "resp_1", "item_id": "item_1", "content_index": 0}}
    assert event.raw_representation is wire


def test_realtime_event_copies_additional_properties() -> None:
    from agent_framework import RealtimeEvent

    core = {"item_id": "item_1", "audio_start_ms": 120}
    properties: dict[str, Any] = {"core": core}

    event = RealtimeEvent("speech_started", additional_properties=properties)
    properties["extra"] = True
    core["item_id"] = "item_2"

    assert event.additional_properties == {"core": {"item_id": "item_1", "audio_start_ms": 120}}


def test_realtime_event_rejects_an_empty_kind() -> None:
    from agent_framework import RealtimeEvent, RealtimeKind

    with pytest.raises(ValueError):
        RealtimeEvent(RealtimeKind(""))


def test_realtime_event_rejects_a_kind_that_is_not_a_string() -> None:
    from typing import cast

    from agent_framework import RealtimeEvent

    with pytest.raises(ValueError):
        RealtimeEvent(cast(Any, 7))


def test_realtime_event_repr_omits_the_raw_representation() -> None:
    from agent_framework import RealtimeEvent

    event = RealtimeEvent(
        "speech_started",
        service_event_type="input_audio_buffer.speech_started",
        raw_representation={"secret_payload": "do-not-print"},
    )

    text = repr(event)

    assert "speech_started" in text
    assert "input_audio_buffer.speech_started" in text
    assert "do-not-print" not in text


def test_realtime_event_is_experimental() -> None:
    from agent_framework import RealtimeEvent

    assert getattr(RealtimeEvent, "__feature_id__", None) == "REALTIME"


def test_realtime_event_is_a_content_type() -> None:
    from typing import get_args

    from agent_framework._types import ContentType  # pyright: ignore[reportPrivateUsage]

    assert "realtime_event" in get_args(ContentType)


def test_from_realtime_event_sets_type_and_kind() -> None:
    from agent_framework import Content

    content = Content.from_realtime_event("speech_started")

    assert content.type == "realtime_event"
    assert content.kind == "speech_started"
    assert content.service_event_type is None


def test_from_realtime_event_keeps_its_fields() -> None:
    from agent_framework import Content

    wire = {"type": "input_audio_buffer.speech_started", "audio_start_ms": 120, "item_id": "item_1"}

    content = Content.from_realtime_event(
        "speech_started",
        service_event_type="input_audio_buffer.speech_started",
        additional_properties={
            "core": {"item_id": "item_1", "audio_start_ms": 120},
            "openai": {"event_id": "event_1"},
        },
        raw_representation=wire,
    )

    assert content.service_event_type == "input_audio_buffer.speech_started"
    assert content.additional_properties == {
        "core": {"item_id": "item_1", "audio_start_ms": 120},
        "openai": {"event_id": "event_1"},
    }
    assert content.raw_representation is wire


def test_from_realtime_event_rejects_an_empty_kind() -> None:
    from agent_framework import Content, RealtimeKind

    with pytest.raises(ValueError):
        Content.from_realtime_event(RealtimeKind(""))


def test_from_realtime_event_is_experimental() -> None:
    from agent_framework import Content

    assert getattr(Content.from_realtime_event, "__feature_id__", None) == "REALTIME"


def test_realtime_event_content_to_dict() -> None:
    from agent_framework import Content

    content = Content.from_realtime_event(
        "speech_started",
        service_event_type="input_audio_buffer.speech_started",
        additional_properties={"core": {"item_id": "item_1", "audio_start_ms": 120}},
        raw_representation={"type": "input_audio_buffer.speech_started"},
    )

    assert content.to_dict() == {
        "type": "realtime_event",
        "kind": "speech_started",
        "service_event_type": "input_audio_buffer.speech_started",
        "additional_properties": {"core": {"item_id": "item_1", "audio_start_ms": 120}},
    }


def test_other_content_does_not_serialize_realtime_fields() -> None:
    from agent_framework import Content

    serialized = Content.from_text("Hello").to_dict(exclude_none=False)

    assert "kind" not in serialized
    assert "service_event_type" not in serialized


@pytest.mark.parametrize("serialized_kind", [{}, {"kind": ""}, {"kind": 7}])
def test_realtime_event_content_from_dict_requires_a_kind(serialized_kind: dict[str, Any]) -> None:
    from agent_framework import Content

    with pytest.raises(ValueError):
        Content.from_dict({"type": "realtime_event", **serialized_kind})


@pytest.mark.parametrize("malformed_kind", [None, "", 7])
def test_realtime_event_content_unpickled_with_a_malformed_kind_raises(malformed_kind: Any) -> None:
    from agent_framework import Content

    state = Content.from_realtime_event("speech_started").__getstate__()
    state["kind"] = malformed_kind
    restored = Content.__new__(Content)

    with pytest.raises(ValueError):
        restored.__setstate__(state)


def test_content_unpickled_from_older_state_has_no_realtime_fields() -> None:
    from agent_framework import Content

    state = Content.from_text("Hello").__getstate__()
    del state["kind"]
    del state["service_event_type"]
    restored = Content.__new__(Content)

    restored.__setstate__(state)

    assert restored.kind is None
    assert restored.service_event_type is None


_ATTRIBUTE_TYPED_DICT_KEYS: list[tuple[str, set[str], set[str]]] = [
    ("RealtimeSessionCreatedAttributes", {"session_id"}, {"model", "expires_at"}),
    ("RealtimeSpeechStartedAttributes", {"item_id", "audio_start_ms"}, set()),
    ("RealtimeSpeechStoppedAttributes", {"item_id", "audio_end_ms"}, set()),
    ("RealtimeResponseCreatedAttributes", {"response_id"}, set()),
    ("RealtimeResponseDoneAttributes", {"response_id", "status"}, {"reason", "error_code", "error_message"}),
    ("RealtimeItemAddedAttributes", {"item_id", "previous_item_id"}, {"role"}),
    ("RealtimeItemTruncatedAttributes", {"item_id", "content_index", "audio_end_ms", "truncated_by"}, set()),
    ("RealtimeInputTranscriptionAttributes", {"item_id", "content_index"}, set()),
    (
        "RealtimeInputTranscriptionFailedAttributes",
        {"item_id", "content_index"},
        {"error_code", "error_message"},
    ),
    ("RealtimeOutputTextAttributes", {"response_id", "item_id", "content_index"}, set()),
    ("RealtimeOutputAudioAttributes", {"response_id", "item_id", "content_index"}, set()),
    ("RealtimeFunctionCallAttributes", {"response_id", "item_id"}, set()),
    ("RealtimeHandoffCompletedAttributes", {"source", "target"}, set()),
    ("RealtimeUsageAttributes", {"response_id"}, set()),
    ("RealtimeErrorAttributes", set(), {"response_id"}),
]


@pytest.mark.parametrize(("name", "required", "optional"), _ATTRIBUTE_TYPED_DICT_KEYS)
def test_attribute_typed_dicts_declare_the_specified_keys(name: str, required: set[str], optional: set[str]) -> None:
    import agent_framework

    typed_dict = getattr(agent_framework, name)

    assert typed_dict.__required_keys__ == required
    assert typed_dict.__optional_keys__ == optional


@pytest.mark.parametrize("name", [name for name, _, _ in _ATTRIBUTE_TYPED_DICT_KEYS])
def test_attribute_typed_dicts_are_experimental(name: str) -> None:
    import agent_framework

    assert getattr(getattr(agent_framework, name), "__feature_id__", None) == "REALTIME"


_OPTIONS_TYPED_DICT_KEYS: list[tuple[str, set[str]]] = [
    (
        "RealtimeOptions",
        {
            "instructions",
            "voice",
            "tool_choice",
            "temperature",
            "max_tokens",
            "metadata",
            "input_audio_format",
            "output_audio_format",
            "output_modalities",
            "turn_detection",
            "transcription",
            "noise_reduction",
        },
    ),
    (
        "RealtimeTurnDetection",
        {"type", "create_response", "interrupt_response", "threshold", "prefix_padding_ms", "silence_duration_ms"},
    ),
    ("RealtimeTranscription", {"model", "language"}),
    ("RealtimeNoiseReduction", {"type"}),
]


@pytest.mark.parametrize(("name", "keys"), _OPTIONS_TYPED_DICT_KEYS)
def test_options_typed_dicts_declare_the_specified_keys(name: str, keys: set[str]) -> None:
    import agent_framework

    typed_dict = getattr(agent_framework, name)

    assert typed_dict.__optional_keys__ == keys
    assert typed_dict.__required_keys__ == set()


@pytest.mark.parametrize("name", [name for name, _ in _OPTIONS_TYPED_DICT_KEYS])
def test_options_typed_dicts_are_experimental(name: str) -> None:
    import agent_framework

    assert getattr(getattr(agent_framework, name), "__feature_id__", None) == "REALTIME"


def test_realtime_client_protocol_is_runtime_checkable() -> None:
    from agent_framework import RealtimeClientProtocol

    class FakeClient:
        async def start_session(self, *, options: Any = None, tools: Any = None) -> Any:
            raise NotImplementedError

    class NotAClient:
        pass

    assert isinstance(FakeClient(), RealtimeClientProtocol)
    assert not isinstance(NotAClient(), RealtimeClientProtocol)


class _SessionWithoutTruncate:
    options: Any = None

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        return None

    async def send(self, value: RealtimeEvent | Message) -> None:
        return None

    def receive(self) -> AsyncIterable[RealtimeEvent]:
        raise NotImplementedError

    async def update_session(self, options: Any, tools: Sequence[Any] | None) -> None:
        return None

    async def create_response(self, options: Any = None) -> str:
        return "resp_1"

    async def say(self, text: str) -> str:
        return "resp_1"

    async def cancel_response(self) -> None:
        return None

    async def close(self) -> None:
        return None


class _FakeSession(_SessionWithoutTruncate):
    async def truncate(self, item_id: str, content_index: int, audio_end_ms: int) -> None:
        return None


def test_realtime_client_session_protocol_is_runtime_checkable() -> None:
    from agent_framework import RealtimeClientSessionProtocol

    assert isinstance(_FakeSession(), RealtimeClientSessionProtocol)
    assert not isinstance(_SessionWithoutTruncate(), RealtimeClientSessionProtocol)


def test_realtime_client_session_send_takes_an_event_or_a_message() -> None:
    from typing import get_type_hints

    from agent_framework import Message, RealtimeClientSessionProtocol, RealtimeEvent

    hints = get_type_hints(RealtimeClientSessionProtocol.send)

    assert hints["value"] == RealtimeEvent | Message


@pytest.mark.parametrize("name", ["RealtimeClientProtocol", "RealtimeClientSessionProtocol"])
def test_realtime_client_protocols_are_experimental(name: str) -> None:
    import agent_framework

    # Protocols carry the stage only in their docstring, so isinstance checks keep working.
    assert ".. warning:: Experimental" in (getattr(agent_framework, name).__doc__ or "")


def test_realtime_exception_is_an_agent_framework_exception() -> None:
    from agent_framework.exceptions import AgentFrameworkException, RealtimeException

    assert issubclass(RealtimeException, AgentFrameworkException)


_REALTIME_EXCEPTION_PARENTS: list[tuple[str, str]] = [
    ("RealtimeConnectionException", "RealtimeException"),
    ("RealtimeControlException", "RealtimeException"),
    ("RealtimeControlTimeoutException", "RealtimeControlException"),
    ("RealtimeActiveResponseException", "RealtimeControlException"),
    ("RealtimeUnsupportedException", "RealtimeException"),
    ("RealtimeConversationClosedException", "RealtimeException"),
    ("RealtimeBackpressureException", "RealtimeException"),
]


@pytest.mark.parametrize(("name", "parent"), _REALTIME_EXCEPTION_PARENTS)
def test_realtime_exception_hierarchy(name: str, parent: str) -> None:
    from agent_framework import exceptions

    assert getattr(exceptions, parent) in getattr(exceptions, name).__bases__


def test_realtime_control_exception_keeps_code_and_param() -> None:
    from agent_framework.exceptions import RealtimeControlException

    rejected = RealtimeControlException("Invalid voice.", code="invalid_value", param="session.voice")
    plain = RealtimeControlException("Rejected.")

    assert str(rejected) == "Invalid voice."
    assert (rejected.code, rejected.param) == ("invalid_value", "session.voice")
    assert (plain.code, plain.param) == (None, None)


def test_realtime_connection_exception_keeps_close_code() -> None:
    from agent_framework.exceptions import RealtimeConnectionException

    closed = RealtimeConnectionException("The provider closed the connection.", close_code=1011)
    failed = RealtimeConnectionException("Couldn't connect.")

    assert str(closed) == "The provider closed the connection."
    assert closed.close_code == 1011
    assert failed.close_code is None


@pytest.mark.parametrize("name", ["RealtimeException", *(name for name, _ in _REALTIME_EXCEPTION_PARENTS)])
def test_realtime_exceptions_are_experimental(name: str) -> None:
    from agent_framework import exceptions

    # vars(), not getattr(): a subclass would otherwise inherit its parent's stage metadata.
    assert vars(getattr(exceptions, name)).get("__feature_id__") == "REALTIME"


_PUBLIC_REALTIME_NAMES = [
    "RealtimeKind",
    "RealtimeKindLiteral",
    "RealtimeEvent",
    *(name for name, _, _ in _ATTRIBUTE_TYPED_DICT_KEYS),
    *(name for name, _ in _OPTIONS_TYPED_DICT_KEYS),
    "RealtimeClientProtocol",
    "RealtimeClientSessionProtocol",
]


def test_realtime_names_are_root_exports() -> None:
    import agent_framework

    assert [name for name in _PUBLIC_REALTIME_NAMES if name not in agent_framework.__all__] == []
    assert all(getattr(agent_framework, name) is not None for name in _PUBLIC_REALTIME_NAMES)


# endregion

# region Verification tests


def _speech_started_content() -> Any:
    from agent_framework import Content

    return Content.from_realtime_event(
        "speech_started",
        service_event_type="input_audio_buffer.speech_started",
        additional_properties={"core": {"item_id": "item_1", "audio_start_ms": 120}},
    )


class TestVerification:
    def test_realtime_event_content_round_trips_through_from_dict(self) -> None:
        from agent_framework import Content

        content = _speech_started_content()

        restored = Content.from_dict(content.to_dict())

        assert restored == content
        assert restored.kind == "speech_started"
        assert restored.service_event_type == "input_audio_buffer.speech_started"

    def test_from_text_without_status_has_none(self) -> None:
        from agent_framework import Content

        assert Content.from_text("Hello").status is None

    def test_chat_response_from_updates_keeps_the_final_text_status(self) -> None:
        from agent_framework import ChatResponse, ChatResponseUpdate, Content

        updates = [
            ChatResponseUpdate(
                contents=[Content.from_text("Hel", merge_mode="append")], role="assistant", message_id="item_1"
            ),
            ChatResponseUpdate(
                contents=[Content.from_text("Hello", merge_mode="replace", status="completed")],
                role="assistant",
                message_id="item_1",
            ),
        ]

        [content] = ChatResponse.from_updates(updates).messages[0].contents

        assert (content.text, content.status) == ("Hello", "completed")

    def test_add_usage_details_sums_the_modality_keys(self) -> None:
        from agent_framework import UsageDetails, add_usage_details

        first = UsageDetails(audio_input_token_count=3, text_output_token_count=5, image_cache_read_input_token_count=1)
        second = UsageDetails(
            audio_input_token_count=4, text_output_token_count=6, image_cache_read_input_token_count=2
        )

        assert add_usage_details(first, second) == {
            "audio_input_token_count": 7,
            "text_output_token_count": 11,
            "image_cache_read_input_token_count": 3,
        }

    def test_from_updates_sums_the_modality_usage_keys(self) -> None:
        from agent_framework import AgentResponse, AgentResponseUpdate, Content

        updates = [
            AgentResponseUpdate(contents=[Content.from_usage({"audio_output_token_count": 10})], role="assistant"),
            AgentResponseUpdate(contents=[Content.from_usage({"audio_output_token_count": 20})], role="assistant"),
        ]

        response = AgentResponse.from_updates(updates)

        assert response.usage_details is not None
        assert response.usage_details["audio_output_token_count"] == 30

    def test_realtime_event_accepts_an_unknown_kind(self) -> None:
        from agent_framework import RealtimeEvent, RealtimeKind

        assert RealtimeEvent(RealtimeKind("future_kind")).kind == "future_kind"

    def test_from_realtime_event_copies_additional_properties(self) -> None:
        from agent_framework import Content

        core = {"item_id": "item_1"}
        properties: dict[str, Any] = {"core": core}

        content = Content.from_realtime_event("speech_started", additional_properties=properties)
        properties["extra"] = True
        core["item_id"] = "item_2"

        assert content.additional_properties == {"core": {"item_id": "item_1"}}

    def test_realtime_event_content_rejects_merge_mode(self) -> None:
        from agent_framework import Content

        with pytest.raises(ValueError):
            Content("realtime_event", kind="speech_started", merge_mode="append")

    def test_unknown_kind_round_trips_through_from_dict(self) -> None:
        from agent_framework import Content, RealtimeKind

        content = Content.from_realtime_event(RealtimeKind("future_kind"), service_event_type="future.event")

        assert Content.from_dict(content.to_dict()).kind == "future_kind"

    def test_agent_response_update_dict_round_trip(self) -> None:
        from agent_framework import AgentResponseUpdate

        update = AgentResponseUpdate(contents=[_speech_started_content()], role="user", message_id="item_1")

        restored = AgentResponseUpdate.from_dict(update.to_dict())

        assert restored.contents == update.contents
        assert restored.contents[0].kind == "speech_started"

    def test_agent_response_update_json_round_trip(self) -> None:
        from agent_framework import AgentResponseUpdate

        update = AgentResponseUpdate(contents=[_speech_started_content()], role="user", message_id="item_1")

        restored = AgentResponseUpdate.from_json(update.to_json())

        assert restored.contents == update.contents

    def test_nested_realtime_event_content_rebuilds(self) -> None:
        from agent_framework import Content

        nested = _speech_started_content()

        result = Content.from_dict({
            "type": "mcp_server_tool_result",
            "call_id": "call_1",
            "output": [nested.to_dict()],
        })

        assert result.output == [nested]

    def test_realtime_event_content_pickle_round_trip(self) -> None:
        import pickle

        from agent_framework import Content

        content = Content.from_realtime_event(
            "speech_started", service_event_type="input_audio_buffer.speech_started", raw_representation={"x": 1}
        )

        restored = pickle.loads(pickle.dumps(content))  # noqa: S301

        assert (restored.kind, restored.service_event_type) == ("speech_started", "input_audio_buffer.speech_started")
        assert restored.raw_representation is None

    def test_from_updates_keeps_realtime_event_content(self) -> None:
        from agent_framework import AgentResponse, AgentResponseUpdate

        content = _speech_started_content()

        response = AgentResponse.from_updates([
            AgentResponseUpdate(contents=[content], role="user", message_id="item_1")
        ])

        assert response.messages[0].contents == [content]

    def test_control_exception_subclasses_keep_code_and_param(self) -> None:
        from agent_framework.exceptions import RealtimeActiveResponseException, RealtimeControlTimeoutException

        busy = RealtimeActiveResponseException("Busy.", code="conversation_already_has_active_response")
        timed_out = RealtimeControlTimeoutException("No acknowledgement.")

        assert busy.code == "conversation_already_has_active_response"
        assert (timed_out.code, timed_out.param) == (None, None)

    def test_wildcard_import_exposes_realtime_names(self) -> None:
        namespace: dict[str, object] = {}

        exec("from agent_framework import *", namespace)  # noqa: S102

        assert [name for name in _PUBLIC_REALTIME_NAMES if name not in namespace] == []


# endregion
