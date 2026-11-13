# Copyright (c) Microsoft. All rights reserved.

"""Tests for the realtime contract types and the additive core changes they need."""

from __future__ import annotations

import pickle
from typing import Any

import pytest

from agent_framework import (
    AgentResponse,
    AgentResponseUpdate,
    ChatResponse,
    ChatResponseUpdate,
    Content,
    RealtimeEvent,
    RealtimeKind,
    UsageDetails,
    add_usage_details,
)
from agent_framework.exceptions import (
    RealtimeActiveResponseException,
    RealtimeConnectionException,
    RealtimeControlException,
    RealtimeControlTimeoutException,
)


def _speech_started_content() -> Content:
    return Content.from_realtime_event(
        "speech_started",
        service_event_type="input_audio_buffer.speech_started",
        additional_properties={"core": {"item_id": "item_1", "audio_start_ms": 120}},
    )


# region Text status


def test_from_text_sets_status() -> None:
    content = Content.from_text("Hello", status="completed")

    assert content.status == "completed"


def test_text_addition_keeps_the_final_status() -> None:
    delta = Content.from_text("Hel", merge_mode="append")
    final = Content.from_text("Hello", merge_mode="replace", status="completed")

    merged = delta + final

    assert merged.text == "Hello"
    assert merged.status == "completed"


def test_text_addition_keeps_an_earlier_status_when_the_later_has_none() -> None:
    final = Content.from_text("Hello", merge_mode="replace", status="completed")
    later = Content.from_text("!", merge_mode="append")

    merged = final + later

    assert merged.text == "Hello!"
    assert merged.status == "completed"


def test_from_updates_keeps_the_final_text_status() -> None:
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


def test_chat_response_from_updates_keeps_the_final_text_status() -> None:
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


# endregion

# region Usage


def test_add_usage_details_sums_the_modality_keys() -> None:
    first = UsageDetails(audio_input_token_count=3, text_output_token_count=5, image_cache_read_input_token_count=1)
    second = UsageDetails(audio_input_token_count=4, text_output_token_count=6, image_cache_read_input_token_count=2)

    assert add_usage_details(first, second) == {
        "audio_input_token_count": 7,
        "text_output_token_count": 11,
        "image_cache_read_input_token_count": 3,
    }


def test_from_updates_sums_the_modality_usage_keys() -> None:
    updates = [
        AgentResponseUpdate(contents=[Content.from_usage({"audio_output_token_count": 10})], role="assistant"),
        AgentResponseUpdate(contents=[Content.from_usage({"audio_output_token_count": 20})], role="assistant"),
    ]

    response = AgentResponse.from_updates(updates)

    assert response.usage_details is not None
    assert response.usage_details["audio_output_token_count"] == 30


# endregion

# region RealtimeEvent


def test_realtime_event_keeps_its_fields() -> None:
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
    core = {"item_id": "item_1", "audio_start_ms": 120}
    properties: dict[str, Any] = {"core": core}

    event = RealtimeEvent("speech_started", additional_properties=properties)
    properties["extra"] = True
    core["item_id"] = "item_2"

    assert event.additional_properties == {"core": {"item_id": "item_1", "audio_start_ms": 120}}


def test_realtime_event_accepts_an_unknown_kind() -> None:
    assert RealtimeEvent(RealtimeKind("future_kind")).kind == "future_kind"


@pytest.mark.parametrize("malformed_kind", ["", 7])
def test_realtime_event_rejects_a_malformed_kind(malformed_kind: Any) -> None:
    with pytest.raises(ValueError):
        RealtimeEvent(malformed_kind)


def test_realtime_event_repr_omits_the_raw_representation() -> None:
    event = RealtimeEvent(
        "speech_started",
        service_event_type="input_audio_buffer.speech_started",
        raw_representation={"secret_payload": "do-not-print"},
    )

    text = repr(event)

    assert "speech_started" in text
    assert "input_audio_buffer.speech_started" in text
    assert "do-not-print" not in text


# endregion

# region Realtime-event content


def test_from_realtime_event_sets_type_and_kind() -> None:
    content = Content.from_realtime_event("speech_started")

    assert content.type == "realtime_event"
    assert content.kind == "speech_started"
    assert content.service_event_type is None


def test_from_realtime_event_keeps_its_fields() -> None:
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


def test_from_realtime_event_copies_additional_properties() -> None:
    core = {"item_id": "item_1"}
    properties: dict[str, Any] = {"core": core}

    content = Content.from_realtime_event("speech_started", additional_properties=properties)
    properties["extra"] = True
    core["item_id"] = "item_2"

    assert content.additional_properties == {"core": {"item_id": "item_1"}}


def test_from_realtime_event_rejects_an_empty_kind() -> None:
    with pytest.raises(ValueError):
        Content.from_realtime_event(RealtimeKind(""))


def test_realtime_event_content_rejects_merge_mode() -> None:
    with pytest.raises(ValueError):
        Content("realtime_event", kind="speech_started", merge_mode="append")


@pytest.mark.parametrize("realtime_field", [{"kind": "output_text"}, {"service_event_type": "response.text.delta"}])
def test_other_content_rejects_realtime_fields(realtime_field: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        Content("text", text="Hello", **realtime_field)


def test_realtime_event_content_to_dict() -> None:
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
    serialized = Content.from_text("Hello").to_dict(exclude_none=False)

    assert "kind" not in serialized
    assert "service_event_type" not in serialized


def test_realtime_event_content_round_trips_through_from_dict() -> None:
    content = _speech_started_content()

    restored = Content.from_dict(content.to_dict())

    assert restored == content
    assert restored.kind == "speech_started"
    assert restored.service_event_type == "input_audio_buffer.speech_started"


def test_unknown_kind_round_trips_through_from_dict() -> None:
    content = Content.from_realtime_event(RealtimeKind("future_kind"), service_event_type="future.event")

    assert Content.from_dict(content.to_dict()).kind == "future_kind"


@pytest.mark.parametrize("serialized_kind", [{}, {"kind": ""}, {"kind": 7}])
def test_realtime_event_content_from_dict_requires_a_kind(serialized_kind: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        Content.from_dict({"type": "realtime_event", **serialized_kind})


def test_agent_response_update_dict_round_trip() -> None:
    update = AgentResponseUpdate(contents=[_speech_started_content()], role="user", message_id="item_1")

    restored = AgentResponseUpdate.from_dict(update.to_dict())

    assert restored.contents == update.contents
    assert restored.contents[0].kind == "speech_started"


def test_agent_response_update_json_round_trip() -> None:
    update = AgentResponseUpdate(contents=[_speech_started_content()], role="user", message_id="item_1")

    restored = AgentResponseUpdate.from_json(update.to_json())

    assert restored.contents == update.contents


def test_nested_realtime_event_content_rebuilds() -> None:
    nested = _speech_started_content()

    result = Content.from_dict({
        "type": "mcp_server_tool_result",
        "call_id": "call_1",
        "output": [nested.to_dict()],
    })

    assert result.output == [nested]


def test_from_updates_keeps_realtime_event_content() -> None:
    content = _speech_started_content()

    response = AgentResponse.from_updates([AgentResponseUpdate(contents=[content], role="user", message_id="item_1")])

    assert response.messages[0].contents == [content]


def test_realtime_event_content_pickle_round_trip() -> None:
    content = Content.from_realtime_event(
        "speech_started", service_event_type="input_audio_buffer.speech_started", raw_representation={"x": 1}
    )

    restored = pickle.loads(pickle.dumps(content))  # noqa: S301

    assert (restored.kind, restored.service_event_type) == ("speech_started", "input_audio_buffer.speech_started")
    assert restored.raw_representation is None


@pytest.mark.parametrize("malformed_kind", [None, "", 7])
def test_realtime_event_content_unpickled_with_a_malformed_kind_raises(malformed_kind: Any) -> None:
    state = Content.from_realtime_event("speech_started").__getstate__()
    state["kind"] = malformed_kind
    restored = Content.__new__(Content)

    with pytest.raises(ValueError):
        restored.__setstate__(state)


def test_content_unpickled_from_older_state_has_no_realtime_fields() -> None:
    state = Content.from_text("Hello").__getstate__()
    del state["kind"]
    del state["service_event_type"]
    restored = Content.__new__(Content)

    restored.__setstate__(state)

    assert restored.kind is None
    assert restored.service_event_type is None


# endregion

# region Exceptions


def test_realtime_control_exception_keeps_code_and_param() -> None:
    rejected = RealtimeControlException("Invalid voice.", code="invalid_value", param="session.voice")
    plain = RealtimeControlException("Rejected.")

    assert str(rejected) == "Invalid voice."
    assert (rejected.code, rejected.param) == ("invalid_value", "session.voice")
    assert (plain.code, plain.param) == (None, None)


def test_control_exception_subclasses_keep_code_and_param() -> None:
    busy = RealtimeActiveResponseException("Busy.", code="conversation_already_has_active_response")
    timed_out = RealtimeControlTimeoutException("No acknowledgement.")

    assert busy.code == "conversation_already_has_active_response"
    assert (timed_out.code, timed_out.param) == (None, None)


def test_realtime_connection_exception_keeps_close_code() -> None:
    closed = RealtimeConnectionException("The provider closed the connection.", close_code=1011)
    failed = RealtimeConnectionException("Couldn't connect.")

    assert str(closed) == "The provider closed the connection."
    assert closed.close_code == 1011
    assert failed.close_code is None


# endregion
