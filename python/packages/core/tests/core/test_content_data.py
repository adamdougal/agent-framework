# Copyright (c) Microsoft. All rights reserved.

"""Reading the bytes, or the base64 payload, that data content carries."""

import pytest

from agent_framework import Content
from agent_framework.exceptions import ContentError

AUDIO_URL = "https://example.com/reply.wav"


def test_get_data_bytes_returns_the_bytes_the_content_was_made_from():
    content = Content.from_data(b"\x00\x01", "audio/pcm")

    assert content.get_data_bytes() == b"\x00\x01"


def test_get_data_base64_returns_the_base64_payload_without_decoding_it():
    content = Content.from_data(b"\x00\x01", "audio/pcm")

    assert content.get_data_base64() == "AAE="


def test_get_data_bytes_raises_for_content_without_a_data_uri():
    content = Content.from_uri(AUDIO_URL, media_type="audio/wav")

    with pytest.raises(ContentError):
        content.get_data_bytes()


def test_get_data_base64_raises_for_content_without_a_data_uri():
    content = Content.from_uri(AUDIO_URL, media_type="audio/wav")

    with pytest.raises(ContentError):
        content.get_data_base64()


class TestVerification:
    @pytest.mark.parametrize("read", [Content.get_data_bytes, Content.get_data_base64])
    def test_both_readers_raise_for_content_with_no_uri(self, read):
        with pytest.raises(ContentError):
            read(Content.from_text("Hi"))

    @pytest.mark.parametrize("read", [Content.get_data_bytes, Content.get_data_base64])
    def test_both_readers_raise_for_a_data_uri_that_is_not_base64(self, read):
        with pytest.raises(ContentError, match="base64 encoding"):
            read(Content.from_uri("data:text/plain,hi"))

    def test_get_data_bytes_raises_for_a_payload_that_is_not_valid_base64(self):
        content = Content.from_uri("data:audio/pcm;base64,!!invalid!!")

        with pytest.raises(ContentError, match="Failed to decode"):
            content.get_data_bytes()

    def test_both_readers_read_a_data_uri_with_media_type_parameters(self):
        content = Content.from_uri("data:audio/pcm;rate=24000;base64,AAE=")

        assert (content.get_data_bytes(), content.get_data_base64()) == (b"\x00\x01", "AAE=")
