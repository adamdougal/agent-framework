# Copyright (c) Microsoft. All rights reserved.

"""What a live conversation asked the app, and the app's answers to it."""

from __future__ import annotations

import asyncio
import copy
from collections.abc import Sequence
from typing import NamedTuple

from .._types import Content


class _AppRunCall(NamedTuple):
    call_id: str
    answer: asyncio.Future[Content]


class AppInput:
    """The conversation's own record of what it asked the app, so only answers to those requests are accepted."""

    def __init__(self) -> None:
        self._approvals: dict[str, asyncio.Future[bool]] = {}
        # Keyed by the call's occurrence id, since a provider can reuse a call id.
        self._results: dict[str, _AppRunCall] = {}

    def approval(self, request_id: str) -> asyncio.Future[bool]:
        decision: asyncio.Future[bool] = asyncio.get_running_loop().create_future()
        self._approvals[request_id] = decision
        return decision

    def result(self, call: Content) -> asyncio.Future[Content]:
        answer: asyncio.Future[Content] = asyncio.get_running_loop().create_future()
        self._results[call.id or ""] = _AppRunCall(call.call_id or "", answer)
        return answer

    def withdraw(self, occurrence_id: str) -> None:
        if (waiting := self._results.pop(occurrence_id, None)) is not None:
            waiting.answer.cancel()

    def _request_answered_by(self, answer: Content) -> tuple[str, str] | None:
        """Keyed by kind as well, since a call can wait for both its approval and its result."""
        if answer.type == "function_result":
            occurrence_id = self._waiting_call(answer)
            return None if occurrence_id is None else ("result", occurrence_id)
        if answer.id in self._approvals:
            return ("approval", answer.id or "")
        return None

    def _waiting_call(self, result: Content) -> str | None:
        """A named occurrence answers only that call; a bare call id answers the earliest call waiting with it."""
        if result.id is not None:
            return result.id if result.id in self._results else None
        return next(
            (occurrence_id for occurrence_id, waiting in self._results.items() if waiting.call_id == result.call_id),
            None,
        )

    def route(self, answers: Sequence[Content]) -> None:
        """Every answer is checked before any is applied, so a bad one leaves the others unanswered.

        Raises:
            ValueError: If an answer matches no request the conversation is waiting on.
        """
        claimed: set[tuple[str, str]] = set()
        for answer in answers:
            request = self._request_answered_by(answer)
            if request is None or request in claimed:
                raise ValueError(
                    f"{answer.type} content with id {answer.id!r} answers nothing this conversation is waiting for."
                )
            claimed.add(request)
        for answer in answers:
            if answer.type == "function_result":
                # A copy, so changing a result after sending it changes nothing.
                self._results.pop(self._waiting_call(answer) or "").answer.set_result(copy.deepcopy(answer))
            else:
                self._approvals.pop(answer.id or "").set_result(answer.approved is True)
