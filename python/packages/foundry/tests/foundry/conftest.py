# Copyright (c) Microsoft. All rights reserved.
import sys
from pathlib import Path
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from pytest import MonkeyPatch, fixture


def pytest_configure() -> None:
    """Put the helpers directory on sys.path so the tests can import their shared helpers by name.

    Under importlib import mode, which the workspace test run uses, pytest doesn't add a test's directory itself. Only
    the helpers directory goes on the path, so this conftest can't shadow another package's conftest imported by name.
    """
    helpers_dir = str(Path(__file__).resolve().parent / "helpers")
    if helpers_dir not in sys.path:
        sys.path.insert(0, helpers_dir)


@fixture
def exclude_list(request: Any) -> list[str]:
    """Fixture that returns a list of environment variables to exclude."""
    return request.param if hasattr(request, "param") else []


@fixture
def override_env_param_dict(request: Any) -> dict[str, str]:
    """Fixture that returns a dict of environment variables to override."""
    return request.param if hasattr(request, "param") else {}


@fixture()
def foundry_unit_test_env(monkeypatch, exclude_list, override_env_param_dict):  # type: ignore
    """Fixture to set environment variables for Foundry settings."""

    if exclude_list is None:
        exclude_list = []

    if override_env_param_dict is None:
        override_env_param_dict = {}

    env_vars = {
        "FOUNDRY_PROJECT_ENDPOINT": "https://test-project.services.ai.azure.com/",
        "FOUNDRY_MODEL": "test-gpt-4o",
    }

    env_vars.update(override_env_param_dict)  # type: ignore

    for key, value in env_vars.items():
        if key in exclude_list:
            monkeypatch.delenv(key, raising=False)  # type: ignore
            continue
        monkeypatch.setenv(key, value)  # type: ignore

    return env_vars


@fixture
def mock_agents_client() -> MagicMock:
    """Fixture that provides a mock AgentsClient."""
    mock_client = MagicMock()

    # Mock agents property
    mock_client.create_agent = AsyncMock()
    mock_client.delete_agent = AsyncMock()

    # Mock agent creation response
    mock_agent = MagicMock()
    mock_agent.id = "test-agent-id"
    mock_client.create_agent.return_value = mock_agent

    # Mock threads property
    mock_client.threads = MagicMock()
    mock_client.threads.create = AsyncMock()
    mock_client.messages.create = AsyncMock()

    # Mock runs property
    mock_client.runs = MagicMock()
    mock_client.runs.list = AsyncMock()
    mock_client.runs.cancel = AsyncMock()
    mock_client.runs.stream = AsyncMock()
    mock_client.runs.submit_tool_outputs_stream = AsyncMock()

    return mock_client


@fixture
def mock_azure_credential() -> MagicMock:
    """Fixture that provides a mock AsyncTokenCredential."""
    return MagicMock()


@fixture
def span_exporter(monkeypatch: MonkeyPatch) -> InMemorySpanExporter:
    """Instrumentation on, with finished spans collected in memory."""
    from agent_framework import observability

    monkeypatch.setattr(observability.OBSERVABILITY_SETTINGS, "enable_instrumentation", True)
    trace.set_tracer_provider(TracerProvider())
    exporter = InMemorySpanExporter()
    cast(TracerProvider, trace.get_tracer_provider()).add_span_processor(SimpleSpanProcessor(exporter))
    return exporter
