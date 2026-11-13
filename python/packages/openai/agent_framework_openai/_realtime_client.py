# Copyright (c) Microsoft. All rights reserved.

"""OpenAI Realtime and Azure OpenAI Realtime as a realtime provider, on the shared OpenAI Realtime session."""

from __future__ import annotations

import asyncio
import importlib.util
import inspect
import json
from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Generic, cast

from agent_framework._feature_stage import ExperimentalFeature, experimental
from agent_framework._settings import SecretString
from agent_framework._telemetry import prepend_agent_framework_to_user_agent
from agent_framework.exceptions import RealtimeConnectionException, SettingNotFoundError
from openai import AsyncAzureOpenAI, AsyncOpenAI

from ._realtime_options import OpenAIRealtimeOptionsT
from ._realtime_session import OpenAIRealtimeSession, _audio_format  # pyright: ignore[reportPrivateUsage]
from ._shared import (
    AzureTokenProvider,
    _resolve_azure_credential_to_token_provider,  # pyright: ignore[reportPrivateUsage]
    load_openai_service_settings,
)

if TYPE_CHECKING:
    from agent_framework import RealtimeClientSessionProtocol, ToolTypes
    from azure.core.credentials import TokenCredential
    from azure.core.credentials_async import AsyncTokenCredential

    AzureCredentialTypes = TokenCredential | AsyncTokenCredential


@experimental(feature_id=ExperimentalFeature.REALTIME)
class OpenAIRealtimeClient(Generic[OpenAIRealtimeOptionsT]):
    """A realtime client for OpenAI Realtime, and for Azure OpenAI Realtime through Azure routing.

    It needs the realtime extra: ``pip install 'agent-framework-openai[realtime]'``. Routing follows the OpenAI chat
    clients: a ``credential``, an ``azure_endpoint`` or the ``AZURE_OPENAI_*`` variables route to Azure OpenAI, where
    the model is the deployment's name, and ``api_key`` or ``OPENAI_API_KEY`` routes to OpenAI. The model is read from
    ``OPENAI_REALTIME_MODEL`` or ``AZURE_OPENAI_REALTIME_MODEL``, then ``OPENAI_MODEL`` or ``AZURE_OPENAI_MODEL``, when
    not passed. Azure OpenAI is reached on the resource's ``/openai/v1`` route, which takes no API version.

    Examples:
        .. code-block:: python

            from agent_framework import RealtimeAgent
            from agent_framework.openai import OpenAIRealtimeClient

            agent = RealtimeAgent(OpenAIRealtimeClient(model="gpt-realtime"), instructions="Be brief.")
            async with agent.start_conversation() as conversation:
                await conversation.send("Hello!")
                async for update in conversation:
                    print(update.text, end="")

    Attributes:
        model: The model, or on Azure OpenAI the deployment's name.
    """

    OTEL_PROVIDER_NAME: ClassVar[str] = "openai"

    def __init__(
        self,
        model: str | None = None,
        *,
        api_key: str | SecretString | Callable[[], str | Awaitable[str]] | None = None,
        credential: AzureCredentialTypes | AzureTokenProvider | None = None,
        azure_endpoint: str | None = None,
        base_url: str | None = None,
        async_client: AsyncOpenAI | None = None,
        env_file_path: str | None = None,
        env_file_encoding: str | None = None,
    ) -> None:
        """Create a realtime client. Nothing connects until ``start_session(...)``.

        Args:
            model: The model, such as ``"gpt-realtime"``, or on Azure OpenAI the deployment's name.

        Keyword Args:
            api_key: An OpenAI API key, or on Azure OpenAI a resource key or a callable that returns a token. Read from
                ``OPENAI_API_KEY`` or ``AZURE_OPENAI_API_KEY`` when not passed.
            credential: An Azure credential or token provider, for the ``cognitiveservices`` scope. Passing it routes to
                Azure OpenAI. The client never closes it.
            azure_endpoint: The Azure OpenAI resource endpoint, such as ``https://<resource>.openai.azure.com``. Read
                from ``AZURE_OPENAI_ENDPOINT`` when not passed.
            base_url: A base URL: on OpenAI, in place of the default; on Azure OpenAI, the full ``.../openai/v1`` URL in
                place of ``azure_endpoint``.
            async_client: An ``AsyncOpenAI`` client to connect with, used as it is. On Azure OpenAI its base URL must be
                the resource's ``/openai/v1`` URL.
            env_file_path: A ``.env`` file to read settings from.
            env_file_encoding: The ``.env`` file's encoding.

        Raises:
            SettingNotFoundError: If the model, the endpoint or authentication is missing.
            ValueError: If ``async_client`` is an ``AsyncAzureOpenAI``, whose realtime route takes an API version.
            ModuleNotFoundError: If the realtime extra isn't installed.
        """
        _require_websockets()
        _reject_azure_sdk_client(async_client)
        if callable(api_key):
            api_key = _off_the_event_loop(api_key)
        if credential is not None:
            credential = _off_the_event_loop(_resolve_azure_credential_to_token_provider(credential))
        settings, self._client, use_azure = load_openai_service_settings(
            model=model,
            api_key=api_key,
            credential=credential,
            org_id=None,
            base_url=base_url,
            endpoint=azure_endpoint,
            api_version=None,
            default_azure_api_version="",
            client=async_client,
            env_file_path=env_file_path,
            env_file_encoding=env_file_encoding,
            openai_model_fields=("realtime_model", "model"),
            azure_model_fields=("realtime_model", "model"),
            azure_v1_route=True,
        )
        if not settings.get("model"):
            # An injected OpenAI client skips the resolver's model check, but connecting needs one.
            raise SettingNotFoundError(
                "OpenAIRealtimeClient needs a model: pass 'model', or set 'OPENAI_REALTIME_MODEL' or 'OPENAI_MODEL'."
            )
        self.model: str = settings["model"]
        if use_azure:
            self.OTEL_PROVIDER_NAME = "azure.ai.openai"  # type: ignore[misc]

    async def start_session(
        self, *, options: OpenAIRealtimeOptionsT | None = None, tools: Sequence[ToolTypes] | None = None
    ) -> RealtimeClientSessionProtocol[OpenAIRealtimeOptionsT]:
        """Connect, apply the initial settings, and return the session once the service acknowledges them.

        The caller owns the session and closes it. If opening fails or is cancelled, the socket is closed first.

        Keyword Args:
            options: The session's initial settings.
            tools: The tools to declare.

        Raises:
            RealtimeConnectionException: If connecting fails, including a rejected key or token.
            RealtimeControlException: If the service rejects the initial settings.
            ValueError: If an audio format is one the service can't take. Nothing connects.
        """
        from websockets.exceptions import WebSocketException

        requested = cast("Mapping[str, Any]", options or {})
        for key in ("input_audio_format", "output_audio_format"):
            if key in requested:
                # A format the session would only reject after connecting would leave a socket to clean up.
                _audio_format(requested[key])
        try:
            sdk_connection = await self._client.realtime.connect(
                model=self.model,
                # Before 3.36, the SDK sends none of the client's default headers on the handshake.
                extra_headers=prepend_agent_framework_to_user_agent(),
                # openai 2.x negotiates permessage-deflate by default, which only costs CPU on compressed audio.
                websocket_connection_options={"compression": None},
            ).enter()
        except (WebSocketException, OSError) as error:
            raise RealtimeConnectionException(f"Couldn't connect to the realtime service: {error}") from error
        session: OpenAIRealtimeSession[OpenAIRealtimeOptionsT] = OpenAIRealtimeSession(
            _OpenAIRealtimeConnection(sdk_connection)
        )
        try:
            await session.start(options=options, tools=tools)
        except BaseException:
            await session.close()
            raise
        return session


class _OpenAIRealtimeConnection:
    """Raw JSON keeps the SDK's event models out of the shared protocol mapping."""

    def __init__(self, connection: Any) -> None:
        self._connection = connection

    async def send(self, event: Mapping[str, Any]) -> None:
        from websockets.exceptions import ConnectionClosed

        try:
            await self._connection.send(event)
        except ConnectionClosed as closed:
            code = _close_code(closed)
            raise RealtimeConnectionException(
                f"Couldn't send to the realtime service: the connection closed with code {code}.", close_code=code
            ) from closed

    async def receive(self) -> Mapping[str, Any] | None:
        from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK

        try:
            frame = await self._connection.recv_bytes()
        except ConnectionClosedOK:
            return None
        except ConnectionClosedError as closed:
            code = _close_code(closed)
            raise RealtimeConnectionException(
                f"The realtime service closed the connection with code {code}.", close_code=code
            ) from closed
        return json.loads(frame)

    async def close(self) -> None:
        await self._connection.close()


def _off_the_event_loop(provider: AzureTokenProvider) -> Callable[[], Awaitable[str]]:
    """A synchronous token provider, such as azure.identity's, can block on I/O such as the Azure CLI.

    It runs in a worker thread, so a slow fetch can't stall the other conversations on the event loop. The SDK awaits
    the result either way.
    """
    if inspect.iscoroutinefunction(provider) or inspect.iscoroutinefunction(type(provider).__call__):
        return cast("Callable[[], Awaitable[str]]", provider)

    async def fetch_token() -> str:
        token = await asyncio.to_thread(provider)
        if inspect.isawaitable(token):
            token = await token
        return token

    return fetch_token


def _close_code(closed: Any) -> int | None:
    """The code the service closed with; a socket that dropped without a close frame has none."""
    return closed.rcvd.code if closed.rcvd is not None else None


def _require_websockets() -> None:
    """The SDK only fails at connect without websockets, so the install hint comes when the client is made."""
    if importlib.util.find_spec("websockets") is None:
        raise ModuleNotFoundError(
            "OpenAIRealtimeClient needs websockets, which the realtime extra installs: "
            "pip install 'agent-framework-openai[realtime]'"
        )


def _reject_azure_sdk_client(async_client: AsyncOpenAI | None) -> None:
    """Azure realtime runs here only on the ``/openai/v1`` route; ``AsyncAzureOpenAI`` connects to the preview one."""
    if isinstance(async_client, AsyncAzureOpenAI):
        raise ValueError(
            "OpenAIRealtimeClient can't use an AsyncAzureOpenAI client, which connects to the preview realtime "
            "route with an api_version. Pass 'azure_endpoint' instead, or an AsyncOpenAI client whose base_url "
            "is the resource's '/openai/v1' URL."
        )
