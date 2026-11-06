# PROTOTYPE, never merged (adamdougal/agent-framework#10).
"""The prototype realtime API. See README.md."""

from ._core import apply_core_patches

apply_core_patches()

from ._agent import FoundryVoiceAgent, ParticipantSettings, RealtimeAgent  # noqa: E402
from ._azure import FoundryVoiceAgentClient, VoiceLiveClient  # noqa: E402
from ._conversation import RealtimeConversation  # noqa: E402
from ._core import (  # noqa: E402
    CORE_CHANGES,
    ConversationContext,
    ConversationMiddleware,
    RealtimeActiveResponseError,
    RealtimeControlError,
    RealtimeControlTimeoutError,
    RealtimeError,
    RealtimeEvent,
    RealtimeOptions,
    RealtimeUnsupportedError,
    kind_of,
)
from ._ga_session import OpenAIRealtimeClient, OpenAIRealtimeSession  # noqa: E402
from ._handoff import HandoffRealtimeAgent, RealtimeHandoffBuilder  # noqa: E402
