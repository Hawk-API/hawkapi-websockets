"""Fake WebSocket used by all tests."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class FakeWebSocket:
    sent_text: list[str] = field(default_factory=list)
    sent_bytes: list[bytes] = field(default_factory=list)
    closed_code: int = 0
    fail_on_send: bool = False

    async def send_text(self, data: str) -> None:
        if self.fail_on_send:
            raise ConnectionError("simulated send failure")
        self.sent_text.append(data)

    async def send_bytes(self, data: bytes) -> None:
        if self.fail_on_send:
            raise ConnectionError("simulated send failure")
        self.sent_bytes.append(data)

    async def close(self, code: int = 1000) -> None:
        self.closed_code = code
