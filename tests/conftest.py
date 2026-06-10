"""Fake WebSocket used by all tests."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field


@dataclass
class FakeWebSocket:
    sent_text: list[str] = field(default_factory=list)
    sent_bytes: list[bytes] = field(default_factory=list)
    closed_code: int = 0
    fail_on_send: bool = False
    send_delay: float = 0.0
    headers: dict[str, str] = field(default_factory=dict)
    incoming: list[str] = field(default_factory=list)

    async def receive_text(self) -> str:
        return self.incoming.pop(0)

    async def send_text(self, data: str) -> None:
        if self.send_delay:
            await asyncio.sleep(self.send_delay)
        if self.fail_on_send:
            raise ConnectionError("simulated send failure")
        self.sent_text.append(data)

    async def send_bytes(self, data: bytes) -> None:
        if self.send_delay:
            await asyncio.sleep(self.send_delay)
        if self.fail_on_send:
            raise ConnectionError("simulated send failure")
        self.sent_bytes.append(data)

    async def close(self, code: int = 1000) -> None:
        self.closed_code = code
