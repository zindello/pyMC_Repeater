"""
Repeater-specific CompanionFrameServer with SQLite persistence.

Thin subclass of :class:`pymc_core.companion.frame_server.CompanionFrameServer`
that adds SQLite-backed message, contact, and channel persistence via a
``sqlite_handler`` dependency.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

from pymc_core.companion.frame_server import CompanionFrameServer as _BaseFrameServer
from pymc_core.companion.models import QueuedMessage

logger = logging.getLogger("CompanionFrameServer")


class CompanionFrameServer(_BaseFrameServer):
    """Adds SQLite persistence for messages, contacts, and channels.

    Constructor signature is intentionally kept compatible with the
    previous monolithic implementation so ``main.py`` call-sites need
    zero changes.
    """

    def __init__(
        self,
        bridge,
        companion_hash: str,
        port: int = 5000,
        bind_address: str = "0.0.0.0",
        sqlite_handler=None,
        local_hash: Optional[int] = None,
        stats_getter=None,
        control_handler=None,
    ):
        super().__init__(
            bridge=bridge,
            companion_hash=companion_hash,
            port=port,
            bind_address=bind_address,
            device_model="pyMC-Repeater-Companion",
            device_version="1.0.0",
            build_date="13 Feb 2026",
            local_hash=local_hash,
            stats_getter=stats_getter,
            control_handler=control_handler,
        )
        self.sqlite_handler = sqlite_handler

    # -----------------------------------------------------------------
    # Persistence hook overrides
    # -----------------------------------------------------------------

    async def _persist_companion_message(self, msg_dict: dict) -> None:
        """Persist message to SQLite and pop from bridge queue."""
        if not self.sqlite_handler:
            return
        await asyncio.to_thread(
            self.sqlite_handler.companion_push_message,
            self.companion_hash,
            msg_dict,
        )
        self.bridge.message_queue.pop_last()

    def _sync_next_from_persistence(self) -> Optional[QueuedMessage]:
        """Retrieve next message from SQLite when bridge queue is empty."""
        if not self.sqlite_handler:
            return None
        msg_dict = self.sqlite_handler.companion_pop_message(self.companion_hash)
        if not msg_dict:
            return None
        return QueuedMessage(
            sender_key=msg_dict.get("sender_key", b""),
            txt_type=msg_dict.get("txt_type", 0),
            timestamp=msg_dict.get("timestamp", 0),
            text=msg_dict.get("text", ""),
            is_channel=bool(msg_dict.get("is_channel", False)),
            channel_idx=msg_dict.get("channel_idx", 0),
            path_len=msg_dict.get("path_len", 0),
        )

    def _save_contacts(self) -> None:
        """Persist contacts to SQLite."""
        if not self.sqlite_handler:
            return
        contacts = self.bridge.get_contacts()
        dicts = []
        for c in contacts:
            pk = c.public_key if isinstance(c.public_key, bytes) else bytes.fromhex(c.public_key)
            dicts.append(
                {
                    "pubkey": pk,
                    "name": c.name,
                    "adv_type": c.adv_type,
                    "flags": c.flags,
                    "out_path_len": c.out_path_len,
                    "out_path": (
                        c.out_path
                        if isinstance(c.out_path, bytes)
                        else (bytes.fromhex(c.out_path) if c.out_path else b"")
                    ),
                    "last_advert_timestamp": c.last_advert_timestamp,
                    "lastmod": c.lastmod,
                    "gps_lat": c.gps_lat,
                    "gps_lon": c.gps_lon,
                    "sync_since": c.sync_since,
                }
            )
        self.sqlite_handler.companion_save_contacts(self.companion_hash, dicts)

    def _save_channels(self) -> None:
        """Persist channels to SQLite."""
        if not self.sqlite_handler:
            return
        channels = []
        max_ch = getattr(getattr(self.bridge, "channels", None), "max_channels", 40)
        for idx in range(max_ch):
            ch = self.bridge.get_channel(idx)
            if ch is not None:
                channels.append(
                    {
                        "channel_idx": idx,
                        "name": ch.name,
                        "secret": ch.secret,
                    }
                )
        self.sqlite_handler.companion_save_channels(self.companion_hash, channels)
