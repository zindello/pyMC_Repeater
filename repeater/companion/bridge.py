"""
Repeater CompanionBridge with SQLite-backed preference persistence.

Persists full NodePrefs as a JSON blob so companion settings (including
auto-add config) survive repeater restarts. Merge-on-load supports
schema evolution when NodePrefs gains or loses fields.
"""

from __future__ import annotations

import dataclasses
import logging
from typing import Any, Callable, Optional

from pymc_core.companion import CompanionBridge

logger = logging.getLogger("RepeaterCompanionBridge")


class RepeaterCompanionBridge(CompanionBridge):
    """CompanionBridge that persists and loads prefs (full NodePrefs) via SQLite JSON blob."""

    def __init__(
        self,
        identity,
        packet_injector: Callable[..., Any],
        node_name: str = "pyMC",
        adv_type: int = 1,
        max_contacts: int = 1000,
        max_channels: int = 40,
        offline_queue_size: int = 512,
        radio_config: Optional[dict] = None,
        authenticate_callback: Optional[Callable[..., tuple[bool, int]]] = None,
        initial_contacts: Optional[Any] = None,
        *,
        sqlite_handler=None,
        companion_hash: str = "",
    ) -> None:
        self._sqlite_handler = sqlite_handler
        self._companion_hash = companion_hash
        super().__init__(
            identity=identity,
            packet_injector=packet_injector,
            node_name=node_name,
            adv_type=adv_type,
            max_contacts=max_contacts,
            max_channels=max_channels,
            offline_queue_size=offline_queue_size,
            radio_config=radio_config,
            authenticate_callback=authenticate_callback,
            initial_contacts=initial_contacts,
        )

    def _save_prefs(self) -> None:
        """Persist full NodePrefs as JSON to SQLite."""
        if not self._sqlite_handler or not self._companion_hash:
            return
        try:
            prefs_dict = dataclasses.asdict(self.prefs)
            self._sqlite_handler.companion_save_prefs(self._companion_hash, prefs_dict)
        except Exception as e:
            logger.warning("Failed to persist companion prefs: %s", e)

    def _load_prefs(self) -> None:
        """Load prefs from SQLite JSON and merge into self.prefs (only known keys)."""
        if not self._sqlite_handler or not self._companion_hash:
            return
        try:
            stored = self._sqlite_handler.companion_load_prefs(self._companion_hash)
            if not stored or not isinstance(stored, dict):
                return
            for key, value in stored.items():
                if not hasattr(self.prefs, key):
                    continue
                current = getattr(self.prefs, key)
                try:
                    if value is None:
                        continue
                    if isinstance(current, bool):
                        setattr(self.prefs, key, bool(value))
                    elif isinstance(current, int):
                        setattr(self.prefs, key, int(value))
                    elif isinstance(current, float):
                        setattr(self.prefs, key, float(value))
                    elif isinstance(current, str):
                        setattr(self.prefs, key, str(value))
                    else:
                        setattr(self.prefs, key, value)
                except (TypeError, ValueError) as e:
                    logger.debug("Skip prefs key %r: %s", key, e)
        except Exception as e:
            logger.warning("Failed to load companion prefs: %s", e)
