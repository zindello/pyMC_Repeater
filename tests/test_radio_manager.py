"""
Tests for RadioManager — asyncio radio lifecycle, retry, disconnect, and cleanup.

Run with:
    python -m pytest tests/test_radio_manager.py -v
"""

import asyncio
import sys
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from repeater.radio_manager import RadioManager


# ---------------------------------------------------------------------------
# Radio mock factories
# ---------------------------------------------------------------------------

class _SX1262Spec:
    """Minimal spec for SX1262-style radio: no is_connected, event-driven disconnect."""
    def cleanup(self): pass


class _KISSSpec:
    """Minimal spec for KISS-style radio: has is_connected bool, polled for disconnect."""
    is_connected: bool
    def cleanup(self): pass


def _sx1262_radio():
    """MagicMock radio without is_connected — triggers the asyncio-event disconnect path."""
    return MagicMock(spec=_SX1262Spec)


def _kiss_radio(connected=True):
    """MagicMock radio with is_connected — triggers the 1s-poll disconnect path."""
    r = MagicMock(spec=_KISSSpec)
    r.is_connected = connected
    return r


# ---------------------------------------------------------------------------
# Manager factory helper
# ---------------------------------------------------------------------------

def _make_manager(radio_type="sx1262", on_connected=None, on_disconnected=None):
    if on_connected is None:
        on_connected = AsyncMock()
    if on_disconnected is None:
        on_disconnected = AsyncMock()
    m = RadioManager(
        get_config=lambda: {"radio_type": radio_type},
        on_connected=on_connected,
        on_disconnected=on_disconnected,
    )
    return m, on_connected, on_disconnected


# ---------------------------------------------------------------------------
# 1. Happy path and status shape
# ---------------------------------------------------------------------------

class TestRadioManagerConnect(unittest.IsolatedAsyncioTestCase):

    async def test_connects_calls_on_connected_with_radio(self):
        mock_radio = _sx1262_radio()
        m, on_connected, _ = _make_manager()

        with patch("repeater.config.get_radio_for_board", return_value=mock_radio), \
             patch("repeater.radio_manager._RETRY_DELAYS", [0] * 4):
            m.start()
            await asyncio.sleep(0.05)

            on_connected.assert_called_once_with(mock_radio)
            await m.stop()

    async def test_status_is_connected_after_success(self):
        m, _, _ = _make_manager()

        with patch("repeater.config.get_radio_for_board", return_value=_sx1262_radio()), \
             patch("repeater.radio_manager._RETRY_DELAYS", [0] * 4):
            m.start()
            await asyncio.sleep(0.05)

            s = m.get_status()
            self.assertEqual(s["status"], "connected")
            self.assertEqual(s["type"], "sx1262")
            self.assertIsNone(s["error"])
            self.assertIsNotNone(s["connected_at"])
            self.assertEqual(s["retry_count"], 0)
            await m.stop()

    async def test_get_status_has_all_required_keys(self):
        m, _, _ = _make_manager()
        s = m.get_status()
        for key in ("status", "type", "error", "connected_at",
                    "last_error_at", "retry_count", "retry_delay_seconds"):
            self.assertIn(key, s)
        self.assertEqual(s["status"], "stopped")

    async def test_get_config_called_fresh_on_every_attempt(self):
        """get_config must be called before each connection attempt so UI config changes apply."""
        config_calls = 0

        def counting_config():
            nonlocal config_calls
            config_calls += 1
            return {"radio_type": "sx1262"}

        attempt = [0]

        def fail_twice_then_connect(config):
            attempt[0] += 1
            if attempt[0] < 3:
                raise RuntimeError("not ready")
            return _sx1262_radio()

        m = RadioManager(counting_config, AsyncMock(), AsyncMock())

        with patch("repeater.config.get_radio_for_board", side_effect=fail_twice_then_connect), \
             patch("repeater.radio_manager._RETRY_DELAYS", [0] * 4):
            m.start()
            await asyncio.sleep(0.1)
            self.assertGreaterEqual(config_calls, 3)
            await m.stop()


# ---------------------------------------------------------------------------
# 2. Retry and backoff
# ---------------------------------------------------------------------------

class TestRadioManagerRetry(unittest.IsolatedAsyncioTestCase):

    async def test_connect_failure_sets_error_status(self):
        m, on_connected, _ = _make_manager()

        with patch("repeater.config.get_radio_for_board", side_effect=RuntimeError("no hardware")), \
             patch("repeater.radio_manager._RETRY_DELAYS", [0] * 4):
            m.start()
            await asyncio.sleep(0.05)

            on_connected.assert_not_called()
            s = m.get_status()
            self.assertEqual(s["status"], "error")
            self.assertEqual(s["error"], "no hardware")
            self.assertIsNotNone(s["last_error_at"])
            await m.stop()

    async def test_retry_count_increments_on_repeated_failure(self):
        m, _, _ = _make_manager()

        with patch("repeater.config.get_radio_for_board", side_effect=RuntimeError("fail")), \
             patch("repeater.radio_manager._RETRY_DELAYS", [0] * 4):
            m.start()
            await asyncio.sleep(0.1)
            self.assertGreater(m.get_status()["retry_count"], 1)
            await m.stop()

    async def test_backoff_delay_sequence_follows_retry_delays(self):
        """Delays passed to _interruptible_wait must match _RETRY_DELAYS in order."""
        delays_used = []

        async def capture_wait(self_inner, delay):
            delays_used.append(delay)

        attempt = [0]

        def fail_three_times(config):
            attempt[0] += 1
            if attempt[0] <= 3:
                raise RuntimeError("fail")
            return _sx1262_radio()

        m, _, _ = _make_manager()

        with patch("repeater.config.get_radio_for_board", side_effect=fail_three_times), \
             patch.object(RadioManager, "_interruptible_wait", capture_wait), \
             patch("repeater.radio_manager._RETRY_DELAYS", [5, 10, 30, 60]):
            m.start()
            await asyncio.sleep(0.1)
            self.assertEqual(delays_used[:3], [5, 10, 30])
            await m.stop()

    async def test_retry_count_and_delay_reset_on_successful_connect(self):
        """retry_count and retry_delay_seconds return to 0 once connected after failures."""
        attempt = [0]

        def fail_twice_then_connect(_):
            attempt[0] += 1
            if attempt[0] <= 2:
                raise RuntimeError("fail")
            return _sx1262_radio()

        m, _, _ = _make_manager()

        with patch("repeater.config.get_radio_for_board", side_effect=fail_twice_then_connect), \
             patch("repeater.radio_manager._RETRY_DELAYS", [0] * 4):
            m.start()
            await asyncio.sleep(0.1)

            s = m.get_status()
            self.assertEqual(s["status"], "connected")
            self.assertEqual(s["retry_count"], 0)
            self.assertEqual(s["retry_delay_seconds"], 0)
            await m.stop()

    async def test_notify_config_changed_unblocks_backoff_immediately(self):
        """notify_config_changed() during a 1-second backoff causes immediate retry."""
        attempt = [0]

        def fail_once_then_connect(config):
            attempt[0] += 1
            if attempt[0] == 1:
                raise RuntimeError("fail")
            return _sx1262_radio()

        m, _, _ = _make_manager()

        with patch("repeater.config.get_radio_for_board", side_effect=fail_once_then_connect), \
             patch("repeater.radio_manager._RETRY_DELAYS", [1, 1, 1, 1]):
            m.start()
            await asyncio.sleep(0.05)       # First attempt failed, now in 1s backoff
            self.assertEqual(m.get_status()["status"], "error")

            m.notify_config_changed()
            await asyncio.sleep(0.1)        # Retry should complete well within 1s

            self.assertEqual(m.get_status()["status"], "connected")
            self.assertEqual(m.get_status()["retry_count"], 0)
            self.assertEqual(m.get_status()["retry_delay_seconds"], 0)
            await m.stop()


# ---------------------------------------------------------------------------
# 3. Mid-run disconnect
# ---------------------------------------------------------------------------

class TestRadioManagerDisconnect(unittest.IsolatedAsyncioTestCase):

    async def test_sx1262_disconnect_via_signal_calls_on_disconnected(self):
        """signal_disconnected() mid-run triggers on_disconnected then reconnect."""
        m, on_connected, on_disconnected = _make_manager()

        with patch("repeater.config.get_radio_for_board", return_value=_sx1262_radio()), \
             patch("repeater.radio_manager._RETRY_DELAYS", [0] * 4):
            m.start()
            await asyncio.sleep(0.05)
            self.assertEqual(m.get_status()["status"], "connected")

            m.signal_disconnected()
            await asyncio.sleep(0.05)

            on_disconnected.assert_called_once()
            await m.stop()

    async def test_kiss_disconnect_via_is_connected_flag(self):
        """is_connected → False on KISS radio triggers on_disconnected and reconnect.

        is_connected is set False inside on_connected (before _wait_for_disconnect
        checks it), so the poll detects it on the first check without a 1s delay.
        """
        mock_radio = _kiss_radio(connected=True)
        on_disconnected = AsyncMock()
        connect_count = [0]
        reconnected = asyncio.Event()

        async def on_connected_fn(radio):
            radio.is_connected = True   # Reset so second connect stays alive
            connect_count[0] += 1
            if connect_count[0] == 1:
                # Set False before _wait_for_disconnect polls — no 1s wait needed
                radio.is_connected = False
            else:
                reconnected.set()

        m = RadioManager(
            get_config=lambda: {"radio_type": "kiss"},
            on_connected=on_connected_fn,
            on_disconnected=on_disconnected,
        )

        with patch("repeater.config.get_radio_for_board", return_value=mock_radio), \
             patch("repeater.radio_manager._RETRY_DELAYS", [0] * 4):
            m.start()
            await asyncio.wait_for(reconnected.wait(), timeout=2.0)

            on_disconnected.assert_called_once()
            self.assertGreaterEqual(connect_count[0], 2)
            await m.stop()

    async def test_on_disconnected_raises_is_swallowed(self):
        """Exception from on_disconnected must not abort the reconnect cycle."""
        connect_count = [0]
        reconnected = asyncio.Event()

        async def on_connected_fn(radio):
            connect_count[0] += 1
            if connect_count[0] > 1:
                reconnected.set()

        m = RadioManager(
            get_config=lambda: {"radio_type": "sx1262"},
            on_connected=on_connected_fn,
            on_disconnected=AsyncMock(side_effect=RuntimeError("teardown failed")),
        )

        with patch("repeater.config.get_radio_for_board", return_value=_sx1262_radio()), \
             patch("repeater.radio_manager._RETRY_DELAYS", [0] * 4):
            m.start()
            await asyncio.sleep(0.05)
            self.assertEqual(connect_count[0], 1)

            m.signal_disconnected()
            await asyncio.wait_for(reconnected.wait(), timeout=1.0)

            self.assertGreaterEqual(connect_count[0], 2)
            await m.stop()

    async def test_notify_config_changed_while_connected_forces_reconnect(self):
        """notify_config_changed() while connected triggers disconnect + reconnect cycle."""
        connect_count = [0]
        reconnected = asyncio.Event()

        async def on_connected_fn(radio):
            connect_count[0] += 1
            if connect_count[0] > 1:
                reconnected.set()

        m = RadioManager(
            get_config=lambda: {"radio_type": "sx1262"},
            on_connected=on_connected_fn,
            on_disconnected=AsyncMock(),
        )

        with patch("repeater.config.get_radio_for_board", return_value=_sx1262_radio()), \
             patch("repeater.radio_manager._RETRY_DELAYS", [0] * 4):
            m.start()
            await asyncio.sleep(0.05)
            self.assertEqual(connect_count[0], 1)

            m.notify_config_changed()
            await asyncio.wait_for(reconnected.wait(), timeout=1.0)

            self.assertGreaterEqual(connect_count[0], 2)
            await m.stop()


# ---------------------------------------------------------------------------
# 4. Shutdown
# ---------------------------------------------------------------------------

class TestRadioManagerShutdown(unittest.IsolatedAsyncioTestCase):

    async def test_stop_during_backoff_exits_cleanly(self):
        m, _, _ = _make_manager()

        with patch("repeater.config.get_radio_for_board", side_effect=RuntimeError("fail")), \
             patch("repeater.radio_manager._RETRY_DELAYS", [60, 60, 60, 60]):
            m.start()
            await asyncio.sleep(0.05)       # Now in 60s backoff
            await m.stop()
            self.assertEqual(m.get_status()["status"], "stopped")

    async def test_stop_while_connected_calls_radio_cleanup(self):
        mock_radio = _sx1262_radio()
        m, _, _ = _make_manager()

        with patch("repeater.config.get_radio_for_board", return_value=mock_radio), \
             patch("repeater.radio_manager._RETRY_DELAYS", [0] * 4):
            m.start()
            await asyncio.sleep(0.05)
            self.assertEqual(m.get_status()["status"], "connected")

            await m.stop()

            mock_radio.cleanup.assert_called_once()
            self.assertEqual(m.get_status()["status"], "stopped")

    async def test_stop_before_start_is_safe(self):
        m, _, _ = _make_manager()
        await m.stop()                      # Must not raise
        self.assertEqual(m.get_status()["status"], "stopped")


# ---------------------------------------------------------------------------
# 5. Error handling
# ---------------------------------------------------------------------------

class TestRadioManagerErrorHandling(unittest.IsolatedAsyncioTestCase):

    async def test_on_connected_raises_triggers_retry(self):
        """on_connected raising is treated as a connection error — manager retries."""
        connect_count = [0]

        async def failing_then_ok(radio):
            connect_count[0] += 1
            if connect_count[0] == 1:
                raise RuntimeError("daemon setup failed")

        m = RadioManager(
            get_config=lambda: {"radio_type": "sx1262"},
            on_connected=failing_then_ok,
            on_disconnected=AsyncMock(),
        )

        with patch("repeater.config.get_radio_for_board", return_value=_sx1262_radio()), \
             patch("repeater.radio_manager._RETRY_DELAYS", [0] * 4):
            m.start()
            await asyncio.sleep(0.1)
            self.assertGreaterEqual(connect_count[0], 2)
            self.assertEqual(m.get_status()["status"], "connected")
            await m.stop()

    async def test_on_connected_raises_calls_cleanup_on_radio(self):
        """Radio must be cleaned up when on_connected raises — no hardware leak."""
        mock_radio = _sx1262_radio()

        m = RadioManager(
            get_config=lambda: {"radio_type": "sx1262"},
            on_connected=AsyncMock(side_effect=RuntimeError("fail")),
            on_disconnected=AsyncMock(),
        )

        with patch("repeater.config.get_radio_for_board", return_value=mock_radio), \
             patch("repeater.radio_manager._RETRY_DELAYS", [0] * 4):
            m.start()
            await asyncio.sleep(0.05)
            mock_radio.cleanup.assert_called()
            await m.stop()

    async def test_radio_cleanup_exception_is_swallowed(self):
        """Exception from radio.cleanup() must not crash the manager or stop()."""
        mock_radio = _sx1262_radio()
        mock_radio.cleanup.side_effect = RuntimeError("cleanup exploded")
        m, _, _ = _make_manager()

        with patch("repeater.config.get_radio_for_board", return_value=mock_radio), \
             patch("repeater.radio_manager._RETRY_DELAYS", [0] * 4):
            m.start()
            await asyncio.sleep(0.05)
            await m.stop()                  # Must not raise
            self.assertEqual(m.get_status()["status"], "stopped")

    async def test_ch341_reset_called_for_ch341_radio_type(self):
        """CH341Async.reset_instance() is called when radio_type is sx1262_ch341."""
        mock_radio = _sx1262_radio()
        mock_ch341_class = MagicMock()
        mock_ch341_module = MagicMock()
        mock_ch341_module.CH341Async = mock_ch341_class

        m = RadioManager(
            get_config=lambda: {"radio_type": "sx1262_ch341"},
            on_connected=AsyncMock(),
            on_disconnected=AsyncMock(),
        )

        with patch("repeater.config.get_radio_for_board", return_value=mock_radio), \
             patch("repeater.radio_manager._RETRY_DELAYS", [0] * 4), \
             patch.dict(sys.modules, {"pymc_core.hardware.ch341.ch341_async": mock_ch341_module}):
            m.start()
            await asyncio.sleep(0.05)
            await m.stop()
            mock_ch341_class.reset_instance.assert_called()


if __name__ == "__main__":
    unittest.main()
