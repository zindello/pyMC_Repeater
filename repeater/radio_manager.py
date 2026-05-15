import asyncio
import logging
import time
from typing import Any, Callable, Dict, Optional

logger = logging.getLogger("RadioManager")

# Retry delays in seconds: 5, 10, 30, 60, 60, 60, ...
_RETRY_DELAYS = [5, 10, 30, 60]


class RadioManager:
    """
    Manages radio hardware lifecycle: connect, retry on failure, report status.

    Runs as a single asyncio task. The synchronous get_radio_for_board() call is
    dispatched via run_in_executor so it never blocks the event loop — consistent
    with engine._record_noise_floor_async().

    on_connected(radio) is called (awaited) when hardware initialises successfully.
    on_disconnected() is called (awaited) when the radio is lost mid-run so the
    daemon can tear down the dispatcher and helpers before RadioManager retries.

    get_config is called on every attempt so that UI-driven config changes are
    picked up without a service restart.
    """

    def __init__(
        self,
        get_config: Callable[[], Dict[str, Any]],
        on_connected: Callable,
        on_disconnected: Callable,
    ) -> None:
        self._get_config = get_config
        self._on_connected = on_connected
        self._on_disconnected = on_disconnected

        self._task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()
        self._retry_now_event = asyncio.Event()
        self._disconnected_event = asyncio.Event()

        self._status: str = "stopped"
        self._radio_type: Optional[str] = None
        self._error: Optional[str] = None
        self._connected_at: Optional[float] = None
        self._last_error_at: Optional[float] = None
        self._retry_count: int = 0
        self._retry_delay: int = 0
        self._current_radio: Any = None

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Spawn the connection loop as an asyncio task."""
        self._stop_event.clear()
        self._task = asyncio.create_task(self._connect_loop(), name="radio-manager")

    async def stop(self) -> None:
        """Signal the loop to stop, await task completion, and clean up hardware."""
        self._stop_event.set()
        self._retry_now_event.set()   # unblock any backoff wait
        self._disconnected_event.set()  # unblock any run_forever wait
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
        self._cleanup_radio()
        self._status = "stopped"

    def notify_config_changed(self) -> None:
        """Reset backoff and retry immediately — call after a config save."""
        self._retry_count = 0
        self._retry_now_event.set()
        if self._status == "connected":
            self._disconnected_event.set()

    def signal_disconnected(self) -> None:
        """
        Called by the daemon when dispatcher.run_forever() exits unexpectedly,
        telling RadioManager the radio is gone and it should re-enter the retry loop.
        """
        self._disconnected_event.set()

    def get_status(self) -> Dict[str, Any]:
        return {
            "status": self._status,
            "type": self._radio_type,
            "error": self._error,
            "connected_at": self._connected_at,
            "last_error_at": self._last_error_at,
            "retry_count": self._retry_count,
            "retry_delay_seconds": self._retry_delay,
        }

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _cleanup_radio(self) -> None:
        if self._current_radio and hasattr(self._current_radio, "cleanup"):
            try:
                self._current_radio.cleanup()
            except Exception as e:
                logger.debug("Radio cleanup error: %s", e)
        # Force re-init on next connect: SX1262Radio is a singleton; cleanup() closes
        # the SPI bus but may not reset _initialized, which would cause begin() to be
        # skipped and the radio to operate with a closed SPI connection.
        if self._current_radio and hasattr(self._current_radio, "_initialized"):
            self._current_radio._initialized = False
        if self._radio_type == "sx1262_ch341":
            try:
                from pymc_core.hardware.ch341.ch341_async import CH341Async
                CH341Async.reset_instance()
            except Exception as e:
                logger.debug("CH341 reset skipped/failed: %s", e)
        self._current_radio = None

    async def _connect_loop(self) -> None:
        loop = asyncio.get_running_loop()

        while not self._stop_event.is_set():
            config = self._get_config()
            self._radio_type = config.get("radio_type", "sx1262")
            self._status = "connecting"
            self._retry_now_event.clear()

            logger.info(
                "Attempting radio connection (type=%s, attempt=%d)",
                self._radio_type,
                self._retry_count + 1,
            )

            try:
                from repeater.config import get_radio_for_board

                radio = await loop.run_in_executor(None, get_radio_for_board, config)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._error = str(e)
                self._last_error_at = time.time()
                self._status = "error"
                delay = _RETRY_DELAYS[min(self._retry_count, len(_RETRY_DELAYS) - 1)]
                self._retry_delay = delay
                self._retry_count += 1
                logger.error(
                    "Radio connection failed: %s. Retrying in %ds (attempt %d)",
                    e,
                    delay,
                    self._retry_count,
                )
                await self._interruptible_wait(delay)
                continue

            # Apply post-init radio configuration
            try:
                if hasattr(radio, "set_event_loop"):
                    radio.set_event_loop(loop)

                if hasattr(radio, "set_custom_cad_thresholds"):
                    cad_config = config.get("radio", {}).get("cad", {})
                    radio.set_custom_cad_thresholds(
                        peak=cad_config.get("peak_threshold", 23),
                        min_val=cad_config.get("min_threshold", 11),
                    )

                if hasattr(radio, "get_frequency"):
                    logger.info("Radio config - Freq: %.1fMHz", radio.get_frequency())
                if hasattr(radio, "get_spreading_factor"):
                    logger.info("Radio config - SF: %s", radio.get_spreading_factor())
                if hasattr(radio, "get_bandwidth"):
                    logger.info("Radio config - BW: %skHz", radio.get_bandwidth())
                if hasattr(radio, "get_coding_rate"):
                    logger.info("Radio config - CR: %s", radio.get_coding_rate())
                if hasattr(radio, "get_tx_power"):
                    logger.info("Radio config - TX Power: %sdBm", radio.get_tx_power())
            except asyncio.CancelledError:
                self._cleanup_radio()
                raise
            except Exception as e:
                logger.warning("Radio post-init configuration failed: %s", e)
                # Non-fatal — continue with whatever the radio supports

            self._current_radio = radio
            self._status = "connected"
            self._connected_at = time.time()
            self._error = None
            self._retry_count = 0
            self._retry_delay = 0
            logger.info("Radio connected (type=%s)", self._radio_type)

            # Notify daemon — this sets up Dispatcher and all helpers
            try:
                await self._on_connected(radio)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("Daemon failed to initialise after radio connection: %s", e)
                self._error = str(e)
                self._last_error_at = time.time()
                self._status = "error"
                self._cleanup_radio()
                delay = _RETRY_DELAYS[min(self._retry_count, len(_RETRY_DELAYS) - 1)]
                self._retry_delay = delay
                self._retry_count += 1
                await self._interruptible_wait(delay)
                continue

            # Wait until the radio dies, the daemon signals disconnect, or we are stopped
            self._disconnected_event.clear()
            await self._wait_for_disconnect()

            if self._stop_event.is_set():
                break

            # Radio lost mid-run — notify daemon to tear down, then retry
            self._status = "error"
            self._last_error_at = time.time()
            logger.warning("Radio disconnected — notifying daemon, will retry")

            try:
                await self._on_disconnected()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning("Daemon disconnect callback error: %s", e)

            self._cleanup_radio()
            delay = _RETRY_DELAYS[min(self._retry_count, len(_RETRY_DELAYS) - 1)]
            self._retry_delay = delay
            self._retry_count += 1
            await self._interruptible_wait(delay)

    async def _interruptible_wait(self, delay: int) -> None:
        """Wait for delay seconds, but return early if stop or retry-now is signalled."""
        try:
            await asyncio.wait_for(
                asyncio.shield(self._retry_now_event.wait()),
                timeout=delay,
            )
            self._retry_now_event.clear()
        except asyncio.TimeoutError:
            pass

    async def _wait_for_disconnect(self) -> None:
        """Block until disconnected_event or stop_event fires."""
        stop_wait = asyncio.ensure_future(self._stop_event.wait())
        disc_wait = asyncio.ensure_future(self._disconnected_event.wait())
        try:
            done, pending = await asyncio.wait(
                [stop_wait, disc_wait],
                return_when=asyncio.FIRST_COMPLETED,
            )
            for t in pending:
                t.cancel()
        except asyncio.CancelledError:
            stop_wait.cancel()
            disc_wait.cancel()
            raise
