"""
Companion frame protocol TCP server.

Implements the MeshCore Companion Radio Protocol over TCP for standard clients.
Frame format: outbound '>' + 2-byte len (LE) + data; inbound '<' + 2-byte len + data.
"""

import asyncio
import base64
import logging
import struct
import time
from typing import Optional

from pymc_core.companion.constants import ADV_TYPE_CHAT
from pymc_core.companion.models import Contact, QueuedMessage

from .constants import (
    RESP_CODE_DEVICE_INFO,
    CMD_ADD_UPDATE_CONTACT,
    CMD_GET_CHANNEL,
    CMD_GET_CONTACT_BY_KEY,
    CMD_SET_CHANNEL,
    CMD_SET_FLOOD_SCOPE,
    CMD_APP_START,
    CMD_DEVICE_QUERY,
    CMD_GET_ADVERT_PATH,
    CMD_GET_BATT_AND_STORAGE,
    CMD_GET_CONTACTS,
    CMD_GET_STATS,
    CMD_IMPORT_CONTACT,
    CMD_REMOVE_CONTACT,
    CMD_RESET_PATH,
    CMD_SEND_BINARY_REQ,
    CMD_SEND_PATH_DISCOVERY_REQ,
    CMD_SEND_CONTROL_DATA,
    CMD_SEND_CHANNEL_TXT_MSG,
    CMD_SEND_LOGIN,
    CMD_SEND_SELF_ADVERT,
    CMD_SEND_STATUS_REQ,
    CMD_SEND_TELEMETRY_REQ,
    CMD_SEND_TRACE_PATH,
    CMD_SEND_TXT_MSG,
    CMD_SET_ADVERT_LATLON,
    CMD_SET_ADVERT_NAME,
    CMD_SYNC_NEXT_MESSAGE,
    ERR_CODE_BAD_STATE,
    ERR_CODE_ILLEGAL_ARG,
    ERR_CODE_NOT_FOUND,
    ERR_CODE_TABLE_FULL,
    ERR_CODE_UNSUPPORTED_CMD,
    FRAME_INBOUND_PREFIX,
    FRAME_OUTBOUND_PREFIX,
    MAX_FRAME_SIZE,
    MAX_PATH_SIZE,
    PUB_KEY_SIZE,
    PUSH_CODE_ADVERT,
    PUSH_CODE_BINARY_RESPONSE,
    PUSH_CODE_LOGIN_FAIL,
    PUSH_CODE_LOGIN_SUCCESS,
    PUSH_CODE_LOG_RX_DATA,
    PUSH_CODE_NEW_ADVERT,
    PUSH_CODE_TRACE_DATA,
    PUSH_CODE_MSG_WAITING,
    PUSH_CODE_PATH_UPDATED,
    PUSH_CODE_SEND_CONFIRMED,
    PUSH_CODE_STATUS_RESPONSE,
    PUSH_CODE_TELEMETRY_RESPONSE,
    RESP_CODE_ADVERT_PATH,
    RESP_CODE_BATT_AND_STORAGE,
    RESP_CODE_CHANNEL_INFO,
    RESP_CODE_CHANNEL_MSG_RECV,
    RESP_CODE_CHANNEL_MSG_RECV_V3,
    RESP_CODE_CONTACT,
    RESP_CODE_CONTACT_MSG_RECV_V3,
    RESP_CODE_CONTACT_MSG_RECV,
    RESP_CODE_CONTACTS_START,
    RESP_CODE_END_OF_CONTACTS,
    RESP_CODE_ERR,
    RESP_CODE_NO_MORE_MESSAGES,
    RESP_CODE_OK,
    RESP_CODE_SELF_INFO,
    RESP_CODE_SENT,
    RESP_CODE_STATS,
    STATS_TYPE_CORE,
    STATS_TYPE_PACKETS,
    STATS_TYPE_RADIO,
    PUSH_CODE_PATH_DISCOVERY_RESPONSE,
    PUSH_CODE_CONTROL_DATA,
)

logger = logging.getLogger("CompanionFrameServer")


class CompanionFrameServer:
    """TCP server for the MeshCore companion frame protocol.

    One client per companion at a time. Listens on configured port.
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
        self.bridge = bridge
        self.companion_hash = companion_hash
        self.port = port
        self.bind_address = bind_address
        self.sqlite_handler = sqlite_handler
        self.local_hash = local_hash  # Repeater's node hash; if path ends with this, we are final hop
        self.stats_getter = stats_getter  # Optional (stats_type: int) -> dict for companion stats
        self._control_handler = control_handler  # Optional; used to register/clear discovery callbacks so "No callback waiting" is not logged
        self._server: Optional[asyncio.Server] = None
        self._client_writer: Optional[asyncio.StreamWriter] = None
        self._client_reader: Optional[asyncio.StreamReader] = None
        self._app_target_ver = 0

    async def start(self) -> None:
        """Start the TCP server."""
        self._server = await asyncio.start_server(
            self._handle_client,
            self.bind_address,
            self.port,
        )
        addr = self._server.sockets[0].getsockname() if self._server.sockets else (self.bind_address, self.port)
        logger.info(f"Companion frame server listening on {addr[0]}:{addr[1]} (hash=0x{int(self.companion_hash):02x})")

    async def stop(self) -> None:
        """Stop the TCP server and disconnect any client."""
        if self._client_writer:
            try:
                self._client_writer.close()
                await self._client_writer.wait_closed()
            except Exception:
                pass
            self._client_writer = None
            self._client_reader = None
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
        logger.info(f"Companion frame server stopped (port={self.port})")

    def _persist_companion_message(self, msg_dict: dict) -> None:
        """Persist a message to SQLite and remove it from the bridge queue so it is delivered once from SQLite."""
        if not self.sqlite_handler:
            return
        self.sqlite_handler.companion_push_message(self.companion_hash, msg_dict)
        self.bridge.message_queue.pop_last()

    def _setup_push_callbacks(self) -> None:
        """Subscribe to bridge events and send PUSH frames to connected client."""

        def _write_push(data: bytes) -> None:
            if self._client_writer and not self._client_writer.is_closing():
                try:
                    frame = bytes([FRAME_OUTBOUND_PREFIX]) + struct.pack("<H", len(data)) + data
                    self._client_writer.write(frame)
                    asyncio.create_task(self._drain_writer())
                except Exception as e:
                    logger.debug(f"Push write error: {e}")

        async def on_message_received(sender_key, text, timestamp, txt_type):
            msg_dict = {
                "sender_key": sender_key,
                "text": text,
                "timestamp": timestamp,
                "txt_type": txt_type,
                "is_channel": False,
                "channel_idx": 0,
                "path_len": 0,
            }
            self._persist_companion_message(msg_dict)
            _write_push(bytes([PUSH_CODE_MSG_WAITING]))

        async def on_send_confirmed(crc):
            data = struct.pack("<B4sI", PUSH_CODE_SEND_CONFIRMED, struct.pack("<I", crc)[:4], 0)
            _write_push(data)

        async def on_advert_received(contact):
            if isinstance(contact, dict):
                pubkey = contact.get("public_key", b"")
                if isinstance(pubkey, str):
                    pubkey = bytes.fromhex(pubkey)
            else:
                pubkey = getattr(contact, "public_key", getattr(contact, "pub_key", b""))
            if isinstance(pubkey, str):
                pubkey = bytes.fromhex(pubkey)
            if len(pubkey) < 32:
                return
            _write_push(bytes([PUSH_CODE_ADVERT]) + pubkey[:32])
            # Full contact push (PUSH_CODE_NEW_ADVERT) so app gets NEW_CONTACT and can add to list
            if not isinstance(contact, dict) and hasattr(contact, "name") and contact.name:
                pubkey_b = pubkey[:32] if isinstance(pubkey, bytes) else bytes.fromhex(str(pubkey))[:32]
                name_b = (contact.name.encode("utf-8")[:32] if isinstance(contact.name, str) else contact.name[:32]).ljust(32, b"\x00")
                opl = getattr(contact, "out_path_len", -1)
                opl_byte = 0xFF if opl < 0 else min(opl, 255)
                out_path = getattr(contact, "out_path", b"") or b""
                if isinstance(out_path, str):
                    out_path = bytes.fromhex(out_path) if out_path else b""
                elif isinstance(out_path, (list, bytearray)):
                    out_path = bytes(out_path)
                out_path = out_path[:MAX_PATH_SIZE].ljust(MAX_PATH_SIZE, b"\x00")
                adv_type = getattr(contact, "adv_type", 0)
                flags = getattr(contact, "flags", 0)
                last_advert = getattr(contact, "last_advert_timestamp", 0)
                gps_lat = getattr(contact, "gps_lat", 0.0)
                gps_lon = getattr(contact, "gps_lon", 0.0)
                lastmod = getattr(contact, "lastmod", 0)
                frame = (
                    bytes([PUSH_CODE_NEW_ADVERT])
                    + pubkey_b
                    + bytes([adv_type, flags, opl_byte])
                    + out_path
                    + name_b
                    + struct.pack("<I", last_advert)
                    + struct.pack("<i", int(gps_lat * 1e6))
                    + struct.pack("<i", int(gps_lon * 1e6))
                    + struct.pack("<I", lastmod)
                )
                _write_push(frame)

        async def on_contact_path_updated(pub_key, path_len, path):
            if isinstance(pub_key, bytes) and len(pub_key) >= 32:
                _write_push(bytes([PUSH_CODE_PATH_UPDATED]) + pub_key[:32])

        async def on_channel_message_received(
            channel_name, sender_name, message_text, timestamp, path_len=0, channel_idx=0
        ):
            msg_dict = {
                "sender_key": b"",
                "text": message_text,
                "timestamp": timestamp,
                "txt_type": 0,
                "is_channel": True,
                "channel_idx": channel_idx,
                "path_len": path_len,
            }
            self._persist_companion_message(msg_dict)
            _write_push(bytes([PUSH_CODE_MSG_WAITING]))

        async def on_binary_response(tag_bytes, response_data, parsed=None, request_type=None):
            # PUSH_CODE_BINARY_RESPONSE: 0x8C + reserved(1) + tag(4) + response_payload
            frame = (
                bytes([PUSH_CODE_BINARY_RESPONSE, 0])
                + (tag_bytes if isinstance(tag_bytes, bytes) else struct.pack("<I", tag_bytes))
                + response_data
            )
            _write_push(frame)

        async def on_path_discovery_response(tag_bytes, contact_pubkey, out_path, in_path):
            # PUSH_CODE_PATH_DISCOVERY_RESPONSE 0x8D: reserved(1) + pub_key_prefix(6) + out_path_len(1) + out_path + in_path_len(1) + in_path
            pub_key_prefix = (contact_pubkey if isinstance(contact_pubkey, bytes) else bytes.fromhex(contact_pubkey))[:6]
            out_path = out_path if isinstance(out_path, bytes) else bytes(out_path)
            in_path = in_path if isinstance(in_path, bytes) else bytes(in_path)
            frame = (
                bytes([PUSH_CODE_PATH_DISCOVERY_RESPONSE, 0])
                + pub_key_prefix
                + bytes([len(out_path)])
                + out_path
                + bytes([len(in_path)])
                + in_path
            )
            _write_push(frame)

        self.bridge.on_message_received(on_message_received)
        self.bridge.on_channel_message_received(on_channel_message_received)
        self.bridge.on_send_confirmed(on_send_confirmed)
        self.bridge.on_advert_received(on_advert_received)
        self.bridge.on_contact_path_updated(on_contact_path_updated)
        self.bridge.on_binary_response(on_binary_response)
        self.bridge.on_path_discovery_response(on_path_discovery_response)

    def push_trace_data(
        self,
        path_len: int,
        flags: int,
        tag: int,
        auth_code: int,
        path_hashes: bytes,
        path_snrs: bytes,
        final_snr_byte: int,
    ) -> None:
        """Push PUSH_CODE_TRACE_DATA (0x89) to client. Matches firmware onTraceRecv() frame format."""
        if not self._client_writer or self._client_writer.is_closing():
            return
        # Firmware: code(1) + reserved(1) + path_len(1) + flags(1) + tag(4) + auth(4) + path_hashes + path_snrs + final_snr(1)
        path_sz = flags & 0x03
        expected_snr_len = path_len >> path_sz
        if len(path_snrs) != expected_snr_len:
            logger.debug("push_trace_data: path_snrs len %s != expected %s", len(path_snrs), expected_snr_len)
            return
        data = (
            bytes([PUSH_CODE_TRACE_DATA, 0, path_len, flags])
            + struct.pack("<II", tag & 0xFFFFFFFF, auth_code & 0xFFFFFFFF)
            + path_hashes
            + path_snrs
            + bytes([final_snr_byte & 0xFF])
        )
        try:
            frame = bytes([FRAME_OUTBOUND_PREFIX]) + struct.pack("<H", len(data)) + data
            self._client_writer.write(frame)
            asyncio.create_task(self._drain_writer())
        except Exception as e:
            logger.debug("push_trace_data error: %s", e)

    def push_rx_raw(self, snr: float, rssi: int, raw: bytes) -> None:
        """Push raw RX packet to client (PUSH_CODE_LOG_RX_DATA 0x88). Matches firmware logRxRaw() so client can track repeats by packet hash."""
        if not self._client_writer or self._client_writer.is_closing():
            logger.debug("push_rx_raw: no client connected (companion %s)", self.companion_hash)
            return
        # Firmware: code(1) + snr(1) + rssi(1) + raw; snr = (int8)(snr*4), rssi = (int8)rssi
        snr_byte = max(-128, min(127, int(round(snr * 4))))
        rssi_byte = max(-128, min(127, int(rssi)))
        if snr_byte < 0:
            snr_byte += 256
        if rssi_byte < 0:
            rssi_byte += 256
        payload_len = min(len(raw), MAX_FRAME_SIZE - 3)
        data = bytes([PUSH_CODE_LOG_RX_DATA, snr_byte & 0xFF, rssi_byte & 0xFF]) + raw[:payload_len]
        try:
            frame = bytes([FRAME_OUTBOUND_PREFIX]) + struct.pack("<H", len(data)) + data
            self._client_writer.write(frame)
            asyncio.create_task(self._drain_writer())
        except Exception as e:
            logger.debug("Push RX raw error: %s", e)

    async def push_control_data(
        self,
        snr: float,
        rssi: int,
        path_len: int,
        path_bytes: bytes,
        payload: bytes,
    ) -> None:
        """Push CONTROL packet to client (PUSH_CODE_CONTROL_DATA 0x8E). Spec: code, SNR*4, RSSI (signed), path_len, payload (no path bytes). Frame layout matches meshcore_py reader (PacketType.CONTROL_DATA) and firmware MyMesh::onControlDataRecv. See docs/companion-discovery.md for discovery payload layout."""
        if not self._client_writer or self._client_writer.is_closing():
            logger.warning("Push control data skipped: no client connection")
            return
        # Discovery response (0x90): clear the no-op callback we registered for this tag
        if self._control_handler and len(payload) >= 6 and (payload[0] & 0xF0) == 0x90:
            tag = struct.unpack("<I", payload[2:6])[0]
            self._control_handler.clear_response_callback(tag)
        # Wire format: int8 SNR (×4), int8 RSSI (dBm); same two's-complement byte as meshcore_py/firmware
        snr_val = snr if isinstance(snr, (int, float)) else 0.0
        rssi_val = rssi if isinstance(rssi, (int, float)) else 0
        snr_byte = max(-128, min(127, int(round(float(snr_val) * 4))))
        rssi_byte = max(-128, min(127, int(rssi_val)))
        if snr_byte < 0:
            snr_byte += 256
        if rssi_byte < 0:
            rssi_byte += 256
        # Match firmware MyMesh::onControlDataRecv: code, snr, rssi, path_len, payload (no path bytes)
        path_len_byte = max(0, min(255, int(path_len) if path_len is not None else 0))
        payload_max = MAX_FRAME_SIZE - 4
        payload_slice = bytes(payload[:payload_max]) if payload else b""
        data = (
            bytes([PUSH_CODE_CONTROL_DATA, snr_byte & 0xFF, rssi_byte & 0xFF, path_len_byte])
            + payload_slice
        )
        try:
            frame = bytes([FRAME_OUTBOUND_PREFIX]) + struct.pack("<H", len(data)) + data
            self._client_writer.write(frame)
            await self._drain_writer()
            logger.debug(
                "Pushed control data 0x8E to client: payload_len=%s, frame_len=%s",
                len(payload_slice), len(frame),
            )
            # DEBUG: exact wire format for comparison with protocol/firmware (code, snr, rssi, path_len, payload...)
            logger.debug(
                "0x8E frame data: %s (snr_byte=%s rssi_byte=%s path_len=%s)",
                data.hex(), snr_byte & 0xFF, rssi_byte & 0xFF, path_len_byte,
            )
        except Exception as e:
            logger.warning("Push control data error: %s", e)

    async def _drain_writer(self) -> None:
        if self._client_writer:
            try:
                await self._client_writer.drain()
            except Exception:
                pass

    def _write_frame(self, data: bytes) -> None:
        """Send a frame to the connected client (outbound format)."""
        if self._client_writer and not self._client_writer.is_closing():
            frame = bytes([FRAME_OUTBOUND_PREFIX]) + struct.pack("<H", len(data)) + data
            self._client_writer.write(frame)

    def _write_ok(self) -> None:
        self._write_frame(bytes([RESP_CODE_OK]))

    def _write_err(self, err_code: int) -> None:
        self._write_frame(bytes([RESP_CODE_ERR, err_code]))

    def _save_contacts(self) -> None:
        """Persist contacts to SQLite."""
        if not self.sqlite_handler:
            return
        contacts = self.bridge.get_contacts()
        dicts = []
        for c in contacts:
            pk = c.public_key if isinstance(c.public_key, bytes) else bytes.fromhex(c.public_key)
            dicts.append({
                "pubkey": pk,
                "name": c.name,
                "adv_type": c.adv_type,
                "flags": c.flags,
                "out_path_len": c.out_path_len,
                "out_path": c.out_path if isinstance(c.out_path, bytes) else (bytes.fromhex(c.out_path) if c.out_path else b""),
                "last_advert_timestamp": c.last_advert_timestamp,
                "lastmod": c.lastmod,
                "gps_lat": c.gps_lat,
                "gps_lon": c.gps_lon,
                "sync_since": c.sync_since,
            })
        self.sqlite_handler.companion_save_contacts(self.companion_hash, dicts)

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """Handle a new client connection. One client at a time."""
        if self._client_writer:
            logger.warning("Companion already has a client; rejecting new connection")
            writer.close()
            await writer.wait_closed()
            return

        self._client_reader = reader
        self._client_writer = writer
        self._setup_push_callbacks()
        logger.info(f"Companion client connected (port={self.port})")

        try:
            while True:
                prefix = await reader.read(1)
                if not prefix:
                    break
                if prefix[0] != FRAME_INBOUND_PREFIX:
                    logger.warning(f"Invalid frame prefix: 0x{prefix[0]:02x}")
                    continue
                len_bytes = await reader.readexactly(2)
                frame_len = struct.unpack("<H", len_bytes)[0]
                if frame_len > MAX_FRAME_SIZE:
                    logger.warning(f"Frame too long: {frame_len}")
                    break
                payload = await reader.readexactly(frame_len)
                await self._handle_cmd(payload)
        except asyncio.IncompleteReadError:
            pass
        except ConnectionResetError:
            pass
        except Exception as e:
            logger.error(f"Client handler error: {e}", exc_info=True)
        finally:
            self._client_writer = None
            self._client_reader = None
            logger.info(f"Companion client disconnected (port={self.port})")

    async def _handle_cmd(self, payload: bytes) -> None:
        """Dispatch command to handler."""
        if not payload:
            return
        cmd = payload[0]
        data = payload[1:]
        # Log every command at INFO so discovery (52) and unsupported are visible in logs
        logger.info("Companion cmd 0x%02x (%s) len=%s", cmd, cmd, len(payload))
        if cmd in (CMD_GET_CHANNEL, CMD_SET_CHANNEL):
            logger.debug(f"Companion cmd 0x{cmd:02x} ({'GET_CHANNEL' if cmd == CMD_GET_CHANNEL else 'SET_CHANNEL'}), payload_len={len(payload)}")

        try:
            if cmd == CMD_APP_START:
                await self._cmd_app_start(data)
            elif cmd == CMD_DEVICE_QUERY:
                await self._cmd_device_query(data)
            elif cmd == CMD_GET_CONTACTS:
                await self._cmd_get_contacts(data)
            elif cmd == CMD_GET_CONTACT_BY_KEY:
                await self._cmd_get_contact_by_key(data)
            elif cmd == CMD_SEND_TXT_MSG:
                await self._cmd_send_txt_msg(data)
            elif cmd == CMD_SEND_CHANNEL_TXT_MSG:
                await self._cmd_send_channel_txt_msg(data)
            elif cmd == CMD_SYNC_NEXT_MESSAGE:
                await self._cmd_sync_next_message(data)
            elif cmd == CMD_SEND_LOGIN:
                await self._cmd_send_login(data)
            elif cmd == CMD_SEND_STATUS_REQ:
                await self._cmd_send_status_req(data)
            elif cmd == CMD_SEND_TELEMETRY_REQ:
                await self._cmd_send_telemetry_req(data)
            elif cmd == CMD_SEND_SELF_ADVERT:
                await self._cmd_send_self_advert(data)
            elif cmd == CMD_SET_ADVERT_NAME:
                await self._cmd_set_advert_name(data)
            elif cmd == CMD_SET_ADVERT_LATLON:
                await self._cmd_set_advert_latlon(data)
            elif cmd == CMD_ADD_UPDATE_CONTACT:
                await self._cmd_add_update_contact(data)
            elif cmd == CMD_REMOVE_CONTACT:
                await self._cmd_remove_contact(data)
            elif cmd == CMD_RESET_PATH:
                await self._cmd_reset_path(data)
            elif cmd == CMD_GET_BATT_AND_STORAGE:
                await self._cmd_get_batt_and_storage(data)
            elif cmd == CMD_GET_STATS:
                await self._cmd_get_stats(data)
            elif cmd == CMD_GET_ADVERT_PATH:
                await self._cmd_get_advert_path(data)
            elif cmd == CMD_IMPORT_CONTACT:
                await self._cmd_import_contact(data)
            elif cmd == CMD_GET_CHANNEL:
                await self._cmd_get_channel(data)
            elif cmd == CMD_SET_CHANNEL:
                await self._cmd_set_channel(data)
            elif cmd == CMD_SEND_BINARY_REQ:
                await self._cmd_send_binary_req(data)
            elif cmd == CMD_SEND_PATH_DISCOVERY_REQ:
                await self._cmd_send_path_discovery_req(data)
            elif cmd == CMD_SEND_CONTROL_DATA:
                await self._cmd_send_control_data(data)
            elif cmd == CMD_SEND_TRACE_PATH:
                await self._cmd_send_trace_path(data)
            elif cmd == CMD_SET_FLOOD_SCOPE:
                # App sends this on connect; no-op for repeater companion (no radio scope)
                self._write_ok()
            else:
                logger.warning(
                    "Companion unsupported cmd 0x%02x (%s) len=%s (expected 52 for path discovery)",
                    cmd, cmd, len(payload),
                )
                self._write_err(ERR_CODE_UNSUPPORTED_CMD)
        except Exception as e:
            logger.error(f"Cmd 0x{cmd:02x} error: {e}", exc_info=True)
            self._write_err(ERR_CODE_ILLEGAL_ARG)

    async def _cmd_app_start(self, data: bytes) -> None:
        if len(data) >= 1:
            self._app_target_ver = data[0]
        # RESP_CODE_SELF_INFO - name is varchar (remainder of frame)
        # Send name without null terminator; client displays remainder of frame as-is
        prefs = self.bridge.get_self_info()
        pubkey = self.bridge.get_public_key()
        name = prefs.node_name.encode("utf-8", errors="replace")
        lat = int(getattr(prefs, "latitude", 0) * 1e6)
        lon = int(getattr(prefs, "longitude", 0) * 1e6)
        frame = (
            bytes([RESP_CODE_SELF_INFO, ADV_TYPE_CHAT, prefs.tx_power_dbm, 22])
            + pubkey
            + struct.pack("<ii", lat, lon)
            + bytes([getattr(prefs, "multi_acks", 0), getattr(prefs, "advert_loc_policy", 0)])
            + bytes([getattr(prefs, "telemetry_mode_base", 0) | (getattr(prefs, "telemetry_mode_location", 0) << 2)])
            + bytes([getattr(prefs, "manual_add_contacts", 0)])
            + struct.pack(
                "<II",
                prefs.frequency_hz // 1000,  # radio_freq: freq * 1000 (e.g. 915 MHz → 915000)
                prefs.bandwidth_hz,  # radio_bw: bandwidth(kHz) * 1000 (e.g. 125 kHz → 125000)
            )
            + bytes([prefs.spreading_factor, prefs.coding_rate])
            + name
        )
        self._write_frame(frame)

    async def _cmd_device_query(self, data: bytes) -> None:
        if len(data) >= 1:
            self._app_target_ver = data[0]
        firmware_ver = 8
        # Protocol: max_contacts_div_2 and max_channels are bytes (ver 3+)
        max_contacts = getattr(
            getattr(self.bridge, "contacts", None), "max_contacts", 1000
        )
        max_channels_val = getattr(
            getattr(self.bridge, "channels", None), "max_channels", 40
        )
        max_contacts_div_2 = min(max_contacts // 2, 255)
        max_channels = min(max_channels_val, 255)
        ble_pin = 0
        build_date = b"13 Feb 2026\x00"[:12].ljust(12, b"\x00")
        model = b"pyMC-Repeater-Companion\x00"[:40].ljust(40, b"\x00")
        version = b"1.0.0\x00"[:20].ljust(20, b"\x00")
        frame = (
            bytes([RESP_CODE_DEVICE_INFO, firmware_ver, max_contacts_div_2, max_channels])
            + struct.pack("<I", ble_pin)
            + build_date
            + model
            + version
        )
        self._write_frame(frame)

    async def _cmd_get_contacts(self, data: bytes) -> None:
        since = struct.unpack("<I", data[:4])[0] if len(data) >= 4 else 0
        contacts = self.bridge.get_contacts(since=since)
        self._write_frame(bytes([RESP_CODE_CONTACTS_START]) + struct.pack("<I", len(contacts)))
        for c in contacts:
            pubkey = c.public_key if isinstance(c.public_key, bytes) else bytes.fromhex(c.public_key)
            name = (c.name.encode("utf-8")[:32] if isinstance(c.name, str) else c.name[:32]).ljust(32, b"\x00")
            # out_path_len is signed byte: -1 (unknown) -> 0xFF, else 0-255
            opl = c.out_path_len if hasattr(c, "out_path_len") else -1
            opl_byte = 0xFF if opl < 0 else min(opl, 255)
            frame = (
                bytes([RESP_CODE_CONTACT])
                + pubkey
                + bytes([c.adv_type if hasattr(c, "adv_type") else 0, c.flags if hasattr(c, "flags") else 0])
                + bytes([opl_byte])
                + (c.out_path[:MAX_PATH_SIZE] if hasattr(c, "out_path") and c.out_path else b"").ljust(MAX_PATH_SIZE, b"\x00")
                + name
                + struct.pack("<I", c.last_advert_timestamp if hasattr(c, "last_advert_timestamp") else 0)
                + struct.pack("<i", int((c.gps_lat if hasattr(c, "gps_lat") else 0) * 1e6))
                + struct.pack("<i", int((c.gps_lon if hasattr(c, "gps_lon") else 0) * 1e6))
                + struct.pack("<I", c.lastmod if hasattr(c, "lastmod") else 0)
            )
            self._write_frame(frame)
        most_recent = max((c.lastmod for c in contacts), default=0)
        self._write_frame(bytes([RESP_CODE_END_OF_CONTACTS]) + struct.pack("<I", most_recent))

    async def _cmd_get_contact_by_key(self, data: bytes) -> None:
        """Handle CMD_GET_CONTACT_BY_KEY (0x1e): lookup by 32-byte pubkey, respond with RESP_CODE_CONTACT or ERR."""
        if len(data) < PUB_KEY_SIZE:
            self._write_err(ERR_CODE_ILLEGAL_ARG)
            return
        pubkey = data[:PUB_KEY_SIZE]
        contact = self.bridge.contacts.get_by_key(pubkey) if hasattr(self.bridge.contacts, "get_by_key") else None
        if not contact:
            self._write_err(ERR_CODE_NOT_FOUND)
            return
        c = contact
        pubkey_b = c.public_key if isinstance(c.public_key, bytes) else bytes.fromhex(c.public_key)
        name = (c.name.encode("utf-8")[:32] if isinstance(c.name, str) else c.name[:32]).ljust(32, b"\x00")
        opl = c.out_path_len if hasattr(c, "out_path_len") else -1
        opl_byte = 0xFF if opl < 0 else min(opl, 255)
        frame = (
            bytes([RESP_CODE_CONTACT])
            + pubkey_b
            + bytes([c.adv_type if hasattr(c, "adv_type") else 0, c.flags if hasattr(c, "flags") else 0])
            + bytes([opl_byte])
            + (c.out_path[:MAX_PATH_SIZE] if hasattr(c, "out_path") and c.out_path else b"").ljust(MAX_PATH_SIZE, b"\x00")
            + name
            + struct.pack("<I", c.last_advert_timestamp if hasattr(c, "last_advert_timestamp") else 0)
            + struct.pack("<i", int((c.gps_lat if hasattr(c, "gps_lat") else 0) * 1e6))
            + struct.pack("<i", int((c.gps_lon if hasattr(c, "gps_lon") else 0) * 1e6))
            + struct.pack("<I", c.lastmod if hasattr(c, "lastmod") else 0)
        )
        self._write_frame(frame)

    async def _cmd_send_txt_msg(self, data: bytes) -> None:
        if len(data) < 12:
            self._write_err(ERR_CODE_ILLEGAL_ARG)
            return
        txt_type = data[0]
        attempt = data[1]
        sender_ts = struct.unpack_from("<I", data, 2)[0]
        pubkey_prefix = data[6:12]
        text = data[12:].decode("utf-8", errors="replace").rstrip("\x00")
        contact = self.bridge.contacts.get_by_key_prefix(pubkey_prefix) if hasattr(self.bridge.contacts, "get_by_key_prefix") else None
        if not contact:
            for c in self.bridge.get_contacts():
                pk = c.public_key if isinstance(c.public_key, bytes) else bytes.fromhex(c.public_key)
                if pk[:6] == pubkey_prefix:
                    contact = c
                    break
        if not contact:
            self._write_err(ERR_CODE_NOT_FOUND)
            return
        pubkey = contact.public_key if isinstance(contact.public_key, bytes) else bytes.fromhex(contact.public_key)
        result = await self.bridge.send_text_message(pubkey, text, txt_type=txt_type, attempt=attempt + 1)
        if result.success:
            ack = result.expected_ack or 0
            timeout = result.timeout_ms or 5000
            frame = bytes([RESP_CODE_SENT, 1 if result.is_flood else 0]) + struct.pack("<II", ack, timeout)
            self._write_frame(frame)
        else:
            self._write_err(ERR_CODE_BAD_STATE)

    async def _cmd_send_channel_txt_msg(self, data: bytes) -> None:
        if len(data) < 6:
            self._write_err(ERR_CODE_ILLEGAL_ARG)
            return
        # Protocol: txt_type(1) + channel_idx(1) + sender_timestamp(4) + text (matches firmware/meshcore_py)
        txt_type = data[0]
        channel_idx = data[1]
        sender_ts = struct.unpack_from("<I", data, 2)[0]
        text = data[6:].decode("utf-8", errors="replace").rstrip("\x00")
        if txt_type != 0:  # TXT_TYPE_PLAIN
            logger.debug("CMD_SEND_CHANNEL_TXT_MSG: unsupported txt_type=%s", txt_type)
            self._write_err(ERR_CODE_UNSUPPORTED_CMD)
            return
        if self.bridge.get_channel(channel_idx) is None:
            logger.info("CMD_SEND_CHANNEL_TXT_MSG: channel idx %s not found", channel_idx)
            self._write_err(ERR_CODE_NOT_FOUND)
            return
        ok = await self.bridge.send_channel_message(channel_idx, text)
        if ok:
            self._write_ok()
        else:
            logger.warning("CMD_SEND_CHANNEL_TXT_MSG: send failed for channel %s", channel_idx)
            self._write_err(ERR_CODE_BAD_STATE)

    async def _cmd_send_binary_req(self, data: bytes) -> None:
        # CMD_SEND_BINARY_REQ: pubkey(32) + req_data (request_type(1) + optional payload)
        if len(data) < 33:
            self._write_err(ERR_CODE_ILLEGAL_ARG)
            return
        pubkey = data[:32]
        req_data = data[32:]
        send_binary_req = getattr(self.bridge, "send_binary_req", None)
        if not send_binary_req:
            self._write_err(ERR_CODE_UNSUPPORTED_CMD)
            return
        try:
            result = await send_binary_req(pubkey, req_data)
        except Exception as e:
            logger.error(f"send_binary_req error: {e}", exc_info=True)
            self._write_err(ERR_CODE_ILLEGAL_ARG)
            return
        if not result.success:
            self._write_err(ERR_CODE_NOT_FOUND)
            return
        # RESP_CODE_SENT: 0x06 + flood(1) + tag(4 LE) + timeout(4 LE)
        tag = result.expected_ack if result.expected_ack is not None else 0
        timeout_ms = result.timeout_ms if result.timeout_ms is not None else 10000
        frame = bytes([RESP_CODE_SENT, 1 if result.is_flood else 0]) + struct.pack("<II", tag, timeout_ms)
        self._write_frame(frame)

    async def _cmd_send_control_data(self, data: bytes) -> None:
        # CMD_SEND_CONTROL_DATA (55): first byte is flags/type (0x80 = DISCOVER_REQ). Firmware: (cmd_frame[1] & 0x80) != 0.
        if len(data) < 2:
            self._write_err(ERR_CODE_ILLEGAL_ARG)
            return
        if (data[0] & 0x80) == 0:
            self._write_err(ERR_CODE_ILLEGAL_ARG)
            return
        # Discovery request: register a no-op response callback so ControlHandler won't log "No callback waiting"
        if self._control_handler and len(data) >= 6 and (data[0] & 0xF0) == 0x80:
            tag = struct.unpack("<I", data[2:6])[0]
            self._control_handler.set_response_callback(tag, lambda _: None)
        send_control = getattr(self.bridge, "send_control_data", None)
        if not send_control:
            self._write_err(ERR_CODE_UNSUPPORTED_CMD)
            return
        try:
            ok = await send_control(data)
        except Exception as e:
            logger.error("send_control_data error: %s", e, exc_info=True)
            self._write_err(ERR_CODE_ILLEGAL_ARG)
            return
        if ok:
            self._write_ok()
            await self._drain_writer()
        else:
            self._write_err(ERR_CODE_TABLE_FULL)

    async def _cmd_send_path_discovery_req(self, data: bytes) -> None:
        # CMD_SEND_PATH_DISCOVERY_REQ (52): reserved(1) + pub_key(32). Firmware: cmd_frame[1]==0, cmd_frame[2:34]=pub_key.
        logger.info("Path discovery request received (cmd 52), data_len=%s", len(data))
        if len(data) < 33:
            self._write_err(ERR_CODE_ILLEGAL_ARG)
            return
        pub_key = data[1:33]
        send_req = getattr(self.bridge, "send_path_discovery_req", None)
        if not send_req:
            self._write_err(ERR_CODE_UNSUPPORTED_CMD)
            return
        try:
            result = await send_req(pub_key)
        except Exception as e:
            logger.error("send_path_discovery_req error: %s", e, exc_info=True)
            self._write_err(ERR_CODE_ILLEGAL_ARG)
            return
        if not result.success:
            self._write_err(ERR_CODE_NOT_FOUND)
            return
        tag = result.expected_ack if result.expected_ack is not None else 0
        timeout_ms = result.timeout_ms if result.timeout_ms is not None else 10000
        frame = bytes([RESP_CODE_SENT, 1 if result.is_flood else 0]) + struct.pack("<II", tag, timeout_ms)
        self._write_frame(frame)

    async def _cmd_send_trace_path(self, data: bytes) -> None:
        # CMD_SEND_TRACE_PATH: tag(4) + auth(4) + flags(1) + path_bytes (firmware MyMesh.cpp)
        if len(data) < 10:
            self._write_err(ERR_CODE_ILLEGAL_ARG)
            return
        tag = struct.unpack_from("<I", data, 0)[0]
        auth_code = struct.unpack_from("<I", data, 4)[0]
        flags = data[8]
        path_bytes = data[9:]
        path_len = len(path_bytes)
        path_sz = flags & 0x03
        # Firmware: (path_len >> path_sz) <= MAX_PATH_SIZE and path_len % (1 << path_sz) == 0
        if (path_len >> path_sz) > MAX_PATH_SIZE or (path_len % (1 << path_sz)) != 0:
            self._write_err(ERR_CODE_ILLEGAL_ARG)
            return
        send_raw = getattr(self.bridge, "send_trace_path_raw", None)
        if not send_raw:
            self._write_err(ERR_CODE_UNSUPPORTED_CMD)
            return
        try:
            ok = await send_raw(tag, auth_code, flags, path_bytes)
        except Exception as e:
            logger.error(f"send_trace_path error: {e}", exc_info=True)
            self._write_err(ERR_CODE_ILLEGAL_ARG)
            return
        if not ok:
            self._write_err(ERR_CODE_TABLE_FULL)
            return
        # RESP_CODE_SENT + 0 (not flood) + tag(4) + est_timeout(4) = 10 bytes (firmware)
        est_timeout_ms = 5000 + (path_len * 200)
        frame = bytes([RESP_CODE_SENT, 0]) + struct.pack("<II", tag, est_timeout_ms)
        self._write_frame(frame)
        # If we are the final hop (path ends with our node), we never receive our own TX; push 0x89 now.
        if path_bytes and self.local_hash is not None and path_bytes[-1] == self.local_hash:
            path_sz = flags & 0x03
            snr_len = path_len >> path_sz
            path_snrs = bytes(snr_len)  # no RX SNR when we're the sender
            final_snr_byte = 0
            self.push_trace_data(
                path_len, flags, tag, auth_code, path_bytes, path_snrs, final_snr_byte
            )

    async def _cmd_sync_next_message(self, data: bytes) -> None:
        msg = self.bridge.sync_next_message()
        if msg is None and self.sqlite_handler:
            msg_dict = self.sqlite_handler.companion_pop_message(self.companion_hash)
            if msg_dict:
                msg = QueuedMessage(
                    sender_key=msg_dict.get("sender_key", b""),
                    txt_type=msg_dict.get("txt_type", 0),
                    timestamp=msg_dict.get("timestamp", 0),
                    text=msg_dict.get("text", ""),
                    is_channel=bool(msg_dict.get("is_channel", False)),
                    channel_idx=msg_dict.get("channel_idx", 0),
                    path_len=msg_dict.get("path_len", 0),
                )
        if msg is None:
            self._write_frame(bytes([RESP_CODE_NO_MORE_MESSAGES]))
            return
        if msg.is_channel:
            # Layout must match meshcore_py reader.py (PacketType.CHANNEL_MSG_RECV and type 17)
            # so client can group repeats by (channel_idx, sender_timestamp, text); path_len differs per repeat.
            path_len_byte = msg.path_len if msg.path_len < 256 else 0xFF
            txt_type = 0  # TXT_TYPE_PLAIN
            text_bytes = (msg.text or "").rstrip("\x00").encode("utf-8", errors="replace")
            if self._app_target_ver >= 3:
                # V3: code(1) + snr(1) + reserved(2) + channel_idx(1) + path_len(1) + txt_type(1) + timestamp(4) + text
                frame = bytes([
                    RESP_CODE_CHANNEL_MSG_RECV_V3,
                    0, 0, 0,  # snr + reserved
                    msg.channel_idx,
                    path_len_byte,
                    txt_type,
                ]) + struct.pack("<I", msg.timestamp) + text_bytes
            else:
                # Type 8: code(1) + channel_idx(1) + path_len(1) + txt_type(1) + timestamp(4) + text
                frame = bytes([RESP_CODE_CHANNEL_MSG_RECV, msg.channel_idx, path_len_byte, txt_type])
                frame += struct.pack("<I", msg.timestamp) + text_bytes
        else:
            prefix = msg.sender_key[:6] if len(msg.sender_key) >= 6 else msg.sender_key.ljust(6, b"\x00")
            path_len_byte = msg.path_len if msg.path_len < 256 else 0xFF
            text_bytes = msg.text.encode("utf-8", errors="replace")
            if self._app_target_ver >= 3:
                frame = bytes([
                    RESP_CODE_CONTACT_MSG_RECV_V3,
                    0, 0, 0,  # snr + reserved
                ]) + prefix + bytes([path_len_byte, msg.txt_type]) + struct.pack("<I", msg.timestamp) + text_bytes
            else:
                frame = bytes([RESP_CODE_CONTACT_MSG_RECV]) + prefix + bytes([path_len_byte, msg.txt_type])
                frame += struct.pack("<I", msg.timestamp) + text_bytes
        self._write_frame(frame)

    async def _cmd_send_login(self, data: bytes) -> None:
        if len(data) < 32:
            self._write_err(ERR_CODE_ILLEGAL_ARG)
            return
        pubkey = data[:32]
        password = data[32:].decode("utf-8", errors="replace").rstrip("\x00") if len(data) > 32 else ""
        self._write_frame(bytes([RESP_CODE_SENT, 1]) + struct.pack("<II", 0, 10000))
        result = await self.bridge.send_login(pubkey, password)
        if result.get("success"):
            self._write_frame(
                bytes([PUSH_CODE_LOGIN_SUCCESS, 1 if result.get("is_admin") else 0])
                + pubkey[:6]
                + struct.pack("<I", result.get("tag", 0))
                + bytes([result.get("acl_permissions", 0)])
            )
        else:
            self._write_frame(bytes([PUSH_CODE_LOGIN_FAIL, 0]) + pubkey[:6])

    async def _cmd_send_status_req(self, data: bytes) -> None:
        # CMD_SEND_STATUS_REQ (27): pub_key(32).
        # Firmware: cmd_frame[0]=CMD, pub_key = &cmd_frame[1]; len >= 1+PUB_KEY_SIZE
        # data here is payload[1:] so data = pub_key(32).  No reserved byte.
        if len(data) < 32:
            self._write_err(ERR_CODE_ILLEGAL_ARG)
            return
        pubkey = data[0:32]
        # Immediate RESP_CODE_SENT so client knows the request was dispatched
        self._write_frame(bytes([RESP_CODE_SENT, 0]) + struct.pack("<II", 0, 15000))
        await self._drain_writer()
        result = await self.bridge.send_status_request(pubkey)
        logger.debug(
            f"STATUS_REQ result: success={result.get('success')}, "
            f"stats_type={result.get('stats', {}).get('type', 'N/A')}, "
            f"raw_bytes_len={len(result.get('stats', {}).get('raw_bytes', b''))}"
        )
        if not result.get("success"):
            # No response or decryption failed — send empty status frame
            self._write_frame(bytes([PUSH_CODE_STATUS_RESPONSE, 0]) + pubkey[:6])
            return
        # Push PUSH_CODE_STATUS_RESPONSE (0x87): reserved(1) + pub_key_prefix(6) + raw_stats_bytes
        # The companion app expects the raw RepeaterStats struct (binary) after the prefix,
        # exactly as the firmware forwards data[4:] from onContactResponse.
        stats_data = result.get("stats", {})
        raw_bytes = stats_data.get("raw_bytes", b"")
        if not raw_bytes:
            # No binary stats available — send empty status frame.
            # NEVER send formatted text as raw bytes: the companion app
            # interprets the frame as a binary RepeaterStats struct and
            # would display garbled values.
            logger.warning(
                "STATUS_REQ: no raw_bytes in stats (type=%s), sending empty status frame",
                stats_data.get("type", "unknown"),
            )
            self._write_frame(bytes([PUSH_CODE_STATUS_RESPONSE, 0]) + pubkey[:6])
            return
        logger.debug(
            f"STATUS_REQ PUSH: raw_bytes={len(raw_bytes)}B, "
            f"first 16: {raw_bytes[:16].hex() if len(raw_bytes) >= 16 else raw_bytes.hex()}"
        )
        self._write_frame(
            bytes([PUSH_CODE_STATUS_RESPONSE, 0])
            + pubkey[:6]
            + raw_bytes
        )

    async def _cmd_send_telemetry_req(self, data: bytes) -> None:
        # CMD_SEND_TELEMETRY_REQ (39): reserved(3) + pub_key(32) + optional flags(1).
        # Firmware: cmd_frame[0]=CMD, reserved(3), pub_key = &cmd_frame[4]; len >= 4+PUB_KEY_SIZE
        # data here is payload[1:] so data = reserved(3) + pub_key(32) + optional flags.
        if len(data) < 35:
            self._write_err(ERR_CODE_ILLEGAL_ARG)
            return
        pubkey = data[3:35]
        # The 3 reserved bytes (data[0..2]) are unused by firmware.
        # Default to requesting all telemetry categories.
        flags = 0x07  # request all: base + location + environment
        want_base = bool(flags & 0x01)
        want_location = bool(flags & 0x02)
        want_environment = bool(flags & 0x04)
        # Immediate RESP_CODE_SENT so client knows the request was dispatched
        self._write_frame(bytes([RESP_CODE_SENT, 0]) + struct.pack("<II", 0, 15000))
        await self._drain_writer()
        result = await self.bridge.send_telemetry_request(
            pubkey,
            want_base=want_base,
            want_location=want_location,
            want_environment=want_environment,
        )
        if not result.get("success"):
            self._write_frame(bytes([PUSH_CODE_TELEMETRY_RESPONSE, 0]) + pubkey[:6])
            return
        # Push PUSH_CODE_TELEMETRY_RESPONSE (0x8B): reserved(1) + pub_key_prefix(6) + raw_lpp_bytes
        # Firmware forwards data[4:] (CayenneLPP bytes) from onContactResponse.
        telem_data = result.get("telemetry_data", {})
        raw_bytes = telem_data.get("raw_bytes", b"")
        if not raw_bytes:
            # No binary telemetry available — send empty telemetry frame.
            # NEVER send formatted text as raw bytes: the companion app
            # interprets the frame as CayenneLPP binary and would display
            # garbled sensor values.
            logger.warning(
                "TELEMETRY_REQ: no raw_bytes in telemetry (type=%s), sending empty frame",
                telem_data.get("type", "unknown"),
            )
            self._write_frame(bytes([PUSH_CODE_TELEMETRY_RESPONSE, 0]) + pubkey[:6])
            return
        self._write_frame(
            bytes([PUSH_CODE_TELEMETRY_RESPONSE, 0])
            + pubkey[:6]
            + raw_bytes
        )

    async def _cmd_send_self_advert(self, data: bytes) -> None:
        flood = len(data) >= 1 and data[0] == 1
        ok = await self.bridge.advertise(flood=flood)
        self._write_ok() if ok else self._write_err(ERR_CODE_BAD_STATE)

    async def _cmd_set_advert_name(self, data: bytes) -> None:
        name = data.decode("utf-8", errors="replace").rstrip("\x00")
        self.bridge.set_advert_name(name)
        self._write_ok()

    async def _cmd_set_advert_latlon(self, data: bytes) -> None:
        if len(data) < 8:
            self._write_err(ERR_CODE_ILLEGAL_ARG)
            return
        lat, lon = struct.unpack_from("<ii", data, 0)
        self.bridge.set_advert_latlon(lat / 1e6, lon / 1e6)
        self._write_ok()

    async def _cmd_add_update_contact(self, data: bytes) -> None:
        # Match meshcore minimum: 36 bytes (pubkey 32 + adv_type 1 + flags 1 + out_path_len 1).
        if len(data) < 36:
            self._write_err(ERR_CODE_ILLEGAL_ARG)
            await self._drain_writer()
            return
        pubkey = data[0:32]
        adv_type = data[32]
        flags = data[33]
        out_path_len = struct.unpack_from("<b", data, 34)[0]  # signed byte, -1 = unknown
        # Safe parsing: only read fields that are present; use defaults to avoid reading past buffer.
        out_path_end = 35 + MAX_PATH_SIZE
        if len(data) >= out_path_end:
            out_path = data[35:out_path_end].rstrip(b"\x00")
        else:
            out_path = data[35:len(data)].rstrip(b"\x00") if len(data) > 35 else b""
        name_start = 35 + MAX_PATH_SIZE
        name_end = name_start + 32
        if len(data) >= name_end:
            name_raw = data[name_start:name_end]
        elif len(data) > name_start:
            name_raw = data[name_start:len(data)].ljust(32, b"\x00")
        else:
            name_raw = b"\x00" * 32
        name = name_raw.split(b"\x00")[0].decode("utf-8", errors="replace")
        last_advert = 0
        if len(data) >= name_end + 4:
            last_advert = struct.unpack_from("<I", data, name_end)[0]
        gps_lat, gps_lon = 0.0, 0.0
        if len(data) >= name_end + 4 + 8:
            gps_lat = struct.unpack_from("<i", data, name_end + 4)[0] / 1e6
            gps_lon = struct.unpack_from("<i", data, name_end + 8)[0] / 1e6
        lastmod = int(time.time())
        if len(data) >= name_end + 4 + 12:
            lastmod = struct.unpack_from("<I", data, name_end + 12)[0]
        contact = Contact(
            public_key=pubkey,
            name=name,
            adv_type=adv_type,
            flags=flags,
            out_path_len=out_path_len,
            out_path=out_path,
            last_advert_timestamp=last_advert,
            lastmod=lastmod,
            gps_lat=gps_lat,
            gps_lon=gps_lon,
        )
        ok = self.bridge.add_update_contact(contact)
        # Send OK/ERR immediately so client gets confirmation (matches meshcore firmware).
        self._write_ok() if ok else self._write_err(ERR_CODE_TABLE_FULL)
        if ok:
            # Echo RESP_CODE_CONTACT so client can add to list without CMD_GET_CONTACTS.
            opl_byte = 0xFF if out_path_len < 0 or out_path_len > 255 else out_path_len
            out_path_padded = (out_path[:MAX_PATH_SIZE] if out_path else b"").ljust(MAX_PATH_SIZE, b"\x00")
            name_padded = (name.encode("utf-8")[:32] if isinstance(name, str) else name[:32]).ljust(32, b"\x00")
            contact_frame = (
                bytes([RESP_CODE_CONTACT])
                + pubkey
                + bytes([adv_type, flags, opl_byte])
                + out_path_padded
                + name_padded
                + struct.pack("<I", last_advert)
                + struct.pack("<i", int(gps_lat * 1e6))
                + struct.pack("<i", int(gps_lon * 1e6))
                + struct.pack("<I", lastmod)
            )
            self._write_frame(contact_frame)
        await self._drain_writer()
        if ok and self.sqlite_handler:
            try:
                self._save_contacts()
            except Exception as e:
                logger.warning("Save contacts after add/update failed: %s", e)

    async def _cmd_remove_contact(self, data: bytes) -> None:
        if len(data) < 32:
            self._write_err(ERR_CODE_ILLEGAL_ARG)
            await self._drain_writer()
            return
        pubkey = data[:32]
        ok = self.bridge.remove_contact(pubkey)
        if ok and self.sqlite_handler:
            self._save_contacts()
        self._write_ok() if ok else self._write_err(ERR_CODE_NOT_FOUND)
        await self._drain_writer()

    async def _cmd_reset_path(self, data: bytes) -> None:
        if len(data) < 32:
            self._write_err(ERR_CODE_ILLEGAL_ARG)
            return
        pubkey = data[:32]
        ok = self.bridge.reset_path(pubkey)
        self._write_ok() if ok else self._write_err(ERR_CODE_NOT_FOUND)

    async def _cmd_get_batt_and_storage(self, data: bytes) -> None:
        millivolts = 0
        used_kb = 0
        total_kb = 0
        frame = bytes([RESP_CODE_BATT_AND_STORAGE]) + struct.pack("<H", millivolts) + struct.pack("<II", used_kb, total_kb)
        self._write_frame(frame)

    async def _cmd_get_stats(self, data: bytes) -> None:
        # CMD_GET_STATS (56): data[0] = stats_type (0=core, 1=radio, 2=packets). Firmware MyMesh.cpp + meshcore_py reader.
        stats_type = data[0] if len(data) >= 1 else STATS_TYPE_PACKETS
        if stats_type not in (STATS_TYPE_CORE, STATS_TYPE_RADIO, STATS_TYPE_PACKETS):
            self._write_err(ERR_CODE_ILLEGAL_ARG)
            return
        stats = (self.stats_getter(stats_type) if self.stats_getter else None) or self.bridge.get_stats(stats_type)
        frame = bytes([RESP_CODE_STATS, stats_type])
        if stats_type == STATS_TYPE_CORE:
            # Format: battery_mv(H) + uptime_secs(I) + errors(H) + queue_len(B) = 9 bytes (meshcore_py <H I H B>)
            battery_mv = int(stats.get("battery_mv", 0))
            uptime_secs = int(stats.get("uptime_secs", 0))
            errors = int(stats.get("errors", 0))
            queue_len = min(255, max(0, int(stats.get("queue_len", 0))))
            frame += struct.pack("<H I H B", battery_mv, uptime_secs, errors, queue_len)
        elif stats_type == STATS_TYPE_RADIO:
            # Format: noise_floor(h) + last_rssi(b) + last_snr(b, SNR*4) + tx_air_secs(I) + rx_air_secs(I) = 12 bytes
            noise_floor = int(stats.get("noise_floor", 0))
            last_rssi = max(-128, min(127, int(stats.get("last_rssi", 0))))
            last_snr_scaled = max(-128, min(127, int(round((stats.get("last_snr") or 0) * 4))))
            tx_air_secs = int(stats.get("tx_air_secs", 0))
            rx_air_secs = int(stats.get("rx_air_secs", 0))
            frame += struct.pack("<h b b I I", noise_floor, last_rssi, last_snr_scaled, tx_air_secs, rx_air_secs)
        else:
            # STATS_TYPE_PACKETS: recv(I)+sent(I)+flood_tx(I)+direct_tx(I)+flood_rx(I)+direct_rx(I)+recv_errors(I) = 28 bytes
            recv = int(stats.get("recv", 0))
            sent = int(stats.get("sent", 0))
            flood_tx = int(stats.get("flood_tx", 0))
            direct_tx = int(stats.get("direct_tx", 0))
            flood_rx = int(stats.get("flood_rx", 0))
            direct_rx = int(stats.get("direct_rx", 0))
            recv_errors = int(stats.get("recv_errors", 0))
            frame += struct.pack("<I I I I I I I", recv, sent, flood_tx, direct_tx, flood_rx, direct_rx, recv_errors)
        self._write_frame(frame)

    async def _cmd_get_advert_path(self, data: bytes) -> None:
        # CMD_GET_ADVERT_PATH (42): reserved(1) + pub_key(32). Return inbound path from advert_paths (path_cache).
        # Firmware: RESP_CODE_ADVERT_PATH(1) + recv_timestamp(4 LE) + path_len(1) + path(path_len)
        if len(data) < 1 + PUB_KEY_SIZE:
            self._write_err(ERR_CODE_ILLEGAL_ARG)
            return
        pub_key = data[1 : 1 + PUB_KEY_SIZE]
        prefix = pub_key[:7]
        found = self.bridge.get_advert_path(prefix) if getattr(self.bridge, "get_advert_path", None) else None
        if not found:
            self._write_err(ERR_CODE_NOT_FOUND)
            return
        path_bytes = getattr(found, "path", None) or b""
        if not isinstance(path_bytes, bytes):
            path_bytes = bytes(path_bytes)
        path_len = min(len(path_bytes), MAX_PATH_SIZE)
        recv_ts = getattr(found, "recv_timestamp", 0)
        frame = bytes([RESP_CODE_ADVERT_PATH]) + struct.pack("<I", recv_ts) + bytes([path_len]) + path_bytes[:path_len]
        self._write_frame(frame)

    async def _cmd_import_contact(self, data: bytes) -> None:
        ok = self.bridge.import_contact(data)
        self._write_ok() if ok else self._write_err(ERR_CODE_ILLEGAL_ARG)

    async def _cmd_get_channel(self, data: bytes) -> None:
        # Payload: channel index (1 byte), or empty for "get full list" (some apps send
        # one request with no payload to receive all channels in one go).
        channel_idx = data[0] if len(data) >= 1 else 0
        get_full_list = len(data) == 0
        max_channels_val = getattr(
            getattr(self.bridge, "channels", None), "max_channels", 40
        )
        logger.debug(
            f"CMD_GET_CHANNEL: idx={channel_idx}, data_len={len(data)}, get_full_list={get_full_list}"
        )

        # Frame format per firmware & meshcore_py: code(1) + channel_idx(1) + name(32) + secret(16)
        def _channel_info_frame(idx: int, ch) -> bytes:
            if ch is None:
                name = b"\x00" * 32
                secret = b"\x00" * 16
            else:
                name = ch.name.encode("utf-8", errors="replace")[:32].ljust(
                    32, b"\x00"
                )
                # Firmware and meshcore_py use 16-byte (128-bit) secret in the frame
                secret = (ch.secret[:16] if ch.secret else b"\x00" * 16).ljust(16, b"\x00")
            return bytes([RESP_CODE_CHANNEL_INFO, idx]) + name + secret

        if get_full_list:
            for idx in range(max_channels_val):
                ch = self.bridge.get_channel(idx)
                frame = _channel_info_frame(idx, ch)
                self._write_frame(frame)
            logger.debug(f"CMD_GET_CHANNEL: sent full list ({max_channels_val} slots)")
            return

        if channel_idx < 0 or channel_idx >= max_channels_val:
            logger.debug(f"CMD_GET_CHANNEL: channel {channel_idx} out of range")
            self._write_err(ERR_CODE_NOT_FOUND)
            return
        ch = self.bridge.get_channel(channel_idx)
        if ch is None:
            logger.debug(f"CMD_GET_CHANNEL: returning empty slot {channel_idx}")
        else:
            logger.debug(f"CMD_GET_CHANNEL: returning {ch.name!r}, secret_len=16")
        frame = _channel_info_frame(channel_idx, ch)
        self._write_frame(frame)

    async def _cmd_set_channel(self, data: bytes) -> None:
        # MeshCore format: channel_idx(1) + name(32) + secret(32) or secret_hex(64)
        logger.debug(f"CMD_SET_CHANNEL: data_len={len(data)}, data_hex={data[:50].hex()}...")
        if len(data) < 34:  # minimum: idx + name(32) + at least 1 byte secret
            logger.debug(f"CMD_SET_CHANNEL: rejected (len {len(data)} < 34)")
            self._write_err(ERR_CODE_ILLEGAL_ARG)
            return
        channel_idx = data[0]
        name_raw = data[1:33]
        name = name_raw.split(b"\x00")[0].decode("utf-8", errors="replace").strip()
        if len(data) >= 97:
            # Hex secret: 64 hex chars = 32 bytes
            try:
                secret = bytes.fromhex(data[33:97].decode("ascii"))
                logger.debug(f"CMD_SET_CHANNEL: parsed hex secret, len={len(secret)}")
            except (ValueError, UnicodeDecodeError) as e:
                logger.debug(f"CMD_SET_CHANNEL: hex secret parse failed: {e}")
                self._write_err(ERR_CODE_ILLEGAL_ARG)
                return
        elif len(data) >= 65:
            # Binary secret: 32 bytes (MeshCore DataStore format)
            secret = data[33:65]
            logger.debug(f"CMD_SET_CHANNEL: parsed 32-byte binary secret")
        elif len(data) >= 49:
            # Legacy: 16-byte binary secret
            secret = data[33:49]
            logger.debug(f"CMD_SET_CHANNEL: parsed 16-byte binary secret")
        else:
            logger.debug(f"CMD_SET_CHANNEL: rejected (len {len(data)} not in 49/65/97)")
            self._write_err(ERR_CODE_ILLEGAL_ARG)
            return
        logger.debug(f"CMD_SET_CHANNEL: idx={channel_idx}, name={name!r}, secret_len={len(secret)}")
        ok = self.bridge.set_channel(channel_idx, name, secret)
        if ok and self.sqlite_handler:
            self._save_channels()
        logger.debug(f"CMD_SET_CHANNEL: set_channel ok={ok}")

        self._write_ok() if ok else self._write_err(ERR_CODE_NOT_FOUND)

    def _save_channels(self) -> None:
        """Persist channels to SQLite."""
        if not self.sqlite_handler:
            return
        channels = []
        max_ch = getattr(
            getattr(self.bridge, "channels", None), "max_channels", 40
        )
        for idx in range(max_ch):
            ch = self.bridge.get_channel(idx)
            if ch is not None:
                channels.append({
                    "channel_idx": idx,
                    "name": ch.name,
                    "secret": ch.secret,
                })
        self.sqlite_handler.companion_save_channels(self.companion_hash, channels)
