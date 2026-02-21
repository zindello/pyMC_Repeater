import asyncio
import logging

from pymc_core.node.handlers.ack import AckHandler
from pymc_core.node.handlers.advert import AdvertHandler
from pymc_core.node.handlers.control import ControlHandler
from pymc_core.node.handlers.group_text import GroupTextHandler
from pymc_core.node.handlers.login_response import LoginResponseHandler
from pymc_core.node.handlers.login_server import LoginServerHandler
from pymc_core.node.handlers.path import PathHandler
from pymc_core.node.handlers.protocol_request import ProtocolRequestHandler
from pymc_core.node.handlers.protocol_response import ProtocolResponseHandler
from pymc_core.node.handlers.text import TextMessageHandler
from pymc_core.node.handlers.trace import TraceHandler

logger = logging.getLogger("PacketRouter")


class PacketRouter:

    def __init__(self, daemon_instance):
        self.daemon = daemon_instance
        self.queue = asyncio.Queue()
        self.running = False
        self.router_task = None
        
    async def start(self):
        self.running = True
        self.router_task = asyncio.create_task(self._process_queue())
        logger.info("Packet router started")
    
    async def stop(self):
        self.running = False
        if self.router_task:
            self.router_task.cancel()
            try:
                await self.router_task
            except asyncio.CancelledError:
                pass
        logger.info("Packet router stopped")
    
    async def enqueue(self, packet):
        """Add packet to router queue."""
        await self.queue.put(packet)

    async def inject_packet(self, packet, wait_for_ack: bool = False):
        try:
            metadata = {
                "rssi": getattr(packet, "rssi", 0),
                "snr": getattr(packet, "snr", 0.0),
                "timestamp": getattr(packet, "timestamp", 0),
            }

            # Use local_transmission=True to bypass forwarding logic
            await self.daemon.repeater_handler(packet, metadata, local_transmission=True)

            # Enqueue so router can deliver to companion(s): TXT_MSG -> dest bridge, ACK -> all bridges (sender sees ACK)
            await self.enqueue(packet)

            packet_len = len(packet.payload) if packet.payload else 0
            logger.debug(
                f"Injected packet processed by engine as local transmission ({packet_len} bytes)"
            )
            return True

        except Exception as e:
            logger.error(f"Error injecting packet through engine: {e}")
            return False
    
    async def _process_queue(self):
        while self.running:
            try:
                packet = await asyncio.wait_for(self.queue.get(), timeout=0.1)
                await self._route_packet(packet)
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Router error: {e}", exc_info=True)

    async def _route_packet(self, packet):

        payload_type = packet.get_payload_type()
        processed_by_injection = False

        # Route to specific handlers for parsing only
        if payload_type == TraceHandler.payload_type():
            # Process trace packet
            if self.daemon.trace_helper:
                await self.daemon.trace_helper.process_trace_packet(packet)
                # Skip engine processing for trace packets - they're handled by trace helper
                processed_by_injection = True

        elif payload_type == ControlHandler.payload_type():
            # Process control/discovery packet
            if self.daemon.discovery_helper:
                await self.daemon.discovery_helper.control_handler(packet)
                packet.mark_do_not_retransmit()
            # Deliver to companions via daemon (frame servers push PUSH_CODE_CONTROL_DATA 0x8E)
            deliver = getattr(self.daemon, "deliver_control_data", None)
            if deliver:
                snr = getattr(packet, "_snr", None) or getattr(packet, "snr", 0.0)
                rssi = getattr(packet, "_rssi", None) or getattr(packet, "rssi", 0)
                path_len = getattr(packet, "path_len", 0) or 0
                path_bytes = (
                    bytes(getattr(packet, "path", []))
                    if getattr(packet, "path", None) is not None
                    else b""
                )[:path_len]
                payload_bytes = bytes(packet.payload) if packet.payload else b""
                await deliver(snr, rssi, path_len, path_bytes, payload_bytes)

        elif payload_type == AdvertHandler.payload_type():
            # Process advertisement packet for neighbor tracking
            if self.daemon.advert_helper:
                rssi = getattr(packet, "rssi", 0)
                snr = getattr(packet, "snr", 0.0)
                await self.daemon.advert_helper.process_advert_packet(packet, rssi, snr)
            # Also feed adverts to companion bridges (for contact/path updates)
            for bridge in getattr(self.daemon, "companion_bridges", {}).values():
                try:
                    await bridge.process_received_packet(packet)
                except Exception as e:
                    logger.debug(f"Companion bridge advert error: {e}")

        elif payload_type == LoginServerHandler.payload_type():
            # Route to companion if dest is a companion; else to login_helper (for logging into this repeater).
            # If dest is remote (no local handler), mark processed so we don't pass our own outbound login TX to the repeater as RX.
            dest_hash = packet.payload[0] if packet.payload else None
            companion_bridges = getattr(self.daemon, "companion_bridges", {})
            if dest_hash is not None and dest_hash in companion_bridges:
                await companion_bridges[dest_hash].process_received_packet(packet)
                processed_by_injection = True
            elif self.daemon.login_helper:
                handled = await self.daemon.login_helper.process_login_packet(packet)
                if handled:
                    processed_by_injection = True
                else:
                    # Login request for remote repeater (we already TXed it via inject); don't treat as RX.
                    processed_by_injection = True

        elif payload_type == AckHandler.payload_type():
            # ACK has no dest in payload (4-byte CRC only); deliver to all bridges so sender sees send_confirmed
            companion_bridges = getattr(self.daemon, "companion_bridges", {})
            for bridge in companion_bridges.values():
                try:
                    await bridge.process_received_packet(packet)
                except Exception as e:
                    logger.debug(f"Companion bridge ACK error: {e}")
            processed_by_injection = True

        elif payload_type == TextMessageHandler.payload_type():
            dest_hash = packet.payload[0] if packet.payload else None
            companion_bridges = getattr(self.daemon, "companion_bridges", {})
            if dest_hash is not None and dest_hash in companion_bridges:
                await companion_bridges[dest_hash].process_received_packet(packet)
                processed_by_injection = True
            elif self.daemon.text_helper:
                handled = await self.daemon.text_helper.process_text_packet(packet)
                if handled:
                    processed_by_injection = True

        elif payload_type == PathHandler.payload_type():
            dest_hash = packet.payload[0] if packet.payload else None
            companion_bridges = getattr(self.daemon, "companion_bridges", {})
            if dest_hash is not None and dest_hash in companion_bridges:
                await companion_bridges[dest_hash].process_received_packet(packet)
                processed_by_injection = True
            elif self.daemon.path_helper:
                await self.daemon.path_helper.process_path_packet(packet)

        elif payload_type == LoginResponseHandler.payload_type():
            # PAYLOAD_TYPE_RESPONSE (0x01): login responses from remote repeaters.
            # Deliver to all companion bridges so the bridge that initiated the login receives it.
            companion_bridges = getattr(self.daemon, "companion_bridges", {})
            for bridge in companion_bridges.values():
                try:
                    await bridge.process_received_packet(packet)
                except Exception as e:
                    logger.debug(f"Companion bridge LOGIN_RESPONSE error: {e}")
            if companion_bridges:
                processed_by_injection = True

        elif payload_type == ProtocolResponseHandler.payload_type():
            # PAYLOAD_TYPE_PATH (0x08): protocol responses (telemetry, binary, etc.).
            # Deliver to all companion bridges (response dest_hash is the client, not the bridge).
            companion_bridges = getattr(self.daemon, "companion_bridges", {})
            for bridge in companion_bridges.values():
                try:
                    await bridge.process_received_packet(packet)
                except Exception as e:
                    logger.debug(f"Companion bridge RESPONSE error: {e}")
            if companion_bridges:
                processed_by_injection = True

        elif payload_type == ProtocolRequestHandler.payload_type():
            dest_hash = packet.payload[0] if packet.payload else None
            companion_bridges = getattr(self.daemon, "companion_bridges", {})
            if dest_hash is not None and dest_hash in companion_bridges:
                await companion_bridges[dest_hash].process_received_packet(packet)
                processed_by_injection = True
            elif self.daemon.protocol_request_helper:
                handled = await self.daemon.protocol_request_helper.process_request_packet(packet)
                if handled:
                    processed_by_injection = True

        elif payload_type == GroupTextHandler.payload_type():
            # GRP_TXT: pass to all companions (they filter by channel); still forward
            companion_bridges = getattr(self.daemon, "companion_bridges", {})
            for bridge in companion_bridges.values():
                try:
                    await bridge.process_received_packet(packet)
                except Exception as e:
                    logger.debug(f"Companion bridge GRP_TXT error: {e}")

        # Only pass to repeater engine if not already processed by injection
        if self.daemon.repeater_handler and not processed_by_injection:
            metadata = {
                "rssi": getattr(packet, "rssi", 0),
                "snr": getattr(packet, "snr", 0.0),
                "timestamp": getattr(packet, "timestamp", 0),
            }
            await self.daemon.repeater_handler(packet, metadata)
