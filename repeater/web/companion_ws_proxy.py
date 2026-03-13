"""
WebSocket proxy for the companion frame protocol.

Bridges browser WebSocket to the companion TCP frame server.
Raw byte pipe — no parsing, all protocol logic lives in the client.
"""

import logging
import socket
import threading
from urllib.parse import parse_qs

import cherrypy
from ws4py.websocket import WebSocket

logger = logging.getLogger("CompanionWSProxy")

# Set by http_server.py before CherryPy starts
_daemon = None


def set_daemon(instance):
    global _daemon
    _daemon = instance


class CompanionFrameWebSocket(WebSocket):

    def opened(self):
        """Authenticate, resolve companion, open TCP socket, start reader."""
        # JWT auth — same pattern as PacketWebSocket
        jwt_handler = cherrypy.config.get("jwt_handler")

        qs = ""
        if hasattr(self, "environ"):
            qs = self.environ.get("QUERY_STRING", "")

        params = parse_qs(qs)
        token = params.get("token", [None])[0]
        companion_name = params.get("companion_name", [None])[0]

        if not jwt_handler:
            logger.warning("Connection rejected: no JWT handler configured")
            self.close(code=1011, reason="server configuration error")
            return

        if not token:
            logger.warning("Connection rejected: missing token")
            self.close(code=1008, reason="unauthorized")
            return

        try:
            payload = jwt_handler.verify_jwt(token)
            if not payload:
                logger.warning("Connection rejected: invalid token")
                self.close(code=1008, reason="unauthorized")
                return
        except Exception as e:
            logger.warning(f"Auth error: {e}")
            self.close(code=1008, reason="unauthorized")
            return

        if not companion_name:
            logger.warning("Connection rejected: missing companion_name")
            self.close(code=1008, reason="missing companion_name")
            return

        # Resolve companion TCP port from config
        tcp_port = self._resolve_tcp_port(companion_name)
        if tcp_port is None:
            logger.warning(f"Connection rejected: companion '{companion_name}' not found")
            self.close(code=1008, reason="companion not found")
            return

        # Open TCP socket to the companion frame server
        try:
            self._tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._tcp.settimeout(5.0)
            self._tcp.connect(("127.0.0.1", tcp_port))
            self._tcp.settimeout(None)
        except Exception as e:
            logger.error(f"TCP connect failed for '{companion_name}' port {tcp_port}: {e}")
            self._tcp = None
            self.close(code=1011, reason="TCP connect failed")
            return

        self._closing = False
        self._reader = threading.Thread(target=self._tcp_to_ws, daemon=True)
        self._reader.start()

        user = payload.get("sub", "unknown")
        logger.info(f"Companion WS opened: user={user}, companion={companion_name}, port={tcp_port}")

    def received_message(self, message):
        """WS → TCP"""
        tcp = getattr(self, "_tcp", None)
        if tcp is None or getattr(self, "_closing", True):
            return
        try:
            data = message.data
            if isinstance(data, str):
                data = data.encode("latin-1")
            tcp.sendall(data)
        except Exception:
            self._teardown()

    def closed(self, code, reason=None):
        logger.info(f"Companion WS closed (code={code})")
        self._teardown()

    # ── internal ─────────────────────────────────────────────────────

    def _resolve_tcp_port(self, companion_name):
        """Look up companion TCP port from daemon config. Returns port or None."""
        if not _daemon:
            return None

        # Verify the companion is actually registered
        identity_manager = getattr(_daemon, "identity_manager", None)
        bridges = getattr(_daemon, "companion_bridges", {})
        if not identity_manager or not bridges:
            return None

        found = False
        for name, identity, _cfg in identity_manager.get_identities_by_type("companion"):
            if name == companion_name:
                h = identity.get_public_key()[0]
                if h in bridges:
                    found = True
                break

        if not found:
            return None

        companions = _daemon.config.get("identities", {}).get("companions") or []
        for entry in companions:
            if entry.get("name") == companion_name:
                return (entry.get("settings") or {}).get("tcp_port", 5000)

        return None

    def _tcp_to_ws(self):
        """TCP → WS reader loop"""
        tcp = getattr(self, "_tcp", None)
        if tcp is None:
            return
        try:
            while not getattr(self, "_closing", True):
                data = tcp.recv(4096)
                if not data:
                    break
                try:
                    self.send(data, binary=True)
                except Exception:
                    break
        except Exception:
            pass
        finally:
            self._teardown()

    def _teardown(self):
        if getattr(self, "_closing", True):
            return
        self._closing = True

        tcp = getattr(self, "_tcp", None)
        if tcp:
            try:
                tcp.close()
            except Exception:
                pass
            self._tcp = None

        try:
            self.close()
        except Exception:
            pass
