import json
import logging
import os
import re
import secrets
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional

import cherrypy
import cherrypy_cors
from pymc_core.protocol.utils import PAYLOAD_TYPES, ROUTE_TYPES

from repeater import __version__
from repeater.data_acquisition import SQLiteHandler
from .api_endpoints import APIEndpoints
from .auth_endpoints import AuthEndpoints
from .auth.jwt_handler import JWTHandler
from .auth.api_tokens import APITokenManager
from .auth import cherrypy_tool  # Import to register the tool

logger = logging.getLogger("HTTPServer")


# In-memory log buffer
class LogBuffer(logging.Handler):

    def __init__(self, max_lines=100):
        super().__init__()
        self.logs = deque(maxlen=max_lines)
        self.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))

    def emit(self, record):

        try:
            msg = self.format(record)
            self.logs.append(
                {
                    "message": msg,
                    "timestamp": datetime.fromtimestamp(record.created).isoformat(),
                    "level": record.levelname,
                }
            )
        except Exception:
            self.handleError(record)


# Global log buffer instance
_log_buffer = LogBuffer(max_lines=100)


class DocEndpoint:
    """Simple wrapper to serve API docs at /doc"""
    
    def __init__(self, api_endpoints):
        self.api_endpoints = api_endpoints
    
    @cherrypy.expose
    def index(self, **kwargs):
        """Serve Swagger UI at /doc"""
        return self.api_endpoints.docs()
    
    @cherrypy.expose
    def docs(self):
        """Serve Swagger UI at /doc/docs"""
        return self.api_endpoints.docs()
    
    @cherrypy.expose
    def openapi_json(self):
        """Serve OpenAPI spec in JSON format at /doc/openapi.json"""
        import os
        import yaml
        import json
        
        spec_path = os.path.join(os.path.dirname(__file__), 'openapi.yaml')
        try:
            with open(spec_path, 'r') as f:
                spec_content = yaml.safe_load(f)
            
            cherrypy.response.headers['Content-Type'] = 'application/json'
            return json.dumps(spec_content).encode('utf-8')
        except FileNotFoundError:
            cherrypy.response.status = 404
            return json.dumps({"error": "OpenAPI spec not found"}).encode('utf-8')
        except Exception as e:
            cherrypy.response.status = 500
            return json.dumps({"error": f"Error loading OpenAPI spec: {e}"}).encode('utf-8')


class StatsApp:

    def __init__(
        self,
        stats_getter: Optional[Callable] = None,
        node_name: str = "Repeater",
        pub_key: str = "",
        send_advert_func: Optional[Callable] = None,
        config: Optional[dict] = None,
        event_loop=None,
        daemon_instance=None,
        config_path=None,
    ):

        self.stats_getter = stats_getter
        self.node_name = node_name
        self.pub_key = pub_key
        self.dashboard_template = None
        self.config = config or {}
        
        # Path to the compiled Vue.js application
        # Use web_path from config if provided, otherwise use default
        default_html_dir = os.path.join(os.path.dirname(__file__), "html")
        web_path = self.config.get("web", {}).get("web_path")
        self.html_dir = web_path if web_path is not None else default_html_dir

        # Create nested API object for routing
        self.api = APIEndpoints(stats_getter, send_advert_func, self.config, event_loop, daemon_instance, config_path)
        
        # Create doc endpoint for API documentation
        self.doc = DocEndpoint(self.api)

    @cherrypy.expose
    def index(self, **kwargs):
        """Serve the Vue.js application index.html."""
        index_path = os.path.join(self.html_dir, "index.html")
        try:
            with open(index_path, 'r', encoding='utf-8') as f:
                return f.read()
        except FileNotFoundError:
            raise cherrypy.HTTPError(404, "Application not found. Please build the frontend first.")
        except Exception as e:
            logger.error(f"Error serving index.html: {e}")
            raise cherrypy.HTTPError(500, "Internal server error")

    @cherrypy.expose
    def default(self, *args, **kwargs):
        """Handle client-side routing - serve index.html for all non-API routes."""
        # Handle OPTIONS requests for any path
        if cherrypy.request.method == "OPTIONS":
            return ""
        
        # Let API routes pass through
        if args and args[0] == 'api':
            raise cherrypy.NotFound()
        
        # For all other routes, serve the Vue.js app (client-side routing)
        return self.index()


class HTTPStatsServer:

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 8000,
        stats_getter: Optional[Callable] = None,
        node_name: str = "Repeater",
        pub_key: str = "",
        send_advert_func: Optional[Callable] = None,
        config: Optional[dict] = None,
        event_loop=None,
        daemon_instance=None,
        config_path=None,
    ):

        self.host = host
        self.port = port
        self.config = config or {}
        self.config_path = config_path
        
        # Initialize authentication handlers
        self._init_auth_handlers()
        
        self.app = StatsApp(
            stats_getter, node_name, pub_key, send_advert_func, config, event_loop, daemon_instance, config_path
        )
        
        # Create auth endpoints (APIEndpoints has the config_manager)
        self.auth_app = AuthEndpoints(self.config, self.jwt_handler, self.token_manager, self.app.api.config_manager)
        
        # Create documentation endpoints as separate app
        self.doc_app = DocEndpoint(self.app.api)
        
        # Set up CORS at the server level if enabled
        self._cors_enabled = self.config.get("web", {}).get("cors_enabled", False)
        logger.info(f"CORS enabled: {self._cors_enabled}")

    def _init_auth_handlers(self):
        """Initialize JWT handler and API token manager."""
        # Get or generate JWT secret from repeater.security
        repeater_config = self.config.get("repeater", {})
        security_config = repeater_config.get("security", {})
        jwt_secret = security_config.get("jwt_secret", "")
        
        if not jwt_secret:
            # Auto-generate JWT secret
            jwt_secret = secrets.token_hex(32)
            logger.warning("No JWT secret found in config, auto-generated one. Please save this to config.yaml:")
            
            # Try to save to config if config_path is available
            if self.config_path:
                try:
                    import yaml
                    with open(self.config_path, 'r') as f:
                        config_data = yaml.safe_load(f) or {}
                    
                    if 'repeater' not in config_data:
                        config_data['repeater'] = {}
                    if 'security' not in config_data['repeater']:
                        config_data['repeater']['security'] = {}
                    config_data['repeater']['security']['jwt_secret'] = jwt_secret
                    
                    with open(self.config_path, 'w') as f:
                        yaml.dump(config_data, f, default_flow_style=False)
                    
                    logger.info(f"Saved auto-generated JWT secret to {self.config_path}")
                except Exception as e:
                    logger.error(f"Failed to save JWT secret to config: {e}")
        
        # Initialize JWT handler (15 minute expiry)
        self.jwt_handler = JWTHandler(jwt_secret, expiry_minutes=15)
        logger.info("JWT handler initialized")
        
        # Initialize API token manager
        storage_dir = self.config.get("storage", {}).get("storage_dir", ".")
        
        # Ensure storage directory exists
        os.makedirs(storage_dir, exist_ok=True)
        
        # Initialize SQLiteHandler and APITokenManager
        self.sqlite_handler = SQLiteHandler(Path(storage_dir))
        self.token_manager = APITokenManager(self.sqlite_handler, jwt_secret)
        logger.info(f"API token manager initialized with database at {storage_dir}/repeater.db")

    def _setup_server_cors(self):
        """Set up CORS using cherrypy_cors.install()"""
        # Configure CORS to allow Authorization header
        # cherrypy-cors will handle preflight requests automatically
        cherrypy_cors.install()
        
        logger.info("CORS support enabled with Authorization header")
    
    def _json_error_handler(self, status, message, traceback, version):
        """Return JSON error responses instead of HTML for API endpoints"""
        cherrypy.response.headers["Content-Type"] = "application/json"
        return json.dumps({
            "success": False,
            "error": message
        })

    def start(self):

        try:
   
            if self._cors_enabled:
                self._setup_server_cors()
            

            default_html_dir = os.path.join(os.path.dirname(__file__), "html")
            web_path = self.config.get("web", {}).get("web_path")
            html_dir = web_path if web_path is not None else default_html_dir
            
            assets_dir = os.path.join(html_dir, "assets")
            next_dir = os.path.join(html_dir, "_next")

            # Build config with conditional CORS settings
            config = {
                "/": {
                    "tools.sessions.on": False,
                    # Ensure proper content types for static files
                    "tools.staticfile.content_types": {
                        'js': 'application/javascript',
                        'css': 'text/css',
                        'html': 'text/html; charset=utf-8',
                        'svg': 'image/svg+xml',
                        'txt': 'text/plain'
                    },
                },
                # Require authentication for all /api endpoints
                "/api": {
                    "tools.require_auth.on": True,
                },
                # Public documentation endpoints (no auth required)
                "/api/openapi": {
                    "tools.require_auth.on": False,
                },
                "/api/docs": {
                    "tools.require_auth.on": False,
                },
                # Public setup wizard endpoints (no auth required)
                "/api/needs_setup": {
                    "tools.require_auth.on": False,
                },
                "/api/hardware_options": {
                    "tools.require_auth.on": False,
                },
                "/api/radio_presets": {
                    "tools.require_auth.on": False,
                },
                "/api/setup_wizard": {
                    "tools.require_auth.on": False,
                },
                "/favicon.ico": {
                    "tools.staticfile.on": True,
                    "tools.staticfile.filename": os.path.join(html_dir, "favicon.ico"),
                },
            }
            
            # Add CORS configuration if enabled
            if self._cors_enabled:
                cors_config = {
                    "cors.expose.on": True,
                    "tools.response_headers.on": True,
                    "tools.response_headers.headers": [
                        ('Access-Control-Allow-Origin', '*'),
                        ('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS'),
                        ('Access-Control-Allow-Headers', 'Authorization, Content-Type, X-API-Key'),
                        ('Access-Control-Allow-Credentials', 'true'),
                    ],
                    # Disable automatic trailing slash redirects to prevent CORS issues
                    "tools.trailing_slash.on": False,
                }
                
                # Apply CORS to paths
                config["/"].update(cors_config)
                config["/api"].update(cors_config)
            
            # Add Vue.js assets support only if assets directory exists
            if os.path.isdir(assets_dir):
                config["/assets"] = {
                    "tools.staticdir.on": True,
                    "tools.staticdir.dir": assets_dir,
                    # Set proper content types for assets
                    "tools.staticdir.content_types": {
                        'js': 'application/javascript',
                        'css': 'text/css',
                        'map': 'application/json'
                    },
                }
            
            # Add Next.js support only if _next directory exists
            if os.path.isdir(next_dir):
                config["/_next"] = {
                    "tools.staticdir.on": True,
                    "tools.staticdir.dir": next_dir,
                    # Set proper content types for Next.js assets
                    "tools.staticdir.content_types": {
                        'js': 'application/javascript',
                        'css': 'text/css',
                        'map': 'application/json'
                    },
                }

            # Only add CORS to static assets if CORS is enabled
            if self._cors_enabled:
                if "/assets" in config:
                    config["/assets"]["cors.expose.on"] = True
                if "/_next" in config:
                    config["/_next"]["cors.expose.on"] = True
                config["/favicon.ico"]["cors.expose.on"] = True

            cherrypy.config.update(
                {
                    "server.socket_host": self.host,
                    "server.socket_port": self.port,
                    "engine.autoreload.on": False,
                    "log.screen": False,
                    "log.access_file": "",  # Disable access log file
                    "log.error_file": "",  # Disable error log file
                    # Disable automatic trailing slash redirects globally
                    "tools.trailing_slash.on": False,
                    # Custom error handler to return JSON for API endpoints
                    "error_page.401": self._json_error_handler,
                    # Add auth handlers to config so they're accessible in endpoints
                    "jwt_handler": self.jwt_handler,
                    "token_manager": self.token_manager,
                }
            )

            # Mount main app
            cherrypy.tree.mount(self.app, "/", config)
            
            # Mount auth endpoints
            auth_config = {
                "/": {
                    "tools.response_headers.on": True,
                    "tools.response_headers.headers": [
                        ('Content-Type', 'application/json'),
                    ],
                    # Disable automatic trailing slash redirects
                    "tools.trailing_slash.on": False,
                }
            }
            if self._cors_enabled:
                auth_config["/"]["cors.expose.on"] = True
                # Add CORS headers for OPTIONS requests
                auth_config["/"]["tools.response_headers.headers"].extend([
                    ('Access-Control-Allow-Origin', '*'),
                    ('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS'),
                    ('Access-Control-Allow-Headers', 'Authorization, Content-Type, X-API-Key'),
                    ('Access-Control-Allow-Credentials', 'true'),
                ])
            
            cherrypy.tree.mount(self.auth_app, "/auth", auth_config)
            
            # Mount documentation endpoints as separate app (no auth required for docs)
            doc_config = {
                "/": {
                    "tools.require_auth.on": False,  # Docs are publicly accessible
                    "tools.response_headers.on": True,
                    "tools.response_headers.headers": [
                        ('Content-Type', 'text/html; charset=utf-8'),
                    ],
                    "tools.trailing_slash.on": False,
                }
            }
            if self._cors_enabled:
                doc_config["/"]["cors.expose.on"] = True
                doc_config["/"]["tools.response_headers.headers"].extend([
                    ('Access-Control-Allow-Origin', '*'),
                    ('Access-Control-Allow-Methods', 'GET, POST, OPTIONS'),
                    ('Access-Control-Allow-Headers', 'Authorization, Content-Type, X-API-Key'),
                ])
            
            cherrypy.tree.mount(self.doc_app, "/doc", doc_config)
            
            # Store auth handlers in cherrypy config for middleware access
            cherrypy.config.update({
                "jwt_handler": self.jwt_handler,
                "token_manager": self.token_manager,
                "security_config": self.config.get("security", {}),
            })

            # Completely disable access logging
            cherrypy.log.access_log.propagate = False
            cherrypy.log.error_log.setLevel(logging.ERROR)

            cherrypy.engine.start()
            server_url = "http://{}:{}".format(self.host, self.port)
            logger.info(f"HTTP stats server started on {server_url}")

        except Exception as e:
            logger.error(f"Failed to start HTTP server: {e}")
            raise

    def stop(self):
        try:
            cherrypy.engine.exit()
            logger.info("HTTP stats server stopped")
        except Exception as e:
            logger.warning(f"Error stopping HTTP server: {e}")
