"""
Service management utilities for pyMC Repeater.
Provides functions for service control operations like restart.
"""

import logging
import subprocess
from typing import Tuple

logger = logging.getLogger("ServiceUtils")


def restart_service() -> Tuple[bool, str]:
    """
    Restart the pymc-repeater service via systemctl.
    
    Uses polkit for authentication (requires proper polkit rules configured).
    NoNewPrivileges systemd flag prevents sudo from working.
    
    Returns:
        Tuple[bool, str]: (success, message)
    """
    try:
        result = subprocess.run(
            ["systemctl", "restart", "pymc-repeater"], capture_output=True, text=True, timeout=5
        )

        if result.returncode == 0:
            logger.info("Service restart command executed successfully")
            return True, "Service restart initiated"
        else:
            error_msg = result.stderr or "Unknown error"
            logger.error(f"Service restart failed: {error_msg}")
            return False, f"Restart failed: {error_msg}"
            
    except subprocess.TimeoutExpired:
        logger.warning("Service restart command timed out (service may be restarting)")
        return True, "Service restart initiated (timeout - likely restarting)"
    except FileNotFoundError:
        logger.error("systemctl not found")
        return False, "systemctl not available"
    except Exception as e:
        logger.error(f"Error executing restart command: {e}")
        return False, f"Restart command failed: {str(e)}"
