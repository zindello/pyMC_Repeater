"""Companion identity support for pyMC Repeater.

Exposes the MeshCore companion frame protocol over TCP for standard clients.
"""

from .frame_server import CompanionFrameServer
from .constants import (
    CMD_APP_START,
    CMD_GET_CONTACTS,
    CMD_SEND_TXT_MSG,
    CMD_SYNC_NEXT_MESSAGE,
    CMD_SEND_LOGIN,
    RESP_CODE_OK,
    RESP_CODE_ERR,
    PUSH_CODE_MSG_WAITING,
)

__all__ = [
    "CompanionFrameServer",
    "CMD_APP_START",
    "CMD_GET_CONTACTS",
    "CMD_SEND_TXT_MSG",
    "CMD_SYNC_NEXT_MESSAGE",
    "CMD_SEND_LOGIN",
    "RESP_CODE_OK",
    "RESP_CODE_ERR",
    "PUSH_CODE_MSG_WAITING",
]
