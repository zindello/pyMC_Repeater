#!/bin/bash
# Convert MeshCore firmware 64-byte private key to pyMC_Repeater format
#
# Usage: sudo ./convert_firmware_key.sh <64-byte-hex-key> [config-path]
# Example: sudo ./convert_firmware_key.sh 987BDA619630197351F2B3040FD19B2EE0DEE357DD69BBEEE295786FA78A4D5F298B0BF1B7DE73CBC23257CDB2C562F5033DF58C232916432948B0F6BA4448F2

set -e

if [ $# -eq 0 ]; then
    echo "Error: No key provided"
    echo ""
    echo "Usage: sudo $0 <64-byte-hex-key> [config-path]"
    echo ""
    echo "This script imports a 64-byte MeshCore firmware private key into"
    echo "pyMC_Repeater config.yaml for full identity compatibility."
    echo ""
    echo "The 64-byte key format: [32-byte scalar][32-byte nonce]"
    echo "  - Enables same node address as firmware device"
    echo "  - Supports signing using MeshCore/orlp ed25519 algorithm"
    echo "  - Fully compatible with pyMC_core LocalIdentity"
    echo ""
    echo "Arguments:"
    echo "  config-path: Optional path to config.yaml (default: /etc/pymc_repeater/config.yaml)"
    echo ""
    echo "Example:"
    echo "  sudo $0 987BDA619630197351F2B3040FD19B2EE0DEE357DD69BBEEE295786FA78A4D5F298B0BF1B7DE73CBC23257CDB2C562F5033DF58C232916432948B0F6BA4448F2"
    exit 1
fi

# Check if running with sudo/root
if [ "$EUID" -ne 0 ]; then
    echo "Error: This script must be run with sudo to update config.yaml"
    echo "Usage: sudo $0 <64-byte-hex-key>"
    exit 1
fi

FULL_KEY="$1"

# Validate hex string
if ! [[ "$FULL_KEY" =~ ^[0-9a-fA-F]+$ ]]; then
    echo "Error: Key must be a hexadecimal string"
    exit 1
fi

KEY_LEN=${#FULL_KEY}

if [ "$KEY_LEN" -ne 128 ]; then
    echo "Error: Key must be 64 bytes (128 hex characters), got $KEY_LEN characters"
    exit 1
fi

# Get config path
CONFIG_PATH="${2:-/etc/pymc_repeater/config.yaml}"

# Check if config exists
if [ ! -f "$CONFIG_PATH" ]; then
    echo "Error: Config file not found: $CONFIG_PATH"
    exit 1
fi

echo "=== MeshCore Firmware Key Import ==="
echo ""
echo "Input (64-byte firmware key):"
echo "  $FULL_KEY"
echo ""

# Verify public key derivation and import key using Python with safe YAML handling
python3 <<EOF
import sys
import yaml
import base64
import hashlib
from pathlib import Path

# Import the key
key_hex = "$FULL_KEY"
key_bytes = bytes.fromhex(key_hex)

# Verify with pyMC if available
try:
    from nacl.bindings import crypto_scalarmult_ed25519_base_noclamp

    scalar = key_bytes[:32]
    pubkey = crypto_scalarmult_ed25519_base_noclamp(scalar)

    print(f"Derived public key: {pubkey.hex()}")

    # Calculate address (MeshCore uses first byte of pubkey directly, not SHA256)
    address = pubkey[0]
    print(f"Node address: 0x{address:02x}")
    print()

except ImportError:
    print("Warning: PyNaCl not available, skipping verification")
    print()

# Load config
config_path = Path("$CONFIG_PATH")
try:
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f) or {}
except Exception as e:
    print(f"Error loading config: {e}")
    sys.exit(1)

# Check for existing key
if 'mesh' in config and 'identity_key' in config['mesh']:
    existing = config['mesh']['identity_key']
    if isinstance(existing, bytes):
        print(f"WARNING: Existing identity_key found ({len(existing)} bytes)")
    else:
        print(f"WARNING: Existing identity_key found")
    print()

# Ensure mesh section exists
if 'mesh' not in config:
    config['mesh'] = {}

# Store the full 64-byte key
config['mesh']['identity_key'] = key_bytes

# Save config atomically
backup_path = f"{config_path}.backup.{Path(config_path).stat().st_mtime_ns}"
import shutil
shutil.copy2(config_path, backup_path)
print(f"Created backup: {backup_path}")

try:
    with open(config_path, 'w') as f:
        yaml.safe_dump(config, f, default_flow_style=False, allow_unicode=True)
    print(f"✓ Successfully updated {config_path}")
    print()
except Exception as e:
    print(f"Error writing config: {e}")
    shutil.copy2(backup_path, config_path)
    print(f"Restored from backup")
    sys.exit(1)

EOF

if [ $? -ne 0 ]; then
    echo "Error: Python script failed"
    exit 1
fi

# Offer to restart service
if systemctl is-active --quiet pymc-repeater 2>/dev/null; then
    read -p "Restart pymc-repeater service now? (yes/no): " RESTART
    if [ "$RESTART" = "yes" ]; then
        systemctl restart pymc-repeater
        echo "✓ Service restarted"
        echo ""
        echo "Check logs for new identity:"
        echo "  sudo journalctl -u pymc-repeater -f | grep -i 'identity\|hash'"
    else
        echo "Remember to restart the service:"
        echo "  sudo systemctl restart pymc-repeater"
    fi
else
    echo "Note: pymc-repeater service is not running"
    echo "Start it with: sudo systemctl start pymc-repeater"
fi
