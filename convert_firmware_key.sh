#!/bin/bash
# Convert MeshCore firmware 64-byte private key to pyMC_Repeater 32-byte seed format
#
# Usage: sudo ./convert_firmware_key.sh <64-byte-hex-key> [config-path]
# Example: sudo ./convert_firmware_key.sh 681b98730e826d3140f9ac02e94e3de14163418a4a8e069bf8c6fb275bf07e6a7c05b788f5eba746ab0deb613b7502ee316346f7886fbfde56180d3aaaa98033

set -e

if [ $# -eq 0 ]; then
    echo "Error: No key provided"
    echo ""
    echo "Usage: sudo $0 <64-byte-hex-key> [config-path]"
    echo ""
    echo "This script converts a 64-byte MeshCore firmware private key to the"
    echo "32-byte seed format used by pyMC_Repeater and updates config.yaml."
    echo ""
    echo "The 64-byte key is SHA-512(seed), where:"
    echo "  - First 32 bytes: Ed25519 scalar (used for public key derivation)"
    echo "  - Last 32 bytes: Nonce seed (used for signature generation)"
    echo ""
    echo "IMPORTANT: The conversion is ONE-WAY. You cannot recover the original"
    echo "seed from the 64-byte key. This script extracts the scalar portion."
    echo ""
    echo "Arguments:"
    echo "  config-path: Optional path to config.yaml (default: /etc/pymc_repeater/config.yaml)"
    echo ""
    echo "Example:"
    echo "  sudo $0 681b98730e826d3140f9ac02e94e3de14163418a4a8e069bf8c6fb275bf07e6a7c05b788f5eba746ab0deb613b7502ee316346f7886fbfde56180d3aaaa98033"
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

# Extract first 32 bytes (64 hex chars) - this is the Ed25519 scalar
SEED_HEX="${FULL_KEY:0:64}"

# Convert to base64 for Python repeater (using Python since xxd may not be installed)
SEED_BASE64=$(python3 -c "import binascii, base64; print(base64.b64encode(binascii.unhexlify('$SEED_HEX')).decode())")

# Get config path
CONFIG_PATH="${2:-/etc/pymc_repeater/config.yaml}"

# Check if config exists
if [ ! -f "$CONFIG_PATH" ]; then
    echo "Error: Config file not found: $CONFIG_PATH"
    exit 1
fi

echo "=== MeshCore Firmware Key Conversion ==="
echo ""
echo "Input (64-byte firmware key):"
echo "  Full key:    $FULL_KEY"
echo ""
echo "Extracted Ed25519 scalar (first 32 bytes):"
echo "  Hex:         $SEED_HEX"
echo "  Base64:      $SEED_BASE64"
echo ""

# Verify public key derivation
echo "Verifying public key derivation..."
PUBLIC_KEY=$(python3 -c "
from nacl.bindings import crypto_scalarmult_ed25519_base
import binascii
seed = binascii.unhexlify('$SEED_HEX')
pub = crypto_scalarmult_ed25519_base(seed)
print(pub.hex())
" 2>/dev/null)

if [ $? -eq 0 ]; then
    echo "  Public key:  $PUBLIC_KEY"
    PUBLIC_HASH=$(python3 -c "print('0x' + '$PUBLIC_KEY'[:2])")
    echo "  Node hash:   $PUBLIC_HASH"
    echo ""
else
    echo "  Warning: Could not verify public key (PyNaCl not installed)"
    echo ""
fi

# Check if identity_key already exists in config
EXISTING_KEY=$(grep -E "^\s*identity_key:" "$CONFIG_PATH" 2>/dev/null || true)
if [ -n "$EXISTING_KEY" ]; then
    echo "WARNING: An identity_key already exists in config.yaml"
    echo "Current: $EXISTING_KEY"
    echo ""
fi

# Ask for confirmation
echo "Target: $CONFIG_PATH"
read -p "Update config.yaml with this identity key? (yes/no): " CONFIRM
if [ "$CONFIRM" != "yes" ]; then
    echo "Aborted."
    exit 0
fi

# Create backup
BACKUP_PATH="${CONFIG_PATH}.backup.$(date +%Y%m%d_%H%M%S)"
cp "$CONFIG_PATH" "$BACKUP_PATH"
echo "Created backup: $BACKUP_PATH"

# Update config.yaml
# First, try to update existing identity_key (commented or not)
if grep -qE "^\s*#?\s*identity_key:" "$CONFIG_PATH"; then
    # Replace existing identity_key line
    sed -i.tmp "/^\s*#\?\s*identity_key:/c\  identity_key: !!binary |\n    $SEED_BASE64" "$CONFIG_PATH"
    # Clean up any orphaned base64 value lines from old commented key
    sed -i.tmp '/^\s*#\s*[A-Za-z0-9+/=]\{40,\}$/d' "$CONFIG_PATH"
    rm -f "${CONFIG_PATH}.tmp"
    echo "✓ Updated existing identity_key in config.yaml"
else
    # Add identity_key to mesh section
    if grep -q "^mesh:" "$CONFIG_PATH"; then
        # Insert after mesh: line
        sed -i.tmp "/^mesh:/a\  identity_key: !!binary |\n    $SEED_BASE64" "$CONFIG_PATH"
        rm -f "${CONFIG_PATH}.tmp"
        echo "✓ Added identity_key to mesh section in config.yaml"
    else
        echo "Error: Could not find 'mesh:' section in config.yaml"
        echo "Please add manually:"
        echo ""
        echo "mesh:"
        echo "  identity_key: !!binary |"
        echo "    $SEED_BASE64"
        exit 1
    fi
fi

echo ""
echo "✓ Successfully updated config.yaml"
echo ""

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
