# Display Support for pyMC Repeater

The pyMC Repeater supports real-time statistics display on OLED screens. This guide covers installation, configuration, and supported hardware.

## Supported Displays

### SSD1327 (128x128 Grayscale OLED)
- **Resolution:** 128x128 pixels
- **Colors:** 16 levels of gray
- **Default I2C Address:** 0x3D
- **Features:** Enhanced visualization with noise floor equalizer
- **Recommended for:** Best visual experience

### SSD1306 (128x64 Monochrome OLED)
- **Resolution:** 128x64 pixels
- **Colors:** Monochrome (black/white)
- **Default I2C Address:** 0x3C
- **Features:** Compact stats display
- **Recommended for:** Budget setups, smaller screens

## Installation

### 1. Install Display Dependencies

Install the optional display package:

```bash
pip install -e ".[display]"
```

Or install manually:

```bash
pip install luma.oled pillow
```

### 2. Enable I2C on Raspberry Pi

```bash
sudo raspi-config
# Navigate to: Interface Options -> I2C -> Enable
```

Reboot after enabling I2C:

```bash
sudo reboot
```

### 3. Verify I2C Connection

Check if your display is detected:

```bash
sudo i2cdetect -y 1
```

You should see your device address (commonly `3C` or `3D`):

```
     0  1  2  3  4  5  6  7  8  9  a  b  c  d  e  f
00:          -- -- -- -- -- -- -- -- -- -- -- -- -- 
10: -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- 
20: -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- 
30: -- -- -- -- -- -- -- -- -- -- -- -- 3d -- -- -- 
40: -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- 
50: -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- 
60: -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- 
70: -- -- -- -- -- -- -- --
```

## Configuration

Edit your `config.yaml` file:

### Basic Configuration (SSD1327)

```yaml
display:
  enabled: true
  type: "ssd1327"
  i2c_port: 1
  i2c_address: 0x3D
  contrast: 255
  update_interval: 1.0
```

### Alternative Display (SSD1306)

```yaml
display:
  enabled: true
  type: "ssd1306"
  i2c_port: 1
  i2c_address: 0x3C
  update_interval: 1.0
```

### Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `enabled` | boolean | `false` | Enable/disable display output |
| `type` | string | `"ssd1327"` | Display type: `ssd1327` or `ssd1306` |
| `i2c_port` | integer | `1` | I2C bus number (usually 1 on Raspberry Pi) |
| `i2c_address` | hex | `0x3D` | I2C device address (`0x3D` for SSD1327, `0x3C` for SSD1306) |
| `contrast` | integer | `255` | Display contrast (0-255, SSD1327 only) |
| `update_interval` | float | `1.0` | Screen refresh interval in seconds |

## Display Layout

### SSD1327 (128x128)

```
┌────────────────────────────┐
│ pyMC            00:05:32   │
├────────────────────────────┤
│ IP 192.168.1.100           │
│                            │
│ TX  1234                   │
│ RX  5678                   │
│ Q   3                      │
│                            │
│ NOISE FLOOR                │
├────────────────────────────┤
│ ▂▃▅▇█▇▅▃▂▃▅▇█▇▅▃▂▃        │ <- Animated equalizer
└────────────────────────────┘
```

### SSD1306 (128x64)

```
┌────────────────────────────┐
│ pyMC            00:05:32   │
├────────────────────────────┤
│ TX: 1234                   │
│ RX: 5678                   │
│ Q: 3                       │
│ IP: 192.168.1.100          │
└────────────────────────────┘
```

## Displayed Statistics

- **Uptime:** Time since repeater started (HH:MM:SS)
- **IP Address:** Current network IP address
- **TX:** Total packets transmitted
- **RX:** Total packets received
- **Q:** Current queue length (pending packets)
- **E:** Error count (if > 0)
- **Noise Floor:** Visual representation of RF noise (SSD1327 only)

## Hardware Connections

### Typical OLED Wiring (Raspberry Pi)

| OLED Pin | RPi Pin | Description |
|----------|---------|-------------|
| VCC | Pin 1 (3.3V) | Power supply |
| GND | Pin 6 (GND) | Ground |
| SDA | Pin 3 (GPIO 2) | I2C Data |
| SCL | Pin 5 (GPIO 3) | I2C Clock |

**Important:** Always use 3.3V, not 5V, for most OLED displays to avoid damage.

## Troubleshooting

### Display Not Detected

```bash
# Check I2C is enabled
lsmod | grep i2c

# Scan for devices
sudo i2cdetect -y 1

# Check permissions
sudo usermod -a -G i2c $USER
# Log out and back in
```

### Display Shows Random Pixels

- Check I2C address matches your hardware
- Try different contrast values (SSD1327)
- Verify wiring connections
- Check for I2C conflicts with other devices

### Import Errors

```bash
# Reinstall display dependencies
pip install --upgrade luma.oled pillow

# Check installation
python3 -c "from luma.oled.device import ssd1327; print('OK')"
```

### Performance Issues

- Increase `update_interval` (e.g., 2.0 or 3.0 seconds)
- Check CPU usage: `top` or `htop`
- Verify I2C bus speed in `/boot/config.txt`

## References

- [Luma.OLED Documentation](https://luma-oled.readthedocs.io/)
- [Raspberry Pi I2C Configuration](https://learn.adafruit.com/adafruits-raspberry-pi-lesson-4-gpio-setup/configuring-i2c)
- [SSD1327 Datasheet](https://cdn-shop.adafruit.com/datasheets/SSD1327.pdf)
- [SSD1306 Datasheet](https://cdn-shop.adafruit.com/datasheets/SSD1306.pdf)

## Common Hardware Sources

- **Waveshare 1.5inch OLED Display Module** (SSD1327, 128x128)
- **Adafruit Monochrome OLED** (SSD1306, 128x64)
- **Generic I2C OLED modules** from various suppliers

Always verify the display type and I2C address before purchasing!
