#!/usr/bin/env python3
"""
Screen Handler for pyMC Repeater
Supports multiple display types to show live repeater statistics
"""

import logging
import time
import socket
import threading
from abc import ABC, abstractmethod
from collections import deque
from typing import Optional, Dict, Any

logger = logging.getLogger("ScreenHandler")

# Suppress PIL debug logging
logging.getLogger("PIL").setLevel(logging.INFO)


class BaseDisplay(ABC):
    """Abstract base class for display implementations"""
    
    def __init__(self, config: dict):
        self.config = config
        self.width = 0
        self.height = 0
        self.start_time = time.time()
        
    @abstractmethod
    def initialize(self) -> bool:
        """Initialize the display hardware. Returns True on success."""
        pass
    
    @abstractmethod
    def show_splash(self):
        """Display startup splash screen"""
        pass
    
    @abstractmethod
    def update_stats(self, stats: dict):
        """Update display with current statistics"""
        pass
    
    @abstractmethod
    def clear(self):
        """Clear the display"""
        pass
    
    def uptime(self) -> str:
        """Calculate and format uptime"""
        s = int(time.time() - self.start_time)
        h = s // 3600
        m = (s % 3600) // 60
        s = s % 60
        return f"{h:02}:{m:02}:{s:02}"
    
    def get_ip(self) -> str:
        """Get local IP address"""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.connect(("8.8.8.8", 80))
            ip = sock.getsockname()[0]
            sock.close()
            return ip
        except Exception:
            return "offline"


class SSD1327Display(BaseDisplay):
    """OLED Display using SSD1327 controller (128x128 grayscale)"""
    
    def __init__(self, config: dict):
        super().__init__(config)
        self.device = None
        self.ip_addr = None
        
        # Equalizer buffers for noise floor visualization
        self.num_bars = 18
        self.bar_gap = 2  # Increased gap for better visual separation
        self.buffers = None
        
        # Dynamic scaling trackers
        self.noise_min = None
        self.noise_max = None
        
        # Activity indicators
        self.last_tx_count = 0
        self.last_rx_count = 0
        self.tx_activity_time = 0
        self.rx_activity_time = 0
        
    def initialize(self) -> bool:
        """Initialize SSD1327 OLED display"""
        try:
            from luma.core.interface.serial import i2c
            from luma.oled.device import ssd1327
            
            i2c_port = self.config.get("i2c_port", 1)
            i2c_address = self.config.get("i2c_address", 0x3D)
            contrast = self.config.get("contrast", 255)
            
            self.device = ssd1327(i2c(port=i2c_port, address=i2c_address))
            self.device.contrast(contrast)
            
            self.width = self.device.width
            self.height = self.device.height
            
            # Initialize equalizer
            self.buffers = [deque([0]*4, maxlen=4) for _ in range(self.num_bars)]
            self.bar_width = (self.width - (self.num_bars - 1) * self.bar_gap) // self.num_bars
            
            self.ip_addr = self.get_ip()
            
            logger.info(f"SSD1327 OLED display initialized ({self.width}x{self.height})")
            return True
            
        except ImportError:
            logger.error("luma.oled library not installed. Install with: pip install luma.oled")
            return False
        except Exception as e:
            logger.error(f"Failed to initialize SSD1327 display: {e}")
            return False
    
    def show_splash(self):
        """Display startup splash screen with loading bar"""
        if not self.device:
            return
            
        try:
            from luma.core.render import canvas
            
            duration = 3.0
            start = time.time()
            
            bar_width = int(self.width * 0.6)
            bar_height = 6
            bar_x = (self.width - bar_width) // 2
            bar_y = (self.height // 2) + 24
            
            while True:
                elapsed = time.time() - start
                if elapsed > duration:
                    break
                
                progress = min(1.0, elapsed / duration)
                fill_w = int(bar_width * progress)
                
                with canvas(self.device) as draw:
                    draw.rectangle((0, 0, self.width, self.height), fill=0)
                    
                    # Logo - use PIL font for better scaling
                    from PIL import ImageFont
                    try:
                        # Try to use a truetype font for better appearance
                        font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 32)
                    except:
                        # Fallback to larger bitmap font simulation
                        font = None
                    
                    title = "pyMC"
                    
                    if font:
                        # Use truetype font
                        bbox = draw.textbbox((0, 0), title, font=font)
                        text_w = bbox[2] - bbox[0]
                        text_h = bbox[3] - bbox[1]
                        x = (self.width - text_w) // 2
                        y = (self.height // 2) - text_h // 2 - 15
                        draw.text((x, y), title, fill=255, font=font)
                    else:
                        # Manual scaling for cleaner look (4x scale)
                        scale = 4
                        text_w = len(title) * 6 * scale
                        text_h = 8 * scale
                        x = (self.width - text_w) // 2
                        y = (self.height // 2) - text_h // 2 - 10
                        
                        for dx in range(scale):
                            for dy in range(scale):
                                draw.text((x + dx, y + dy), title, fill=255)
                    
                    # Loading bar frame
                    draw.rectangle(
                        (bar_x, bar_y, bar_x + bar_width, bar_y + bar_height),
                        outline=120
                    )
                    
                    # Loading bar fill
                    if fill_w > 0:
                        draw.rectangle(
                            (bar_x, bar_y, bar_x + fill_w, bar_y + bar_height),
                            fill=200
                        )
                
                time.sleep(0.04)
                
        except Exception as e:
            logger.error(f"Failed to show splash screen: {e}")
    
    def update_stats(self, stats: dict):
        """Update display with repeater statistics"""
        if not self.device:
            return
            
        try:
            from luma.core.render import canvas
            
            # Extract stats
            tx_packets = stats.get("packets_sent", 0)
            rx_packets = stats.get("packets_received", 0)
            uptime_str = self.uptime()
            noise_floor_raw = stats.get("noise_floor", None)
            noise_floor_spectrum = stats.get("noise_floor_spectrum", None)
            queue_len = stats.get("queue_len", 0)
            errors = stats.get("errors", 0)
            
            # Normalize noise floor to list format
            if noise_floor_spectrum:
                # Use real spectrum data from database
                noise_floor = list(noise_floor_spectrum)
                # Pad with current value if not enough data
                if len(noise_floor) < self.num_bars:
                    current = noise_floor[-1] if noise_floor else noise_floor_raw
                    noise_floor.extend([current] * (self.num_bars - len(noise_floor)))
            elif noise_floor_raw is None:
                noise_floor = []
            elif isinstance(noise_floor_raw, (int, float)):
                # Single value - replicate across all bars
                noise_floor = [noise_floor_raw] * self.num_bars
            elif isinstance(noise_floor_raw, (list, tuple)):
                noise_floor = list(noise_floor_raw)
            else:
                noise_floor = []
            
            # Layout positions
            title_y = 4
            info_y = 26
            tx_y = 44
            rx_y = 58
            queue_y = 72
            # Noise floor chart at bottom
            eq_height = 20  # Height for equalizer bars section
            eq_bottom = self.height - 2
            eq_top = eq_bottom - eq_height
            divider_y = eq_top - 2
            label_y = divider_y - 27
            
            with canvas(self.device) as draw:
                # Header
                draw.text((4, title_y), "pyMC", fill=255)
                draw.text(
                    (self.width - len(uptime_str) * 6 - 4, title_y),
                    uptime_str,
                    fill=140
                )
                
                # IP Address
                draw.text((4, info_y), f"IP {self.ip_addr}", fill=140)
                
                # Check for activity changes
                current_time = time.time()
                if tx_packets != self.last_tx_count:
                    self.tx_activity_time = current_time
                    self.last_tx_count = tx_packets
                if rx_packets != self.last_rx_count:
                    self.rx_activity_time = current_time
                    self.last_rx_count = rx_packets
                
                # TX Box (left half)
                tx_box_x = 1
                tx_box_y = 40
                tx_box_w = (self.width // 2) - 2
                tx_box_h = 30
                
                # Use TrueType font for labels
                from PIL import ImageFont
                try:
                    label_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 10)
                except:
                    label_font = None
                
                draw.rectangle((tx_box_x, tx_box_y, tx_box_x + tx_box_w, tx_box_y + tx_box_h), outline=200, width=1)
                if label_font:
                    draw.text((tx_box_x + 8, tx_box_y + 3), "TX", font=label_font, fill=200)
                    draw.text((tx_box_x + 8, tx_box_y + 14), str(tx_packets) if tx_packets else "0", font=label_font, fill=255)
                else:
                    draw.text((tx_box_x + 8, tx_box_y + 4), "TX", fill=200)
                    draw.text((tx_box_x + 8, tx_box_y + 14), str(tx_packets) if tx_packets else "0", fill=255)
                
                # TX Activity indicator (flashing dot)
                if current_time - self.tx_activity_time < 1.5:  # Flash for 1.5 seconds
                    # Blink effect: alternate on/off every 0.3 seconds
                    if int((current_time - self.tx_activity_time) * 5) % 2 == 0:
                        indicator_x = tx_box_x + tx_box_w - 8
                        indicator_y = tx_box_y + 6
                        draw.ellipse((indicator_x, indicator_y, indicator_x + 4, indicator_y + 4), fill=255)
                
                # RX Box (right half)
                rx_box_x = (self.width // 2) + 1
                rx_box_y = 40
                rx_box_w = (self.width // 2) - 2
                rx_box_h = 30
                
                draw.rectangle((rx_box_x, rx_box_y, rx_box_x + rx_box_w, rx_box_y + rx_box_h), outline=200, width=1)
                if label_font:
                    draw.text((rx_box_x + 8, rx_box_y + 3), "RX", font=label_font, fill=200)
                    draw.text((rx_box_x + 8, rx_box_y + 14), str(rx_packets) if rx_packets else "0", font=label_font, fill=255)
                else:
                    draw.text((rx_box_x + 8, rx_box_y + 4), "RX", fill=200)
                    draw.text((rx_box_x + 8, rx_box_y + 14), str(rx_packets) if rx_packets else "0", fill=255)
                
                # RX Activity indicator (flashing dot)
                if current_time - self.rx_activity_time < 1.5:  # Flash for 1.5 seconds
                    # Blink effect: alternate on/off every 0.3 seconds
                    if int((current_time - self.rx_activity_time) * 5) % 2 == 0:
                        indicator_x = rx_box_x + rx_box_w - 8
                        indicator_y = rx_box_y + 6
                        draw.ellipse((indicator_x, indicator_y, indicator_x + 4, indicator_y + 4), fill=255)
                
                # Noise floor visualization with actual value
                draw.text((4, label_y), "NOISE", fill=160)
                draw.text((4, label_y + 10), "FLOOR", fill=160)
                # Display current noise floor value (not average of spectrum)
                if noise_floor_raw is not None:
                    from PIL import ImageFont
                    try:
                        # Use larger truetype font for noise floor
                        font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 14)
                        noise_str = f"{noise_floor_raw:.0f}"
                        bbox = draw.textbbox((0, 0), noise_str, font=font)
                        text_w = bbox[2] - bbox[0]
                        x = self.width - text_w - 24
                        y = label_y + 5
                        draw.text((x, y), noise_str, fill=255, font=font)
                        # Draw dBm unit in smaller font next to it
                        draw.text((x + text_w + 2, y + 6), "dBm", fill=200)
                    except:
                        # Fallback if font not available
                        noise_str = f"{noise_floor_raw:.0f} dBm"
                        draw.text((self.width - len(noise_str) * 7 - 8, label_y - 4), noise_str, fill=255)
                draw.line((0, divider_y, self.width, divider_y), fill=90)
                
                # Draw equalizer bars with dynamic scaling - vertical bars across full width
                # Calculate actual data range for better visualization
                valid_data = [n for n in noise_floor if n is not None]
                if valid_data:
                    data_min = min(valid_data)
                    data_max = max(valid_data)
                    
                    # Update tracking for smooth scaling transitions
                    if self.noise_min is None or data_min < self.noise_min:
                        self.noise_min = data_min
                    if self.noise_max is None or data_max > self.noise_max:
                        self.noise_max = data_max
                    
                    # Use actual data range with 2dB padding for better visualization
                    scaling_min = min(data_min - 2, self.noise_min - 2) if self.noise_min else -110
                    scaling_max = max(data_max + 2, self.noise_max + 2) if self.noise_max else -50
                else:
                    # Default scale if no data
                    scaling_min = -110
                    scaling_max = -50
                
                data_range = scaling_max - scaling_min
                if data_range < 1:
                    data_range = 10  # Minimum range to prevent division issues
                
                # Calculate bar width for horizontal arrangement - use full width
                bar_width = self.width / self.num_bars
                
                for i in range(self.num_bars):
                    # Use real noise floor data if available
                    if i < len(noise_floor) and noise_floor[i] is not None:
                        # Convert dBm to visual level using dynamic scaling
                        dbm = noise_floor[i]
                        # Clamp to scaling range
                        normalized = (dbm - scaling_min) / data_range
                        normalized = max(0, min(1, normalized))
                        bar_height = int(normalized * eq_height)
                    else:
                        # Default to middle range if no data
                        bar_height = eq_height // 2
                    
                    self.buffers[i].append(bar_height)
                    level = sum(self.buffers[i]) // len(self.buffers[i])
                    
                    # Calculate horizontal position (left to right across full width)
                    x0 = int(i * bar_width)
                    x1 = int((i + 1) * bar_width)
                    
                    # Calculate vertical position (bar extends up from bottom)
                    y1 = eq_bottom  # Bottom of bar at display bottom
                    y0 = y1 - level  # Top of bar based on height
                    
                    # Dynamic contrast: taller bars get brighter colors (50-255 range)
                    if eq_height > 0:
                        bar_fraction = level / eq_height  # 0 to 1
                        # Map to contrast range: 50 (dim) to 240 (bright)
                        shade = int(50 + (bar_fraction * 190))
                    else:
                        shade = 150
                    
                    # Draw the bar with outline for better definition
                    draw.rectangle((x0, y0, x1, y1), fill=shade)
                    # Add subtle outline for separation
                    draw.rectangle((x0, y0, x1, y1), outline=shade // 2, width=1)
                    
        except Exception as e:
            logger.error(f"Failed to update display: {e}")
    
    def clear(self):
        """Clear the display"""
        if self.device:
            try:
                from luma.core.render import canvas
                with canvas(self.device) as draw:
                    draw.rectangle((0, 0, self.width, self.height), fill=0)
            except Exception as e:
                logger.error(f"Failed to clear display: {e}")


class SSD1306Display(BaseDisplay):
    """OLED Display using SSD1306 controller (128x64 monochrome)"""
    
    def __init__(self, config: dict):
        super().__init__(config)
        self.device = None
        self.ip_addr = None
        
    def initialize(self) -> bool:
        """Initialize SSD1306 OLED display"""
        try:
            from luma.core.interface.serial import i2c
            from luma.oled.device import ssd1306
            
            i2c_port = self.config.get("i2c_port", 1)
            i2c_address = self.config.get("i2c_address", 0x3C)
            
            self.device = ssd1306(i2c(port=i2c_port, address=i2c_address))
            self.width = self.device.width
            self.height = self.device.height
            
            self.ip_addr = self.get_ip()
            
            logger.info(f"SSD1306 OLED display initialized ({self.width}x{self.height})")
            return True
            
        except ImportError:
            logger.error("luma.oled library not installed. Install with: pip install luma.oled")
            return False
        except Exception as e:
            logger.error(f"Failed to initialize SSD1306 display: {e}")
            return False
    
    def show_splash(self):
        """Display startup splash screen"""
        if not self.device:
            return
            
        try:
            from luma.core.render import canvas
            
            duration = 2.0
            start = time.time()
            
            while time.time() - start < duration:
                with canvas(self.device) as draw:
                    draw.rectangle((0, 0, self.width, self.height), fill=0)
                    
                    # Centered title
                    title = "pyMC Repeater"
                    text_w = len(title) * 6
                    x = (self.width - text_w) // 2
                    y = self.height // 2 - 8
                    draw.text((x, y), title, fill=255)
                    
                time.sleep(0.1)
                
        except Exception as e:
            logger.error(f"Failed to show splash screen: {e}")
    
    def update_stats(self, stats: dict):
        """Update display with repeater statistics (compact layout for 64px height)"""
        if not self.device:
            return
            
        try:
            from luma.core.render import canvas
            
            tx_packets = stats.get("packets_sent", 0)
            rx_packets = stats.get("packets_received", 0)
            uptime_str = self.uptime()
            queue_len = stats.get("queue_len", 0)
            
            with canvas(self.device) as draw:
                # Compact layout for smaller display
                draw.text((2, 2), "pyMC", fill=255)
                draw.text((self.width - len(uptime_str) * 6 - 2, 2), uptime_str, fill=255)
                
                draw.line((0, 14, self.width, 14), fill=255)
                
                draw.text((2, 18), f"TX: {tx_packets}", fill=255)
                draw.text((2, 30), f"RX: {rx_packets}", fill=255)
                draw.text((2, 42), f"Q: {queue_len}", fill=255)
                
                draw.text((2, 54), f"IP: {self.ip_addr}", fill=255)
                
        except Exception as e:
            logger.error(f"Failed to update display: {e}")
    
    def clear(self):
        """Clear the display"""
        if self.device:
            try:
                from luma.core.render import canvas
                with canvas(self.device) as draw:
                    draw.rectangle((0, 0, self.width, self.height), fill=0)
            except Exception as e:
                logger.error(f"Failed to clear display: {e}")


class ScreenHandler:
    """Main screen handler that manages display updates"""
    
    def __init__(self, config: dict, stats_provider=None):
        self.config = config.get("display", {})
        self.enabled = self.config.get("enabled", False)
        self.stats_provider = stats_provider
        self.display: Optional[BaseDisplay] = None
        self.update_thread = None
        self.running = False
        
        if self.enabled:
            self._initialize_display()
    
    def _initialize_display(self):
        """Initialize the configured display type"""
        display_type = self.config.get("type", "ssd1327").lower()
        
        display_classes = {
            "ssd1327": SSD1327Display,
            "ssd1306": SSD1306Display,
        }
        
        display_class = display_classes.get(display_type)
        if not display_class:
            logger.error(f"Unknown display type: {display_type}")
            logger.info(f"Available types: {', '.join(display_classes.keys())}")
            return
        
        self.display = display_class(self.config)
        
        if self.display.initialize():
            self.display.show_splash()
            self._start_update_thread()
        else:
            logger.warning("Display initialization failed, screen updates disabled")
            self.display = None
    
    def _start_update_thread(self):
        """Start background thread for display updates"""
        self.running = True
        self.update_thread = threading.Thread(target=self._update_loop, daemon=True)
        self.update_thread.start()
        logger.info("Screen update thread started")
    
    def _update_loop(self):
        """Background loop to update display periodically"""
        # Use faster update interval for smoother animations
        update_interval = self.config.get("update_interval", 0.5)
        
        while self.running:
            try:
                if self.stats_provider:
                    stats = self.stats_provider()
                    if self.display and stats:
                        self.display.update_stats(stats)
                
                time.sleep(update_interval)
                
            except Exception as e:
                logger.error(f"Error in display update loop: {e}")
                time.sleep(5)  # Back off on error
    
    def stop(self):
        """Stop the screen handler"""
        self.running = False
        if self.update_thread:
            self.update_thread.join(timeout=2)
        if self.display:
            self.display.clear()
        logger.info("Screen handler stopped")
