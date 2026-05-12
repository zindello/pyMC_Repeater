"""
INA219 current/voltage/power monitor sensor plug-in.

Requires: pip install adafruit-circuitpython-ina219

Config example:
  - type: ina219
    name: "power_monitor"
    enabled: true
        auto_install_packages: false
    settings:
      i2c_address: 0x40  # Default INA219 I2C address
      max_expected_amps: 2.0
      shunt_ohms: 0.1  # 0.1 Ohm shunt resistor
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from .base import SensorBase
from .registry import SensorRegistry


@SensorRegistry.register("ina219")
class INA219Sensor(SensorBase):
    sensor_type = "ina219"

    def __init__(self, name: str, config: Optional[Dict[str, Any]] = None, log=None):
        super().__init__(name=name, config=config, log=log)

        self.i2c_address = self.settings.get("i2c_address", 0x40)
        self.max_expected_amps = float(self.settings.get("max_expected_amps", 2.0))
        self.shunt_ohms = float(self.settings.get("shunt_ohms", 0.1))

        self.available = False
        if not self.ensure_python_modules(
            [
                ("board", "board"),
                ("busio", "adafruit-blinka"),
                ("adafruit_ina219", "adafruit-circuitpython-ina219"),
            ]
        ):
            return

        try:
            import board  # type: ignore[import-not-found]
            import busio  # type: ignore[import-not-found]
            from adafruit_ina219 import Adafruit_INA219  # type: ignore[import-not-found]

            # Create I2C interface
            i2c = busio.I2C(board.SCL, board.SDA)

            # Create sensor object
            self.ina219 = Adafruit_INA219(
                i2c_addr=self.i2c_address,
                i2c=i2c,
            )

            # Configure for expected current range
            self.ina219.set_calibration_32V_2A() if self.max_expected_amps <= 2.0 else None

            self.available = True
            self.log.info(
                "INA219 initialized (addr=0x%02X, shunt=%.3fΩ, max_A=%.1f)",
                self.i2c_address,
                self.shunt_ohms,
                self.max_expected_amps,
            )
        except Exception as exc:
            self.log.warning(
                "INA219 init failed (addr=0x%02X): %s",
                self.i2c_address,
                exc,
            )
            self.available = False

    def _read(self) -> Dict[str, Any]:
        """Read voltage, current, and power from INA219."""
        if not self.available:
            raise RuntimeError("INA219 device not available")
        
        try:
            return {
                "bus_voltage_v": round(self.ina219.bus_voltage, 3),
                "shunt_voltage_v": round(self.ina219.shunt_voltage, 4),
                "current_ma": round(self.ina219.current, 2),
                "power_mw": round(self.ina219.power, 2),
            }
        except Exception as exc:
            raise RuntimeError(f"INA219 read failed: {exc}") from exc
