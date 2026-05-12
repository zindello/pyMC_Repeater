from repeater.sensors import SensorBase, SensorConfigManager, SensorManager, SensorRegistry


class _DummySensor(SensorBase):
    sensor_type = "dummy"

    def _read(self):
        return {"value": self.settings.get("value", 0)}


class _FailingSensor(SensorBase):
    sensor_type = "failing"

    def _read(self):
        raise RuntimeError("boom")


SensorRegistry.register("dummy", _DummySensor)
SensorRegistry.register("failing", _FailingSensor)


def test_sensor_config_manager_reads_sensor_definitions():
    config = {
        "sensors": {
            "enabled": True,
            "poll_interval_seconds": 12,
            "definitions": [
                {"name": "demo", "type": "dummy", "settings": {"value": 7}},
            ],
        }
    }

    manager = SensorConfigManager(config)

    assert manager.is_enabled() is True
    assert manager.get_poll_interval_seconds() == 12.0
    assert manager.get_definitions()[0]["type"] == "dummy"


def test_sensor_manager_loads_and_reads_sensors_without_stopping_on_failure():
    config = {
        "sensors": {
            "enabled": True,
            "definitions": [
                {"name": "good", "type": "dummy", "settings": {"value": 11}},
                {"name": "bad", "type": "failing"},
                {"name": "skipped", "type": "missing", "enabled": True},
            ],
        }
    }

    manager = SensorManager(config)

    summary = manager.get_summary()
    assert summary["configured"] == 3
    assert summary["loaded"] == 2

    readings = manager.read_all()
    assert len(readings) == 2
    assert readings[0]["ok"] is True
    assert readings[0]["data"]["value"] == 11
    assert readings[1]["ok"] is False
    assert readings[1]["error"].startswith("RuntimeError:")
