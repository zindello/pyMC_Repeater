import time
import importlib.util
from pathlib import Path

_MODULE_PATH = Path(__file__).resolve().parents[1] / "repeater" / "data_acquisition" / "gps_service.py"
_SPEC = importlib.util.spec_from_file_location("repeater_gps_service", _MODULE_PATH)
_MODULE = importlib.util.module_from_spec(_SPEC)
assert _SPEC and _SPEC.loader
_SPEC.loader.exec_module(_MODULE)
GPSService = _MODULE.GPSService
NMEAParser = _MODULE.NMEAParser


def _sentence(payload: str) -> str:
    checksum = 0
    for char in payload:
        checksum ^= ord(char)
    return f"${payload}*{checksum:02X}"


def test_nmea_parser_combines_rmc_gga_gsa_gsv_attributes():
    parser = NMEAParser()

    assert parser.ingest_sentence(
        _sentence("GPRMC,123519,A,4807.038,N,01131.000,E,022.4,084.4,230394,003.1,W")
    )
    assert parser.ingest_sentence(
        _sentence("GPGGA,123519,4807.038,N,01131.000,E,1,08,0.9,545.4,M,46.9,M,,")
    )
    assert parser.ingest_sentence(
        _sentence("GPGSA,A,3,04,05,09,12,24,25,29,,,,,,1.8,1.0,1.5")
    )
    assert parser.ingest_sentence(
        _sentence("GPGSV,1,1,03,04,77,045,42,05,13,180,35,09,07,095,29")
    )

    snapshot = parser.snapshot()

    assert snapshot["status"]["state"] == "valid_fix"
    assert snapshot["position"]["latitude"] == 48.1173
    assert snapshot["position"]["longitude"] == 11.51666667
    assert snapshot["position"]["altitude_m"] == 545.4
    assert snapshot["motion"]["speed_knots"] == 22.4
    assert snapshot["motion"]["speed_kmh"] == 41.485
    assert snapshot["motion"]["course_degrees"] == 84.4
    assert snapshot["motion"]["magnetic_variation_degrees"] == -3.1
    assert snapshot["accuracy"]["hdop"] == 1.0
    assert snapshot["accuracy"]["pdop"] == 1.8
    assert snapshot["accuracy"]["vdop"] == 1.5
    assert snapshot["fix"]["quality"] == 1
    assert snapshot["fix"]["quality_label"] == "GPS"
    assert snapshot["fix"]["gsa_fix_type_label"] == "3D fix"
    assert snapshot["satellites"]["used_count"] == 7
    assert snapshot["satellites"]["in_view_count"] == 3
    assert snapshot["satellites"]["snr"]["max"] == 42.0
    assert snapshot["time"]["date"] == "1994-03-23"
    assert snapshot["time"]["datetime_utc"] == "1994-03-23T12:35:19.000000+00:00"
    assert set(snapshot["nmea"]["seen_sentence_types"]) == {"GGA", "GSA", "GSV", "RMC"}


def test_nmea_parser_rejects_bad_checksum_when_validation_enabled():
    parser = NMEAParser(validate_checksum=True)

    accepted = parser.ingest_sentence(
        "$GPRMC,123519,A,4807.038,N,01131.000,E,022.4,084.4,230394,003.1,W*00"
    )

    snapshot = parser.snapshot()
    assert accepted is False
    assert snapshot["status"]["state"] == "error"
    assert snapshot["nmea"]["invalid_checksum_count"] == 1
    assert snapshot["status"]["last_error"] == "NMEA checksum mismatch"


def test_gps_service_file_source_reads_nmea_lines(tmp_path):
    path = tmp_path / "gps_nmea.txt"
    path.write_text(
        "\n".join(
            [
                _sentence("GPRMC,010203,A,4250.123,N,07106.456,W,000.0,180.0,230426,,"),
                _sentence("GPGGA,010203,4250.123,N,07106.456,W,1,05,1.4,32.0,M,0.0,M,,"),
            ]
        ),
        encoding="utf-8",
    )

    service = GPSService(
        {
            "gps": {
                "enabled": True,
                "source": "file",
                "source_path": str(path),
                "poll_interval_seconds": 0.05,
                "stale_after_seconds": 5.0,
            }
        }
    )
    service.start()
    try:
        deadline = time.time() + 1.0
        snapshot = service.get_snapshot()
        while snapshot["status"]["state"] == "no_data" and time.time() < deadline:
            time.sleep(0.05)
            snapshot = service.get_snapshot()
    finally:
        service.stop()

    assert snapshot["status"]["state"] == "valid_fix"
    assert snapshot["position"]["latitude"] == 42.83538333
    assert snapshot["position"]["longitude"] == -71.1076
    assert snapshot["satellites"]["used_count"] == 5


def test_gps_service_uses_manual_location_until_gps_fix():
    service = GPSService(
        {
            "repeater": {
                "latitude": 42.123456,
                "longitude": -71.654321,
            },
            "gps": {
                "enabled": True,
            },
        }
    )

    assert service.ingest_sentence(
        _sentence("GPGGA,010203,4250.123,N,07106.456,W,0,00,8.8,32.0,M,0.0,M,,")
    )

    snapshot = service.get_snapshot()

    assert snapshot["status"]["state"] == "invalid_fix"
    assert snapshot["position"]["latitude"] == 42.123456
    assert snapshot["position"]["longitude"] == -71.654321
    assert snapshot["gps_position"]["latitude"] == 42.83538333
    assert snapshot["gps_position"]["longitude"] == -71.1076
    assert snapshot["manual_position"]["latitude"] == 42.123456
    assert snapshot["manual_position"]["longitude"] == -71.654321
    assert snapshot["position_meta"]["source"] == "manual_config"
    assert snapshot["position_meta"]["source_label"] == "manual config until GPS fix"
    assert snapshot["position_meta"]["manual_config_available"] is True
    assert snapshot["position_meta"]["gps_fix_valid"] is False


def test_gps_service_uses_gps_location_after_valid_fix():
    service = GPSService(
        {
            "repeater": {
                "latitude": 42.123456,
                "longitude": -71.654321,
            },
            "gps": {
                "enabled": True,
            },
        }
    )

    assert service.ingest_sentence(
        _sentence("GPGGA,010203,4250.123,N,07106.456,W,1,05,1.4,32.0,M,0.0,M,,")
    )

    snapshot = service.get_snapshot()

    assert snapshot["status"]["state"] == "valid_fix"
    assert snapshot["position"]["latitude"] == 42.83538333
    assert snapshot["position"]["longitude"] == -71.1076
    assert snapshot["manual_position"]["latitude"] == 42.123456
    assert snapshot["manual_position"]["longitude"] == -71.654321
    assert snapshot["position_meta"]["source"] == "gps"
    assert snapshot["position_meta"]["source_label"] == "GPS fix"
    assert snapshot["position_meta"]["manual_config_available"] is True
    assert snapshot["position_meta"]["gps_fix_valid"] is True


def test_gps_service_treats_zero_zero_manual_location_as_unset():
    service = GPSService(
        {
            "repeater": {
                "latitude": 0.0,
                "longitude": 0.0,
            },
            "gps": {
                "enabled": True,
            },
        }
    )

    assert service.ingest_sentence(
        _sentence("GPGGA,010203,4250.123,N,07106.456,W,0,00,8.8,32.0,M,0.0,M,,")
    )

    snapshot = service.get_snapshot()

    assert snapshot["status"]["state"] == "invalid_fix"
    assert snapshot["position"]["latitude"] == 42.83538333
    assert snapshot["position"]["longitude"] == -71.1076
    assert snapshot["manual_position"] is None
    assert snapshot["position_meta"]["source"] == "gps"
    assert snapshot["position_meta"]["source_label"] == "GPS estimate"
    assert snapshot["position_meta"]["manual_config_available"] is False
    assert snapshot["position_meta"]["gps_fix_valid"] is False


def test_gps_service_reflects_runtime_manual_location_updates():
    config = {
        "repeater": {
            "latitude": 0.0,
            "longitude": 0.0,
        },
        "gps": {
            "enabled": True,
        },
    }
    service = GPSService(config)

    assert service.ingest_sentence(
        _sentence("GPGGA,010203,4250.123,N,07106.456,W,0,00,8.8,32.0,M,0.0,M,,")
    )
    assert service.get_snapshot()["position_meta"]["source"] == "gps"

    config["repeater"]["latitude"] = 42.123456
    config["repeater"]["longitude"] = -71.654321
    snapshot = service.get_snapshot()

    assert snapshot["position"]["latitude"] == 42.123456
    assert snapshot["position"]["longitude"] == -71.654321
    assert snapshot["gps_position"]["latitude"] == 42.83538333
    assert snapshot["gps_position"]["longitude"] == -71.1076
    assert snapshot["position_meta"]["source"] == "manual_config"
