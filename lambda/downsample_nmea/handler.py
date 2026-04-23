"""
VG710 NMEA + CAN-bus verwerker.

Triggered via EventBridge (S3 ObjectCreated).

NMEA-verwerking:
  Input:  s3://{bucket}/vg710-raw/{device_id}/nmea/{date}/{hour}/{ts}.ndjson
  Output: s3://{bucket}/vg710-5min/{device_id}/{date}/{window_start}.ndjson
  Inhoud: eerste GPS-positie per 5-minuutvenster (GGA parsed)

CAN-heartbeat:
  Input:  s3://{bucket}/vg710-raw/{device_id}/can/{date}/{hour}/{ts}.ndjson
  Output: s3://{bucket}/vg710-can-heartbeat/{device_id}/{date}/{window_start}.ndjson
  Inhoud: één JSON-regel per actieve sensorgroep per venster

Sensorgroepen worden geconfigureerd via de SENSOR_GROUPS omgevingsvariabele:
  Format: JSON-array, bijv.:
  [{"name":"Temperatuur","id_min":640,"id_max":671},
   {"name":"Weerstation","id_min":896,"id_max":927},
   {"name":"Trilling","id_min":1152,"id_max":1183}]
"""

import json
import logging
import os
from datetime import datetime
from urllib.parse import unquote, unquote_plus

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

S3 = boto3.client("s3")

WINDOW_MINUTES       = int(os.environ.get("WINDOW_MINUTES", "5"))
OUTPUT_PREFIX        = os.environ.get("OUTPUT_PREFIX", "vg710-5min")
CAN_HEARTBEAT_PREFIX = os.environ.get("CAN_HEARTBEAT_PREFIX", "vg710-can-heartbeat")

GGA_FIX_LABELS = {
    0: "Geen fix", 1: "Standalone", 2: "DGPS", 3: "PPS",
    4: "RTK Fixed", 5: "RTK Float", 6: "Geschat",
}

# Sensorgroepen laden vanuit env (lijst van {"name","id_min","id_max"})
_RAW_SENSOR_GROUPS = os.environ.get("SENSOR_GROUPS", "[]")
try:
    SENSOR_GROUPS = json.loads(_RAW_SENSOR_GROUPS)
except Exception:
    SENSOR_GROUPS = []


# ---------------------------------------------------------------------------
# Gedeelde helpers
# ---------------------------------------------------------------------------

def _floor_to_window(ts_str):
    dt = datetime.fromisoformat(ts_str)
    windowed = dt.replace(
        second=0, microsecond=0,
        minute=(dt.minute // WINDOW_MINUTES) * WINDOW_MINUTES,
    )
    # Tijdzone verwijderen: schone bestandsnamen zonder '+00-00'
    return windowed.replace(tzinfo=None).isoformat()


def _parse_device_from_key(input_key, subfolder):
    """Return (device_id, date_part) uit een S3-sleutel van de vorm
    vg710-raw/{device_id}/{subfolder}/{date}/{hour}/{ts}.ndjson."""
    parts = input_key.split("/")
    try:
        idx = next(i for i, p in enumerate(parts) if p == subfolder)
    except StopIteration:
        return None, None
    if idx < 1 or len(parts) < idx + 2:
        return None, None
    return parts[idx - 1], parts[idx + 1]


def write_window(bucket, out_key, record):
    """Schrijf één venster-record naar S3 (één JSON-regel)."""
    S3.put_object(
        Bucket=bucket,
        Key=out_key,
        Body=(json.dumps(record, separators=(",", ":")) + "\n").encode("utf-8"),
        ContentType="application/x-ndjson",
    )


# ---------------------------------------------------------------------------
# NMEA parsing
# ---------------------------------------------------------------------------

def _nmea_to_decimal(value, direction):
    if not value:
        return None
    try:
        dot = value.index(".")
        degrees = int(value[:dot - 2])
        minutes = float(value[dot - 2:])
        decimal = degrees + minutes / 60.0
        if direction in ("S", "W"):
            decimal = -decimal
        return round(decimal, 8)
    except Exception:
        return None


def _parse_gga(sentence):
    try:
        if "*" in sentence:
            sentence = sentence[:sentence.index("*")]
        parts = sentence.split(",")
        if len(parts) < 10 or not parts[0].endswith("GGA"):
            return None
        fix_quality = int(parts[6]) if parts[6] else 0
        if fix_quality == 0:
            return None
        lat = _nmea_to_decimal(parts[2], parts[3])
        lon = _nmea_to_decimal(parts[4], parts[5])
        if lat is None or lon is None:
            return None
        return {
            "fix_quality": fix_quality,
            "fix_label": GGA_FIX_LABELS.get(fix_quality, f"Fix {fix_quality}"),
            "lat": lat,
            "lon": lon,
            "satellites": int(parts[7]) if parts[7] else None,
            "hdop": float(parts[8]) if parts[8] else None,
            "altitude": float(parts[9]) if parts[9] else None,
        }
    except Exception:
        return None


def _is_nmea_key(key):
    return "/nmea/" in key and key.endswith(".ndjson")


def make_nmea_output_key(device_id, date_part, window_start):
    return f"{OUTPUT_PREFIX}/{device_id}/{date_part}/{window_start.replace(':', '-')}.ndjson"


def process_nmea_file(bucket, key):
    """Parseer een NMEA-bestand en retourneer windows {window_start: record}."""
    obj = S3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read().decode("utf-8")

    windows = {}
    lines_parsed = 0

    for line in body.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            continue

        sentence = rec.get("sentence", "")
        ts = rec.get("ts", "")
        if not ts or "GGA" not in sentence:
            continue

        lines_parsed += 1
        gga = _parse_gga(sentence)
        if not gga:
            continue

        window = _floor_to_window(ts)
        if window not in windows:
            windows[window] = {
                "device_id": rec.get("device_id", "unknown"),
                "ts": ts,
                "window_start": window,
                "date_part": window[:10],
                **gga,
            }

    logger.info("nmea_parsed", extra={"gga_lines": lines_parsed, "windows": len(windows)})
    return windows


# ---------------------------------------------------------------------------
# CAN-bus heartbeat verwerking
# ---------------------------------------------------------------------------

def _is_can_key(key):
    return "/can/" in key and key.endswith(".ndjson")


def make_can_output_key(device_id, date_part, window_start):
    return f"{CAN_HEARTBEAT_PREFIX}/{device_id}/{date_part}/{window_start.replace(':', '-')}.ndjson"


def _sensor_group_for(can_id):
    """Retourneer de naam van de sensorgroep voor dit CAN ID, of None."""
    for group in SENSOR_GROUPS:
        if group["id_min"] <= can_id <= group["id_max"]:
            return group["name"]
    return None


def process_can_file(bucket, key):
    """
    Parseer een CAN-bestand.
    Retourneert {window_start: {sensor_group: count, ...}} per venster.
    Als SENSOR_GROUPS leeg is, worden alle berichten in één groep 'CAN' geteld.
    """
    obj = S3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read().decode("utf-8")

    windows = {}

    for line in body.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            continue

        ts = rec.get("ts", "")
        if not ts:
            continue
        try:
            window = _floor_to_window(ts)
        except Exception:
            continue

        device_id = rec.get("device_id", "unknown")
        can_id = rec.get("id")

        if SENSOR_GROUPS and can_id is not None:
            group = _sensor_group_for(int(can_id))
            if group is None:
                continue  # buiten alle geconfigureerde bereiken
        else:
            group = "CAN"

        if window not in windows:
            windows[window] = {"_device_id": device_id}
        windows[window][group] = windows[window].get(group, 0) + 1

    logger.info("can_parsed", extra={"windows": len(windows)})
    return windows


def _build_can_records(windows):
    """
    Zet het ruwe windows-dict om naar een lijst van output-records,
    één record per (window_start, sensor_group).
    """
    records = []
    for window_start, counts in windows.items():
        device_id = counts.pop("_device_id", "unknown")
        for group, count in counts.items():
            records.append({
                "device_id": device_id,
                "window_start": window_start,
                "date_part": window_start[:10],
                "sensor_group": group,
                "message_count": count,
            })
    return records


# ---------------------------------------------------------------------------
# Event parsing — supports both EventBridge and direct S3 event formats
# ---------------------------------------------------------------------------

def _extract_records(event):
    pairs = []
    if event.get("source") == "aws.s3":
        bucket = event["detail"]["bucket"]["name"]
        key = unquote(event["detail"]["object"]["key"])
        pairs.append((bucket, key))
        return pairs
    for record in event.get("Records", []):
        if "s3" in record:
            bucket = record["s3"]["bucket"]["name"]
            key = unquote_plus(record["s3"]["object"]["key"])
            pairs.append((bucket, key))
    return pairs


# ---------------------------------------------------------------------------
# Lambda entry point
# ---------------------------------------------------------------------------

def handler(event, context):
    records = _extract_records(event)
    processed = skipped = errors = 0

    for bucket, key in records:

        # --- NMEA ---
        if _is_nmea_key(key):
            device_id, date_part = _parse_device_from_key(key, "nmea")
            if not device_id:
                logger.warning(f"SKIP cannot parse device from NMEA key: {key}")
                skipped += 1
                continue
            try:
                windows = process_nmea_file(bucket, key)
                if not windows:
                    logger.info(f"SKIP no valid GGA in {key}")
                    skipped += 1
                    continue
                for window_start, data in windows.items():
                    out_key = make_nmea_output_key(data["device_id"], data["date_part"], window_start)
                    write_window(bucket, out_key, data)
                logger.info(f"NMEA OK {len(windows)} vensters → {OUTPUT_PREFIX}/{device_id}/{date_part}/")
                processed += 1
            except ClientError as e:
                logger.error(f"AWS error NMEA ({e.response['Error']['Code']}): {key}")
                errors += 1
                raise
            except Exception as e:
                logger.error(f"Error NMEA {key}: {e}")
                errors += 1
                raise

        # --- CAN heartbeat ---
        elif _is_can_key(key):
            device_id, date_part = _parse_device_from_key(key, "can")
            if not device_id:
                logger.warning(f"SKIP cannot parse device from CAN key: {key}")
                skipped += 1
                continue
            try:
                windows = process_can_file(bucket, key)
                if not windows:
                    logger.info(f"SKIP no valid CAN records in {key}")
                    skipped += 1
                    continue
                # Groepeer records per window_start en schrijf als multi-line NDJSON
                all_records = _build_can_records(windows)
                by_window = {}
                for rec in all_records:
                    by_window.setdefault(rec["window_start"], []).append(rec)
                for window_start, recs in by_window.items():
                    out_key = make_can_output_key(device_id, recs[0]["date_part"], window_start)
                    body = "\n".join(json.dumps(r, separators=(",", ":")) for r in recs) + "\n"
                    S3.put_object(
                        Bucket=bucket, Key=out_key,
                        Body=body.encode("utf-8"),
                        ContentType="application/x-ndjson",
                    )
                logger.info(f"CAN OK {len(by_window)} vensters → {CAN_HEARTBEAT_PREFIX}/{device_id}/{date_part}/")
                processed += 1
            except ClientError as e:
                logger.error(f"AWS error CAN ({e.response['Error']['Code']}): {key}")
                errors += 1
                raise
            except Exception as e:
                logger.error(f"Error CAN {key}: {e}")
                errors += 1
                raise

        else:
            logger.info(f"SKIP not NMEA or CAN: {key}")
            skipped += 1

    summary = {"processed": processed, "skipped": skipped, "errors": errors}
    logger.info(f"Summary: {summary}")
    return summary
