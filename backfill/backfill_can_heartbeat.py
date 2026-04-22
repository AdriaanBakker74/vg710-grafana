#!/usr/bin/env python3
"""
Backfill historische CAN-bestanden naar de can-heartbeat dataset.

Verwerkt alle bestaande CAN-bestanden parallel en slaat de output op in S3.
Gebruik dit eenmalig na de eerste deployment van de Lambda.

Gebruik:
  pip install boto3
  python3 backfill_can_heartbeat.py                              # alle data
  python3 backfill_can_heartbeat.py --date-from 2026-04-01      # vanaf datum
  python3 backfill_can_heartbeat.py --date-to   2026-04-13      # tot datum
  python3 backfill_can_heartbeat.py --workers 50                 # parallelisme
  python3 backfill_can_heartbeat.py --dry-run                    # simulatie

Sensorgroepen worden geconfigureerd via --sensor-groups of de SENSOR_GROUPS
omgevingsvariabele (JSON-array van {"name","id_min","id_max"}).
"""

import argparse
import json
import logging
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

_thread_local = threading.local()

_BOTO_CONFIG = Config(
    retries={"max_attempts": 5, "mode": "adaptive"},
    max_pool_connections=5,
)


def _get_s3():
    if not hasattr(_thread_local, "s3"):
        _thread_local.s3 = boto3.client("s3", region_name=REGION, config=_BOTO_CONFIG)
    return _thread_local.s3


logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)
log = logging.getLogger(__name__)

REGION = "eu-north-1"
BUCKET = "bmc-vg710-raw-eun1"
RAW_PREFIX = "vg710-raw"
OUTPUT_PREFIX = "vg710-can-heartbeat"
WINDOW_MINUTES = 5

DEFAULT_SENSOR_GROUPS = [
    {"name": "Temperatuur", "id_min": 640,  "id_max": 680},
    {"name": "Weerstation", "id_min": 896,  "id_max": 936},
    {"name": "Trilling",    "id_min": 1152, "id_max": 1192},
]

SENSOR_GROUPS = None  # set in main()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _sensor_group_for(can_id):
    for group in SENSOR_GROUPS:
        if group["id_min"] <= can_id <= group["id_max"]:
            return group["name"]
    return None


def _floor_to_window(ts_str):
    dt = datetime.fromisoformat(ts_str)
    windowed = dt.replace(
        second=0, microsecond=0,
        minute=(dt.minute // WINDOW_MINUTES) * WINDOW_MINUTES,
    )
    return windowed.replace(tzinfo=None).isoformat()


def _filename_to_iso_ts(filename_noext):
    """
    CAN-bestandsnamen gebruiken koppeltekens i.p.v. dubbele punten in de tijd.
    '2026-04-06T14-51-45.078530+00-00' → '2026-04-06T14:51:45.078530+00:00'
    """
    if "T" not in filename_noext:
        return filename_noext
    date_str, time_str = filename_noext.split("T", 1)
    time_str = re.sub(r"^(\d{2})-(\d{2})-", r"\1:\2:", time_str)
    time_str = re.sub(r"([+-]\d{2})-(\d{2})$", r"\1:\2", time_str)
    return f"{date_str}T{time_str}"


def _make_output_key(device_id, date_part, window_start):
    window_safe = window_start.replace(":", "-")
    return f"{OUTPUT_PREFIX}/{device_id}/{date_part}/{window_safe}.ndjson"


# ---------------------------------------------------------------------------
# S3 helpers
# ---------------------------------------------------------------------------

def _list_can_keys(s3, date_from, date_to):
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=RAW_PREFIX + "/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if "/can/" not in key or not key.endswith(".ndjson"):
                continue
            parts = key.split("/")
            try:
                can_idx = next(i for i, p in enumerate(parts) if p == "can")
                date_part = parts[can_idx + 1]
            except (StopIteration, IndexError):
                continue
            if date_from and date_part < date_from:
                continue
            if date_to and date_part > date_to:
                continue
            yield key


def _output_exists(s3, out_key):
    try:
        s3.head_object(Bucket=BUCKET, Key=out_key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
            return False
        raise


# ---------------------------------------------------------------------------
# Verwerking per bestand
# ---------------------------------------------------------------------------

def _process_key(key, dry_run, skip_existing):
    s3 = _get_s3()

    parts = key.split("/")
    try:
        can_idx = next(i for i, p in enumerate(parts) if p == "can")
    except StopIteration:
        return "skip", key, "geen can-pad"
    if can_idx < 1 or len(parts) < can_idx + 3:
        return "skip", key, "onverwachte sleutelstructuur"

    device_id = parts[can_idx - 1]
    date_part = parts[can_idx + 1]
    filename = parts[-1]

    try:
        ts_str = _filename_to_iso_ts(filename.replace(".ndjson", ""))
        primary_window = _floor_to_window(ts_str)
    except Exception:
        primary_window = None

    if skip_existing and primary_window:
        primary_out_key = _make_output_key(device_id, date_part, primary_window)
        if _output_exists(s3, primary_out_key):
            return "skip", key, f"venster {primary_window} bestaat al"

    if dry_run:
        return "dry-run", key, f"→ {device_id}/{date_part}/{primary_window}"

    obj = s3.get_object(Bucket=BUCKET, Key=key)
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

        rec_device_id = rec.get("device_id", device_id)
        can_id = rec.get("id")

        if SENSOR_GROUPS and can_id is not None:
            group = _sensor_group_for(int(can_id))
            if group is None:
                continue
        else:
            group = "CAN"

        if window not in windows:
            windows[window] = {"_device_id": rec_device_id}
        windows[window][group] = windows[window].get(group, 0) + 1

    if not windows:
        return "skip", key, "geen geldige CAN-berichten in geconfigureerde groepen"

    # Schrijf per venster één NDJSON-bestand met meerdere regels
    by_window = {}
    for window_start, counts in windows.items():
        rec_device_id = counts.pop("_device_id", device_id)
        for group, count in counts.items():
            record = {
                "device_id": rec_device_id,
                "window_start": window_start,
                "date_part": window_start[:10],
                "sensor_group": group,
                "message_count": count,
            }
            by_window.setdefault(window_start, []).append(record)

    written = 0
    for window_start, records in by_window.items():
        out_key = _make_output_key(device_id, records[0]["date_part"], window_start)
        if skip_existing and _output_exists(s3, out_key):
            continue
        body_out = "\n".join(json.dumps(r, separators=(",", ":")) for r in records) + "\n"
        s3.put_object(
            Bucket=BUCKET,
            Key=out_key,
            Body=body_out.encode("utf-8"),
            ContentType="application/x-ndjson",
        )
        written += 1

    if written == 0:
        return "skip", key, "alle vensters bestaan al"
    return "ok", key, f"{written} vensters → {device_id}/{date_part}/"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    global SENSOR_GROUPS

    parser = argparse.ArgumentParser(description="VG710 CAN heartbeat backfill")
    parser.add_argument("--date-from",      default=None, metavar="YYYY-MM-DD")
    parser.add_argument("--date-to",        default=None, metavar="YYYY-MM-DD")
    parser.add_argument("--workers",        type=int, default=50,
                        help="Aantal parallelle threads (default: 50)")
    parser.add_argument("--dry-run",        action="store_true", help="Simuleer zonder te schrijven")
    parser.add_argument("--sensor-groups",  default=None, metavar="JSON",
                        help='JSON-array van sensorgroepen, bijv. \'[{"name":"Temp","id_min":640,"id_max":680}]\'')
    parser.add_argument("--skip-existing",  action="store_true", default=True)
    parser.add_argument("--no-skip-existing", dest="skip_existing", action="store_false")
    args = parser.parse_args()

    # Sensorgroepen bepalen: arg > env > default
    raw_groups = args.sensor_groups or os.environ.get("SENSOR_GROUPS", "")
    if raw_groups:
        try:
            SENSOR_GROUPS = json.loads(raw_groups)
        except Exception as e:
            log.error(f"Ongeldige --sensor-groups JSON: {e}")
            sys.exit(1)
    else:
        SENSOR_GROUPS = DEFAULT_SENSOR_GROUPS

    log.info(f"Sensorgroepen: {SENSOR_GROUPS}")

    s3 = boto3.client("s3", region_name=REGION)

    log.info(f"Zoeken in s3://{BUCKET}/{RAW_PREFIX}/*/can/  "
             f"(van={args.date_from or 'begin'} tot={args.date_to or 'nu'})")
    keys = list(_list_can_keys(s3, args.date_from, args.date_to))
    log.info(f"Gevonden: {len(keys)} CAN bestanden")
    log.info(f"Werkers: {args.workers}  |  skip-existing: {args.skip_existing}")

    if not keys:
        log.info("Niets te verwerken.")
        return

    counts = {"ok": 0, "skip": 0, "error": 0, "dry-run": 0}
    start = time.time()
    last_log = start

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {
            pool.submit(_process_key, key, args.dry_run, args.skip_existing): key
            for key in keys
        }
        for i, future in enumerate(as_completed(futures), 1):
            try:
                status, key, detail = future.result()
                counts[status] += 1
                if status == "ok":
                    short_key = "/".join(key.split("/")[-4:])
                    log.info(f"[{i}/{len(keys)}] OK   {short_key} → {detail}")
                elif status == "error":
                    short_key = "/".join(key.split("/")[-4:])
                    log.error(f"[{i}/{len(keys)}] FOUT {short_key}: {detail}")
            except Exception as e:
                counts["error"] += 1
                log.error(f"[{i}/{len(keys)}] FOUT {futures[future]}: {e}")

            now = time.time()
            if now - last_log >= 30:
                elapsed = now - start
                rate = i / elapsed if elapsed > 0 else 0
                eta = (len(keys) - i) / rate if rate > 0 else 0
                log.info(
                    f"Voortgang: {i}/{len(keys)} bestanden  "
                    f"({rate:.0f}/s)  ETA: {eta/60:.0f} min  "
                    f"ok={counts['ok']}  skip={counts['skip']}  fout={counts['error']}"
                )
                last_log = now

    elapsed = time.time() - start
    log.info(
        f"\nKlaar in {elapsed:.0f}s — "
        f"ok={counts['ok']}  skip={counts['skip']}  "
        f"fout={counts['error']}  dry-run={counts['dry-run']}"
    )
    if counts["error"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
