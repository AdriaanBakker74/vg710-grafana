#!/usr/bin/env python3
"""
Backfill historische NMEA bestanden naar de 5-minuten dataset.

Verwerkt alle bestaande NMEA bestanden parallel en slaat de output op in S3.
Gebruik dit eenmalig na de eerste deployment van de Lambda.

Gebruik:
  pip install boto3
  python3 backfill_nmea.py                              # alle data
  python3 backfill_nmea.py --date-from 2026-04-01      # vanaf datum
  python3 backfill_nmea.py --date-to   2026-04-13      # tot datum
  python3 backfill_nmea.py --workers 50                 # parallelisme
  python3 backfill_nmea.py --dry-run                    # simulatie

Optimalisatie: de window_start wordt bepaald uit de bestandsnaam (gratis).
Als het bijbehorende uitvoerbestand al bestaat, wordt de download overgeslagen.
Dit maakt hervatten na onderbreking zeer efficiënt.
"""

import argparse
import json
import logging
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

# Één S3-client per thread: hergebruikt TCP/TLS-verbindingen en is ~100x sneller
# dan elke aanroep een nieuwe client laten aanmaken.
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
OUTPUT_PREFIX = "vg710-5min"
WINDOW_MINUTES = 5

GGA_FIX_LABELS = {
    0: "Geen fix", 1: "Standalone", 2: "DGPS", 3: "PPS",
    4: "RTK Fixed", 5: "RTK Float", 6: "Geschat",
}


# ---------------------------------------------------------------------------
# NMEA parsing (identiek aan Lambda handler)
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


def _floor_to_window(ts_str):
    dt = datetime.fromisoformat(ts_str)
    windowed = dt.replace(second=0, microsecond=0, minute=(dt.minute // WINDOW_MINUTES) * WINDOW_MINUTES)
    # Tijdzone verwijderen: schone bestandsnamen zonder '+00-00', veilig voor S3 URL-encoding
    return windowed.replace(tzinfo=None).isoformat()


def _filename_to_iso_ts(filename_noext):
    """
    NMEA-bestandsnamen gebruiken koppeltekens i.p.v. dubbele punten in de tijd.
    '2026-04-06T14-51-45.078530+00-00' → '2026-04-06T14:51:45.078530+00:00'
    """
    if "T" not in filename_noext:
        return filename_noext
    date_str, time_str = filename_noext.split("T", 1)
    # Vervang UU-MM-SS koppeltekens door dubbele punten
    time_str = re.sub(r"^(\d{2})-(\d{2})-", r"\1:\2:", time_str)
    # Vervang tijdzone-koppelteken: +00-00 → +00:00
    time_str = re.sub(r"([+-]\d{2})-(\d{2})$", r"\1:\2", time_str)
    return f"{date_str}T{time_str}"


def _make_window_output_key(device_id, date_part, window_start):
    """
    Één S3-object per 5-minuut-venster per apparaat.
    Output: {OUTPUT_PREFIX}/{device_id}/{YYYY-MM-DD}/{YYYY-MM-DDTHH-MM-SS}.ndjson
    """
    window_safe = window_start.replace(":", "-")
    return f"{OUTPUT_PREFIX}/{device_id}/{date_part}/{window_safe}.ndjson"


# ---------------------------------------------------------------------------
# S3 helpers
# ---------------------------------------------------------------------------

def _list_nmea_keys(s3, date_from, date_to):
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=RAW_PREFIX + "/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if "/nmea/" not in key or not key.endswith(".ndjson"):
                continue
            parts = key.split("/")
            try:
                nmea_idx = next(i for i, p in enumerate(parts) if p == "nmea")
                date_part = parts[nmea_idx + 1]
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


def _process_key(key, dry_run, skip_existing):
    """
    Verwerk één NMEA invoerbestand.

    Optimalisatie: de primaire window_start wordt bepaald uit de bestandsnaam.
    Als het uitvoerbestand al bestaat, wordt de download volledig overgeslagen.
    Dit maakt het hervatten na onderbreking ~10x sneller (geen GET nodig).
    """
    s3 = _get_s3()

    # Haal device_id en date_part uit de sleutel
    parts = key.split("/")
    try:
        nmea_idx = next(i for i, p in enumerate(parts) if p == "nmea")
    except StopIteration:
        return "skip", key, "geen nmea-pad"
    if nmea_idx < 1 or len(parts) < nmea_idx + 3:
        return "skip", key, "onverwachte sleutelstructuur"

    device_id = parts[nmea_idx - 1]
    date_part = parts[nmea_idx + 1]
    filename = parts[-1]  # bijv. "2026-04-13T10:04:35.ndjson"

    # Bepaal primaire window_start uit bestandsnaam (geen S3-aanroep nodig).
    # Bestandsnamen gebruiken koppeltekens i.p.v. dubbele punten in de tijd.
    try:
        ts_str = _filename_to_iso_ts(filename.replace(".ndjson", ""))
        primary_window = _floor_to_window(ts_str)
    except Exception:
        primary_window = None

    # Snel overslaan als uitvoer al bestaat — geen download nodig
    if skip_existing and primary_window:
        primary_out_key = _make_window_output_key(device_id, date_part, primary_window)
        if _output_exists(s3, primary_out_key):
            return "skip", key, f"venster {primary_window} bestaat al"

    if dry_run:
        return "dry-run", key, f"→ {device_id}/{date_part}/{primary_window}"

    # Download en verwerk het bestand
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
        sentence = rec.get("sentence", "")
        ts = rec.get("ts", "")
        if not ts or "GGA" not in sentence:
            continue
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

    if not windows:
        return "skip", key, "geen geldige GGA-regels"

    written = 0
    for window_start, data in windows.items():
        out_key = _make_window_output_key(device_id, date_part, window_start)
        if skip_existing and _output_exists(s3, out_key):
            continue
        s3.put_object(
            Bucket=BUCKET,
            Key=out_key,
            Body=(json.dumps(data, separators=(",", ":")) + "\n").encode("utf-8"),
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
    parser = argparse.ArgumentParser(description="VG710 NMEA backfill naar 5-minuten dataset")
    parser.add_argument("--date-from", default=None, metavar="YYYY-MM-DD")
    parser.add_argument("--date-to",   default=None, metavar="YYYY-MM-DD")
    parser.add_argument("--workers",   type=int, default=100,
                        help="Aantal parallelle threads (default: 100)")
    parser.add_argument("--dry-run",   action="store_true", help="Simuleer zonder te schrijven")
    parser.add_argument("--skip-existing", action="store_true", default=True,
                        help="Sla vensters over die al verwerkt zijn (default: aan)")
    parser.add_argument("--no-skip-existing", dest="skip_existing", action="store_false")
    args = parser.parse_args()

    s3 = boto3.client("s3", region_name=REGION)

    log.info(f"Zoeken in s3://{BUCKET}/{RAW_PREFIX}/  "
             f"(van={args.date_from or 'begin'} tot={args.date_to or 'nu'})")
    keys = list(_list_nmea_keys(s3, args.date_from, args.date_to))
    log.info(f"Gevonden: {len(keys)} NMEA bestanden")
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

            # Voortgangsrapport elke 30 seconden
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
