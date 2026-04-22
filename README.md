# VG710 Grafana Dashboard

Grafana dashboard voor GPS-tracking en CAN-bus monitoring van VG710 industriële routers via AWS S3 en Athena.

## Architectuur

```
VG710 router
    │
    ├─▶ NMEA (GGA) bestanden, ~30 seconden per bestand
    │   S3: bmc-vg710-raw-eun1/vg710-raw/{device_id}/nmea/{datum}/{uur}/{ts}.ndjson
    │       │
    │       ▼ EventBridge (S3 ObjectCreated)
    │   Lambda: vg710-nmea-downsampler
    │       │  · Parseert GGA-regels (lat/lon/hoogte/satellieten/HDOP/fix_quality)
    │       │  · Groepeert op 5-minuutvensters
    │       │  · Schrijft één bestand per venster (idempotent)
    │       ▼
    │   S3: bmc-vg710-raw-eun1/vg710-5min/{device_id}/{datum}/{venster}.ndjson
    │       │
    │       ▼ Athena externe tabel
    │   vg710_db.nmea_5min
    │       │
    │       ▼ Grafana Cloud (Amazon Athena datasource)
    │   Dashboard: VG710 GPS Track
    │
    └─▶ CAN-bus bestanden, ~30 seconden per bestand
        S3: bmc-vg710-raw-eun1/vg710-raw/{device_id}/can/{datum}/{uur}/{ts}.ndjson
            │
            ▼ EventBridge (S3 ObjectCreated)
        Lambda: vg710-nmea-downsampler
            │  · Groepeert berichten op 5-minuutvensters
            │  · Classificeert CAN IDs in sensorgroepen
            │  · Schrijft heartbeat per sensorgroep per venster
            ▼
        S3: bmc-vg710-raw-eun1/vg710-can-heartbeat/{device_id}/{datum}/{venster}.ndjson
            │
            ▼ Athena externe tabel
        vg710_db.can_heartbeat
            │
            ▼ Grafana Cloud (Amazon Athena datasource)
        Dashboard: VG710 GPS Track + VG710 Online Status
```

**Ruwe 1Hz-data** blijft intact in `vg710-raw/` voor download.
Het dashboard gebruikt uitsluitend de lichte `nmea_5min` en `can_heartbeat` datasets.

## Bestanden

```
deploy.sh                               Deploymentscript (AWS CLI)
lambda/
  downsample_nmea/handler.py            Lambda: NMEA → 5-min posities + CAN → heartbeat
backfill/
  backfill_nmea.py                      Eenmalig backfill-script voor historische NMEA-data
  backfill_can_heartbeat.py             Eenmalig backfill-script voor historische CAN-data
athena/
  create_nmea_5min_table.sql            Athena-tabel voor de 5-min GPS-dataset
  create_can_heartbeat_table.sql        Athena-tabel voor de CAN-heartbeat dataset
  create_nmea_gps_5min_view.sql         Alternatieve view op nmea_partitioned (ongebruikt)
  sample_queries.sql                    Voorbeeldquery's voor Grafana
grafana/
  dashboard.json                        Dashboard: VG710 GPS Track (analyse + CAN)
  status_dashboard.json                 Dashboard: VG710 Online Status (mobiel)
cloudformation/
  stack.yaml                            CloudFormation-referentie (niet gedeployed)
```

## AWS-resources

| Resource | Naam |
|---|---|
| S3 bucket | `bmc-vg710-raw-eun1` (regio: `eu-north-1`) |
| Lambda | `vg710-nmea-downsampler` |
| IAM-rol | `vg710-nmea-downsampler-role` |
| EventBridge-regel | `vg710-nmea-new-file` |
| SQS Dead Letter Queue | `vg710-nmea-downsampler-dlq` |
| Athena database | `vg710_db` |
| Athena tabel | `vg710_db.nmea_5min` |
| Athena tabel | `vg710_db.can_heartbeat` |
| Athena resultaten | `bmc-vg710-athena-results-eun1` |

## Grafana dashboards

| Dashboard | UID | Doel |
|---|---|---|
| VG710 GPS Track | `vg710-gps-track` | GPS-route, signaalanalyse, CAN-sensoraanwezigheid |
| VG710 Online Status | `vg710-status` | Live status per component, optimaal voor mobiel |

### VG710 GPS Track

Volledig analysedashboard met:
- GPS-routekaart (interactief, tooltip met modem/HDOP/hoogte)
- Signaalkwaliteit: satellieten, HDOP, hoogte, GNSS-status (gekleurde stippen)
- CAN-sensoraanwezigheid per 5-minuutvenster (State Timeline)
- Historische data exporteerbaar als CSV

### VG710 Online Status

Eenvoudig statusdashboard, geoptimaliseerd voor mobiel gebruik:
- Grote gekleurde vlakken (GROEN = online, ROOD = offline)
- Componenten: Modem · GNSS Ontvanger · Temperatuur · Weerstation · Trilling
- Tijdstip laatste ontvangen data
- Auto-refresh elke 5 minuten, geen tijdkiezer

## S3-structuur

```
bmc-vg710-raw-eun1/
├── vg710-raw/
│   └── {device_id}/
│       ├── nmea/
│       │   └── {YYYY-MM-DD}/{HH}/{ts}.ndjson     ← ruwe 1Hz NMEA
│       └── can/
│           └── {YYYY-MM-DD}/{HH}/{ts}.ndjson     ← ruwe CAN-bus berichten
├── vg710-5min/
│   └── {device_id}/
│       └── {YYYY-MM-DD}/{venster}.ndjson          ← één GPS-record per 5-minuutvenster
├── vg710-can-heartbeat/
│   └── {device_id}/
│       └── {YYYY-MM-DD}/{venster}.ndjson          ← één regel per sensorgroep per venster
└── lambda/
    └── vg710-nmea-downsampler.zip
```

## nmea_5min schema

| Veld | Type | Omschrijving |
|---|---|---|
| `device_id` | STRING | Apparaat-ID (bijv. `VF7102446015943`) |
| `ts` | STRING | Eerste timestamp in het venster (ISO 8601) |
| `window_start` | STRING | Start van het 5-minuutvenster (ISO 8601, zonder tijdzone) |
| `date_part` | STRING | Datum als `YYYY-MM-DD` (voor filtering) |
| `lat` | DOUBLE | Breedtegraad (decimalen, WGS84) |
| `lon` | DOUBLE | Lengtegraad (decimalen, WGS84) |
| `altitude` | DOUBLE | Hoogte boven zeeniveau (meter) |
| `fix_quality` | INT | GGA fix quality (0=geen fix, 1=Standalone, 2=DGPS, 4=RTK Fixed) |
| `fix_label` | STRING | Leesbaar label voor fix_quality |
| `satellites` | INT | Aantal gebruikte satellieten |
| `hdop` | DOUBLE | Horizontal Dilution of Precision (lager = nauwkeuriger) |

## can_heartbeat schema

| Veld | Type | Omschrijving |
|---|---|---|
| `device_id` | STRING | Apparaat-ID |
| `window_start` | STRING | Start van het 5-minuutvenster (ISO 8601, zonder tijdzone) |
| `date_part` | STRING | Datum als `YYYY-MM-DD` (voor filtering) |
| `sensor_group` | STRING | Sensorgroep (bijv. `Temperatuur`, `Weerstation`, `Trilling`) |
| `message_count` | INT | Aantal CAN-berichten in dit venster voor deze groep |

## Sensorgroepen (CAN-bus)

Geconfigureerd via de `SENSOR_GROUPS` omgevingsvariabele van de Lambda:

| Groep | CAN ID bereik | Voorbeeldapparaat |
|---|---|---|
| Temperatuur | 640 – 680 | ID 658 (0x292) |
| Weerstation | 896 – 936 | ID 914 (0x392) |
| Trilling | 1152 – 1192 | ID 1170 (0x492) |

Format: JSON-array, bijv.:
```json
[{"name":"Temperatuur","id_min":640,"id_max":680},
 {"name":"Weerstation","id_min":896,"id_max":936},
 {"name":"Trilling","id_min":1152,"id_max":1192}]
```

## Alerting

Grafana Managed Alerts bewaken de modemstatus:

| Alert | Voorwaarde | Pending | Herhaling |
|---|---|---|---|
| VG710 Modem Offline | Geen NMEA-data ≥ 10 min | 10 min | 1 uur |

Notificaties gaan naar:
- **Grafana IRM** — push naar de Grafana iOS/Android app
- **E-mail** — `a.bakker@bmc-consultancy.com` als backup

De alert detecteert automatisch alle bekende modems (geen configuratie per apparaat nodig). Bij herstel volgt automatisch een "Resolved" melding.

## Deployment

### Vereisten

- AWS CLI geconfigureerd met toegang tot `eu-north-1`
- Rechten voor: Lambda, IAM, S3, SQS, EventBridge, CloudWatch, Athena
- Python 3 met `boto3` voor de backfill

### Eerste keer deployen

```bash
# Stap 1: Infrastructuur aanmaken en Lambda deployen
./deploy.sh

# Stap 2: Athena-tabellen aanmaken (eenmalig in Athena console)
# athena/create_nmea_5min_table.sql
# athena/create_can_heartbeat_table.sql

# Stap 3: Historische NMEA-data verwerken
cd backfill
python3 backfill_nmea.py --workers 50

# Stap 4: Historische CAN-data verwerken
python3 backfill_can_heartbeat.py --workers 50

# Optioneel: alleen een bepaalde periode
python3 backfill_nmea.py --date-from 2026-04-01 --date-to 2026-04-13 --workers 50
python3 backfill_can_heartbeat.py --date-from 2026-04-01 --date-to 2026-04-13 --workers 50
```

### Lambda bijwerken

```bash
cd lambda/downsample_nmea
zip /tmp/lambda.zip handler.py
aws s3 cp /tmp/lambda.zip s3://bmc-vg710-raw-eun1/lambda/vg710-nmea-downsampler.zip --region eu-north-1
aws lambda update-function-code \
  --function-name vg710-nmea-downsampler \
  --s3-bucket bmc-vg710-raw-eun1 \
  --s3-key lambda/vg710-nmea-downsampler.zip \
  --region eu-north-1
aws lambda wait function-updated --function-name vg710-nmea-downsampler --region eu-north-1
```

### Sensorgroepen aanpassen

```bash
# Schrijf nieuwe configuratie naar bestand en update Lambda
cat > /tmp/lambda_env.json << 'EOF'
{
  "Variables": {
    "WINDOW_MINUTES": "5",
    "OUTPUT_PREFIX": "vg710-5min",
    "CAN_HEARTBEAT_PREFIX": "vg710-can-heartbeat",
    "SENSOR_GROUPS": "[{\"name\":\"Temperatuur\",\"id_min\":640,\"id_max\":680},{\"name\":\"Weerstation\",\"id_min\":896,\"id_max\":936},{\"name\":\"Trilling\",\"id_min\":1152,\"id_max\":1192}]"
  }
}
EOF
aws lambda update-function-configuration \
  --function-name vg710-nmea-downsampler \
  --environment "file:///tmp/lambda_env.json" \
  --region eu-north-1
```

### Grafana dashboards uploaden

```bash
# GPS Track dashboard
python3 -c "
import json
with open('grafana/dashboard.json') as f: d = json.load(f)
import urllib.request, urllib.parse
payload = json.dumps({'dashboard': d['dashboard'], 'overwrite': True, 'folderId': 0}).encode()
req = urllib.request.Request('https://bmcconsultancy.grafana.net/api/dashboards/db',
    data=payload, headers={'Authorization': 'Bearer <TOKEN>', 'Content-Type': 'application/json'})
print(urllib.request.urlopen(req).read().decode())
"

# Status dashboard (zelfde aanpak met status_dashboard.json)
```

## Backfill

### NMEA backfill

Verwerkt alle bestaande NMEA-bestanden in S3 naar de `vg710-5min` dataset.

- **Slim overslaan**: `window_start` berekend uit bestandsnaam (geen download nodig als venster al bestaat)
- **Hervattend**: veilig opnieuw te starten na onderbreking
- **Parallellisme**: 50 workers standaard, één S3-client per thread
- **Snelheid**: ~230 bestanden/s → 881.000 bestanden in ~60 minuten

```bash
python3 backfill/backfill_nmea.py --workers 50
python3 backfill/backfill_nmea.py --no-skip-existing --workers 50  # herverwerken
```

### CAN backfill

Verwerkt alle bestaande CAN-bestanden in S3 naar de `vg710-can-heartbeat` dataset.

- **Snelheid**: ~230 bestanden/s → 511.000 bestanden in ~37 minuten
- Sensorgroepen configureerbaar via `--sensor-groups` of `SENSOR_GROUPS` omgevingsvariabele

```bash
python3 backfill/backfill_can_heartbeat.py --workers 50
python3 backfill/backfill_can_heartbeat.py --date-from 2026-04-01 --workers 50
```

## Athena-query's

### Alle GPS-posities in een periode

```sql
SELECT window_start, lat, lon, altitude, satellites, hdop, fix_label
FROM vg710_db.nmea_5min
WHERE device_id = 'VF7102446015943'
  AND date_part BETWEEN '2026-04-06' AND '2026-04-13'
ORDER BY window_start ASC;
```

### Laatste bekende positie

```sql
SELECT *
FROM vg710_db.nmea_5min
WHERE device_id = 'VF7102446015943'
  AND date_part = date_format(current_date, '%Y-%m-%d')
ORDER BY window_start DESC
LIMIT 1;
```

### CAN-sensoraanwezigheid vandaag

```sql
SELECT window_start, sensor_group, message_count
FROM vg710_db.can_heartbeat
WHERE device_id = 'VF7102446015943'
  AND date_part = date_format(current_date, '%Y-%m-%d')
ORDER BY window_start ASC;
```

### Modems die offline zijn (laatste 10 minuten)

```sql
SELECT device_id, MAX(window_start) AS laatste_data
FROM vg710_db.nmea_5min
WHERE date_part >= date_format(date_add('day', -1, current_date), '%Y-%m-%d')
GROUP BY device_id
HAVING MAX(from_iso8601_timestamp(window_start)) < (current_timestamp - interval '10' minute);
```

## Monitoring

| CloudWatch-alarm | Drempelwaarde | Betekenis |
|---|---|---|
| `vg710-nmea-downsampler-dlq-niet-leeg` | ≥ 1 bericht | Lambda-aanroep mislukt na 3 pogingen |
| `vg710-nmea-downsampler-fouten` | ≥ 5 fouten in 5 min | Lambda geeft herhaald fouten |

| Grafana alert | Drempelwaarde | Notificatie |
|---|---|---|
| VG710 Modem Offline | Geen data ≥ 10 min | IRM push + e-mail |

```bash
# Lambda-logs volgen
aws logs tail /aws/lambda/vg710-nmea-downsampler --follow --region eu-north-1
```

## Schaalbaarheid

- **Meerdere apparaten**: automatisch opgepikt. Elk nieuw VG710 dat data naar `vg710-raw/` stuurt, triggert de Lambda en verschijnt in het dashboard.
- **30 apparaten**: Lambda-concurrentie is ingesteld op max 50. S3 en Athena schalen onbeperkt.
- **Latentie**: ≤ 5 minuten na binnenkomst van een bestand is de status zichtbaar in Grafana.
- **Kosten**: Lambda-kosten zijn verwaarloosbaar (ms per bestand). Athena-kosten schalen met datavolume (klein door partitionering op `date_part`).
