# VG710 Grafana Dashboard

Grafana dashboard voor GPS-tracking van VG710 industriële routers via AWS S3 en Athena.

## Architectuur

```
VG710 router
    │
    ▼ NMEA (GGA) bestanden, ~30 seconden per bestand
S3: bmc-vg710-raw-eun1/vg710-raw/{device_id}/nmea/{datum}/{uur}/{ts}.ndjson
    │
    ▼ EventBridge (S3 ObjectCreated)
Lambda: vg710-nmea-downsampler
    │  · Parseert GGA-regels (lat/lon/hoogte/satellieten/HDOP)
    │  · Groepeert op 5-minuutvensters
    │  · Schrijft één bestand per venster (idempotent)
    ▼
S3: bmc-vg710-raw-eun1/vg710-5min/{device_id}/{datum}/{venster}.ndjson
    │
    ▼ Athena externe tabel
vg710_db.nmea_5min
    │
    ▼ Grafana Cloud (Amazon Athena datasource)
Dashboard: VG710 GPS Track
```

**Ruwe 1Hz-data** (12+ miljoen records) blijft intact in `vg710-raw/` voor download.
Het dashboard gebruikt uitsluitend de lichte `nmea_5min` dataset (~42.000 records per apparaat per jaar).

## Bestanden

```
deploy.sh                          Deploymentscript (AWS CLI)
lambda/
  downsample_nmea/handler.py       Lambda-functie: NMEA → 5-min posities
backfill/
  backfill_nmea.py                 Eenmalig backfill-script voor historische data
athena/
  create_nmea_5min_table.sql       Athena-tabel voor de 5-min dataset
  create_nmea_gps_5min_view.sql    Alternatieve view op nmea_partitioned (ongebruikt)
  sample_queries.sql               Voorbeeldquery's voor Grafana
grafana/
  dashboard.json                   Grafana dashboard (VG710 GPS Track)
cloudformation/
  stack.yaml                       CloudFormation-referentie (niet gedeployed)
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
| Athena resultaten | `bmc-vg710-athena-results-eun1` |

## S3-structuur

```
bmc-vg710-raw-eun1/
├── vg710-raw/
│   └── {device_id}/
│       └── nmea/
│           └── {YYYY-MM-DD}/
│               └── {HH}/
│                   └── {YYYY-MM-DDThh-mm-ss.xxxxxx+00-00}.ndjson   ← ruwe 1Hz NMEA
├── vg710-5min/
│   └── {device_id}/
│       └── {YYYY-MM-DD}/
│           └── {YYYY-MM-DDThh-mm-ss}.ndjson   ← één bestand per 5-minuutvenster
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
| `fix_quality` | INT | GGA fix quality (0=geen fix, 1=GPS, 2=DGPS, 4=RTK) |
| `fix_label` | STRING | Leesbaar label voor fix_quality |
| `satellites` | INT | Aantal gebruikte satellieten |
| `hdop` | DOUBLE | Horizontal Dilution of Precision (lager = nauwkeuriger) |

## Deployment

### Vereisten

- AWS CLI geconfigureerd met toegang tot `eu-north-1`
- Rechten voor: Lambda, IAM, S3, SQS, EventBridge, CloudWatch, Athena
- Python 3 met `boto3` voor de backfill

### Eerste keer deployen

```bash
# Stap 1: Infrastructuur aanmaken en Lambda deployen
./deploy.sh

# Stap 2: Historische data verwerken (eenmalig)
cd backfill
python3 backfill_nmea.py --workers 50

# Optioneel: alleen een bepaalde periode
python3 backfill_nmea.py --date-from 2026-04-01 --date-to 2026-04-13 --workers 50

# Simulatie zonder te schrijven
python3 backfill_nmea.py --dry-run
```

### Lambda bijwerken

```bash
# Alleen de Lambda-code bijwerken (infrastructuur ongewijzigd)
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

### Grafana dashboard importeren

```bash
# Dashboard uploaden/bijwerken via API
curl -X POST \
  -H "Authorization: Bearer <API-TOKEN>" \
  -H "Content-Type: application/json" \
  -d @grafana/dashboard.json \
  "https://bmcconsultancy.grafana.net/api/dashboards/db"
```

## Backfill

De backfill verwerkt alle bestaande NMEA-bestanden in S3 en schrijft de 5-min dataset.
Kenmerken:

- **Slim overslaan**: de `window_start` wordt berekend uit de bestandsnaam (geen download nodig). Als het uitvoerbestand al bestaat → overgeslagen zonder S3 GET.
- **Hervattend**: na onderbreking veilig opnieuw te starten; reeds verwerkte vensters worden overgeslagen.
- **Parallellisme**: 50 workers (instelbaar). Één S3-client per thread voor maximale doorvoer.
- **Geschatte snelheid**: ~300 bestanden/s → 881.000 bestanden in ~50 minuten.

```bash
# Hervatten na onderbreking (skip-existing is standaard aan)
python3 backfill/backfill_nmea.py --workers 50

# Opnieuw verwerken (bestaande bestanden overschrijven)
python3 backfill/backfill_nmea.py --no-skip-existing --workers 50
```

## Athena-query's

### Alle posities in een periode

```sql
SELECT window_start, lat, lon, altitude, satellites, hdop
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

### Actieve apparaten vandaag

```sql
SELECT DISTINCT device_id
FROM vg710_db.nmea_5min
WHERE date_part = date_format(current_date, '%Y-%m-%d');
```

## Monitoring

| CloudWatch-alarm | Drempelwaarde | Betekenis |
|---|---|---|
| `vg710-nmea-downsampler-dlq-niet-leeg` | ≥ 1 bericht | Lambda-aanroep mislukt na 3 pogingen |
| `vg710-nmea-downsampler-fouten` | ≥ 5 fouten in 5 min | Lambda geeft herhaald fouten |

Logs bekijken:

```bash
aws logs tail /aws/lambda/vg710-nmea-downsampler --follow --region eu-north-1
```

## Schaalbaarheid

- **Meerdere apparaten**: automatisch opgepikt. Elk nieuw VG710 dat data naar `vg710-raw/` stuurt, triggert de Lambda en verschijnt in het dashboard.
- **30 apparaten**: Lambda-concurrentie is ingesteld op max 50. S3 en Athena schalen onbeperkt.
- **Latentie**: ≤ 5 minuten na binnenkomst van een NMEA-bestand is de positie zichtbaar in Grafana.
- **Kosten**: Lambda-kosten zijn verwaarloosbaar (ms per bestand). Athena-kosten schalen met datavolume in `nmea_5min` (klein).
