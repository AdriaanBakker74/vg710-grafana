-- nmea_5min tabel in vg710_db.
-- Bevat pre-computed 5-minuten GPS posities, geschreven door de Lambda downsampler.
-- Eenmalig uitvoeren. Daarna automatisch gevuld via Lambda bij elk nieuw NMEA bestand.

CREATE EXTERNAL TABLE IF NOT EXISTS vg710_db.nmea_5min (
    device_id    STRING,
    ts           STRING,
    window_start STRING,
    date_part    STRING,
    lat          DOUBLE,
    lon          DOUBLE,
    altitude     DOUBLE,
    fix_quality  INT,
    fix_label    STRING,
    satellites   INT,
    hdop         DOUBLE
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ('ignore.malformed.json' = 'true')
STORED AS INPUTFORMAT
    'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://bmc-vg710-raw-eun1/vg710-5min/'
TBLPROPERTIES ('has_encrypted_data' = 'false');
