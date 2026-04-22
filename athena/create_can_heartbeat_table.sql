-- CAN-bus heartbeat tabel in vg710_db.
-- Bevat per 5-minuutvenster de tellingen per sensorgroep.
-- Gevuld door de Lambda vg710-nmea-downsampler bij elk nieuw CAN-bestand.
-- Eenmalig uitvoeren; daarna automatisch gevuld.

CREATE EXTERNAL TABLE IF NOT EXISTS vg710_db.can_heartbeat (
    device_id     STRING,
    window_start  STRING,
    date_part     STRING,
    sensor_group  STRING,
    message_count INT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ('ignore.malformed.json' = 'true')
STORED AS INPUTFORMAT
    'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://bmc-vg710-raw-eun1/vg710-can-heartbeat/'
TBLPROPERTIES ('has_encrypted_data' = 'false');
