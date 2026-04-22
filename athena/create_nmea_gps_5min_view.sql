-- Maak de 5-minuten GPS view aan in vg710_db.
-- Voer dit eenmalig uit in de Athena Query Editor.
--
-- Logica: neem de eerste geldige GGA-positie per 5-minuten venster per apparaat.
-- Bron: nmea_gps view → nmea_partitioned (gepartitioneerd op dt + hr).

CREATE OR REPLACE VIEW vg710_db.nmea_gps_5min AS
SELECT
    device_id,
    from_unixtime(floor(to_unixtime(ts) / 300) * 300) AS window_start,
    min_by(latitude,    ts) AS latitude,
    min_by(longitude,   ts) AS longitude,
    min_by(altitude_m,  ts) AS altitude_m,
    min_by(fix_quality, ts) AS fix_quality,
    min_by(num_satellites, ts) AS num_satellites,
    min_by(hdop,        ts) AS hdop,
    min(dt)                 AS dt
FROM vg710_db.nmea_gps
GROUP BY
    device_id,
    floor(to_unixtime(ts) / 300);
