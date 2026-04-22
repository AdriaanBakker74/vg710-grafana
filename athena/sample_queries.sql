-- ============================================================
-- GPS TRACK — 5-minuten (voor Grafana Geomap panel)
-- Database:  vg710_db
-- View:      nmea_gps_5min
--
-- $__from / $__to zijn Grafana tijdvariabelen (Unix milliseconden).
-- dt-filter zorgt voor partitie-pruning → snelle query.
-- ============================================================

SELECT
    device_id,
    window_start                    AS time,
    latitude,
    longitude,
    altitude_m,
    fix_quality,
    num_satellites,
    hdop
FROM vg710_db.nmea_gps_5min
WHERE
    window_start >= from_unixtime($__from / 1000)
    AND window_start <  from_unixtime($__to   / 1000)
    AND dt >= date_format(from_unixtime($__from / 1000), '%Y-%m-%d')
    AND dt <= date_format(from_unixtime($__to   / 1000), '%Y-%m-%d')
ORDER BY window_start;


-- ============================================================
-- RUWE GPS DATA — download tabel (Table panel met CSV-knop)
-- Database:  vg710_db
-- View:      nmea_gps (1-seconde data)
-- ============================================================

SELECT
    device_id,
    ts                              AS time,
    round(latitude,  6)             AS lat,
    round(longitude, 6)             AS lon,
    altitude_m,
    fix_quality,
    num_satellites,
    hdop
FROM vg710_db.nmea_gps
WHERE
    ts >= from_unixtime($__from / 1000)
    AND ts <  from_unixtime($__to   / 1000)
    AND dt >= date_format(from_unixtime($__from / 1000), '%Y-%m-%d')
    AND dt <= date_format(from_unixtime($__to   / 1000), '%Y-%m-%d')
ORDER BY ts;


-- ============================================================
-- CONTROLE: aantal posities per dag
-- ============================================================

SELECT
    dt                              AS dag,
    device_id,
    COUNT(*)                        AS posities_5min
FROM vg710_db.nmea_gps_5min
GROUP BY dt, device_id
ORDER BY dt DESC;
