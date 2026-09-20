-- ============================================================
-- VT WiFi Sentinel - Database Setup Script
-- Run this in your Databricks SQL editor to recreate all tables and data.
-- ============================================================

CREATE CATALOG IF NOT EXISTS vt_connectivity;
CREATE SCHEMA IF NOT EXISTS vt_connectivity.campus;

-- Step 1: Create zone_reference (28 VT campus zones with accurate GPS)
CREATE TABLE IF NOT EXISTS vt_connectivity.campus.zone_reference (
  zone_id STRING, zone_label STRING, building STRING, floor STRING,
  lat DOUBLE, lon DOUBLE, is_indoor BOOLEAN
);

INSERT INTO vt_connectivity.campus.zone_reference
SELECT 'maroon_bay_1' AS zone_id, 'Bus Stop Bay 1' AS zone_label, NULL AS building, NULL AS floor, 37.232077 AS lat, -80.424637 AS lon, false AS is_indoor
UNION ALL SELECT 'maroon_bay_5', 'Bus Stop Bay 5', NULL, NULL, 37.231420, -80.424917, false
UNION ALL SELECT 'mccomas_hall', 'McComas Hall', NULL, NULL, 37.220655, -80.421332, false
UNION ALL SELECT 'payne_hall', 'Payne Hall', 'Payne', NULL, 37.225678, -80.419804, false
UNION ALL SELECT 'orange_loop', 'Orange Loop', NULL, NULL, 37.229990, -80.426553, false
UNION ALL SELECT 'burruss_hall', 'Burruss Hall', 'Burruss', NULL, 37.228623, -80.423225, false
UNION ALL SELECT 'eggleston_quad', 'Eggleston Quad', NULL, NULL, 37.227134, -80.419530, false
UNION ALL SELECT 'newman_library', 'Newman Library', 'Newman', NULL, 37.228369, -80.418996, false
UNION ALL SELECT 'drillfield', 'Drillfield', NULL, NULL, 37.227782, -80.422278, false
UNION ALL SELECT 'squires', 'Squires', 'Squires', NULL, 37.229279, -80.417411, false
UNION ALL SELECT 'torgersen_hall', 'Torgersen Hall', 'Torgersen', NULL, 37.229797, -80.420767, false
UNION ALL SELECT 'upper_quad', 'Upper Quad', NULL, NULL, 37.231051, -80.419945, false
UNION ALL SELECT 'dada_decision_sciences', 'Dada and Decision Sciences', NULL, NULL, 37.231249, -80.427444, false
UNION ALL SELECT 'moss_arts_center', 'Moss Arts Center', NULL, NULL, 37.231512, -80.417846, false
UNION ALL SELECT 'goodwin_hall', 'Goodwin Hall', 'Goodwin', NULL, 37.232110, -80.425480, true
UNION ALL SELECT 'pamplin_hall', 'Pamplin Hall', 'Pamplin', NULL, 37.228115, -80.424900, false
UNION ALL SELECT 'rec_sports', 'Rec Sports Field House', NULL, NULL, 37.215252, -80.418961, false
UNION ALL SELECT 'english_field', 'English Field', NULL, NULL, 37.218447, -80.424554, false
UNION ALL SELECT 'duck_pond_lot', 'Duck Pond Lot', NULL, NULL, 37.220611, -80.428841, false
UNION ALL SELECT 'litton_reaves', 'Litton Reaves', NULL, NULL, 37.221924, -80.423678, false
UNION ALL SELECT 'hutcheson_hall', 'Hutcheson Hall', NULL, NULL, 37.225348, -80.423706, false
UNION ALL SELECT 'lane_stadium', 'Lane Stadium', NULL, NULL, 37.219816, -80.418000, false
UNION ALL SELECT 'ag_quad', 'Ag Quad', NULL, NULL, 37.225935, -80.417331, false
UNION ALL SELECT 'slusher_quad', 'Slusher Quad', NULL, NULL, 37.225700, -80.421762, false
UNION ALL SELECT 'vet_med', 'Vet Med', NULL, NULL, 37.217654, -80.426913, false
UNION ALL SELECT 'duck_pond', 'Duck Pond', NULL, NULL, 37.220611, -80.428841, false
UNION ALL SELECT 'dietrick_quad', 'Dietrick Quad', NULL, NULL, 37.223793, -80.420248, false
UNION ALL SELECT 'pritchard_quad', 'Pritchard Quad', NULL, NULL, 37.224927, -80.419078, false;

-- Step 2: Create outdoor_measurements with calibrated synthetic data
CREATE TABLE IF NOT EXISTS vt_connectivity.campus.outdoor_measurements AS
WITH hours AS (SELECT explode(sequence(7, 23)) AS hour_of_day),
real_data AS (
  SELECT 'maroon_bay_1' AS zone_id, 'Bus Stop Bay 1' AS zone_label, 37.232077 AS lat, -80.424637 AS lon, 20 AS hour_of_day, 80.6 AS signal_pct, 78.7 AS bandwidth_mbps, 18.4 AS latency_ms, 10.0 AS jitter_ms, 0.0 AS packet_loss_pct, 'good' AS teams_quality
  UNION ALL SELECT 'maroon_bay_5', 'Bus Stop Bay 5', 37.231420, -80.424917, 20, 83.0, 0.0, 35.4, 10.0, 0.0, 'poor'
  UNION ALL SELECT 'maroon_bay_5', 'Bus Stop Bay 5', 37.231420, -80.424917, 21, 55.0, 59.5, 17.1, 10.0, 0.0, 'good'
),
synthetic AS (
  SELECT z.zone_id, z.zone_label, z.lat, z.lon, h.hour_of_day,
    CASE WHEN z.is_indoor THEN 60 + rand() * 35 WHEN ABS(z.lat - 37.2278) < 0.003 AND ABS(z.lon + 80.4223) < 0.003 THEN 35 + rand() * 50 ELSE 15 + rand() * 55 END AS signal_pct,
    CASE WHEN rand() < 0.15 THEN 0.0 ELSE rand() * 120 END AS bandwidth_mbps,
    10 + rand() * 25 AS latency_ms, 1 + rand() * 30 AS jitter_ms,
    CASE WHEN rand() < 0.7 THEN 0.0 ELSE rand() * 20 END AS packet_loss_pct
  FROM vt_connectivity.campus.zone_reference z CROSS JOIN hours h
  LEFT ANTI JOIN real_data r ON z.zone_id = r.zone_id AND h.hour_of_day = r.hour_of_day
),
synthetic_q AS (
  SELECT *, CASE WHEN bandwidth_mbps >= 1.5 AND latency_ms < 150 AND packet_loss_pct < 1.0 THEN 'good' WHEN bandwidth_mbps >= 0.5 AND latency_ms < 300 THEN 'moderate' ELSE 'poor' END AS teams_quality FROM synthetic
)
SELECT * FROM real_data
UNION ALL
SELECT zone_id, zone_label, lat, lon, hour_of_day, signal_pct, bandwidth_mbps, latency_ms, jitter_ms, packet_loss_pct, teams_quality FROM synthetic_q;

-- Step 3: Create empty bronze tables for pipeline
CREATE TABLE IF NOT EXISTS vt_connectivity.campus.bronze_measurements (
  test_id STRING, timestamp STRING, location_name STRING, latitude DOUBLE, longitude DOUBLE,
  source STRING, ssid STRING, bssid STRING, signal_pct INT, estimated_rssi_dbm DOUBLE,
  band STRING, channel STRING, radio_type STRING, rx_link_mbps DOUBLE, tx_link_mbps DOUBLE,
  wifi_interface STRING, adapter_description STRING, ping_host STRING,
  latency_ms DOUBLE, jitter_ms DOUBLE, packet_loss_pct DOUBLE,
  ping_packets_received INT, ping_packets_sent INT,
  download_mbps DOUBLE, upload_mbps DOUBLE, zone_id STRING, ingested_at TIMESTAMP
);
CREATE TABLE IF NOT EXISTS vt_connectivity.campus.bronze_iperf (test_id STRING, timestamp STRING, zone_id STRING, bandwidth_mbps DOUBLE, jitter_ms DOUBLE, packet_loss_pct DOUBLE, direction STRING, ingested_at TIMESTAMP);
CREATE TABLE IF NOT EXISTS vt_connectivity.campus.silver_measurements (zone_id STRING, zone_label STRING, lat DOUBLE, lon DOUBLE, signal_pct DOUBLE, bandwidth_mbps DOUBLE, latency_ms DOUBLE, jitter_ms DOUBLE, packet_loss_pct DOUBLE, download_mbps DOUBLE, upload_mbps DOUBLE, hour_of_day INT, teams_quality STRING, ingested_at TIMESTAMP);
CREATE TABLE IF NOT EXISTS vt_connectivity.campus.gold_zone_summary (zone_id STRING, building STRING, floor STRING, hour_of_day INT, avg_signal_pct DOUBLE, avg_tcp_down_mbps DOUBLE, avg_inet_latency_ms DOUBLE, avg_jitter_ms DOUBLE, avg_packet_loss_pct DOUBLE, teams_quality STRING, sample_count INT);
CREATE VOLUME IF NOT EXISTS vt_connectivity.campus.csv_uploads;

SELECT 'zone_reference' AS table_name, COUNT(*) AS rows FROM vt_connectivity.campus.zone_reference
UNION ALL SELECT 'outdoor_measurements', COUNT(*) FROM vt_connectivity.campus.outdoor_measurements;
