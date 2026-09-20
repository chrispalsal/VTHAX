-- ============================================================
-- VT WiFi Sentinel - Database Setup Script
-- Run this in your Databricks SQL editor to recreate all tables and data.
-- This is standalone - no access to the original workspace needed.
-- ============================================================

-- Step 1: Create catalog and schema (requires CREATE CATALOG privilege)
-- If you can't create a catalog, replace 'vt_connectivity' with an existing one
CREATE CATALOG IF NOT EXISTS vt_connectivity;
CREATE SCHEMA IF NOT EXISTS vt_connectivity.campus;

-- Step 2: Create zone_reference (21 VT campus building zones)
CREATE TABLE IF NOT EXISTS vt_connectivity.campus.zone_reference (
  zone_id STRING, zone_label STRING, building STRING, floor STRING,
  lat DOUBLE, lon DOUBLE, is_indoor BOOLEAN
);

INSERT INTO vt_connectivity.campus.zone_reference
SELECT 'ag_quad' AS zone_id, 'Ag Quad' AS zone_label, NULL AS building, NULL AS floor, 37.225034 AS lat, -80.426832 AS lon, false AS is_indoor
UNION ALL SELECT 'airport_area', 'Airport Research Park', NULL, NULL, 37.227015, -80.415012, false
UNION ALL SELECT 'burruss_hall', 'Burruss Hall', 'Burruss', NULL, 37.229689, -80.423856, false
UNION ALL SELECT 'drillfield', 'Drillfield', NULL, NULL, 37.228500, -80.423500, false
UNION ALL SELECT 'drillfield_bus', 'Drillfield Bus Stop', NULL, NULL, 37.228015, -80.424200, false
UNION ALL SELECT 'engineering_quad', 'Engineering Quad', NULL, NULL, 37.231012, -80.422815, false
UNION ALL SELECT 'goodwin_hall', 'Goodwin Hall', 'Goodwin', NULL, 37.225635, -80.424455, true
UNION ALL SELECT 'hillcrest_hall', 'Hillcrest Hall', NULL, NULL, 37.231523, -80.423812, false
UNION ALL SELECT 'lane_stadium', 'Lane Stadium', NULL, NULL, 37.224212, -80.418531, false
UNION ALL SELECT 'lower_quad', 'Lower Quad', NULL, NULL, 37.226034, -80.426034, false
UNION ALL SELECT 'maroon_bay_1', 'Bus Stop Bay 1', NULL, NULL, 37.232077, -80.424637, false
UNION ALL SELECT 'maroon_bay_5', 'Bus Stop Bay 5', NULL, NULL, 37.231420, -80.424917, false
UNION ALL SELECT 'newman_library', 'Newman Library', 'Newman', NULL, 37.228406, -80.424751, false
UNION ALL SELECT 'owens_hall', 'Owens Hall', NULL, NULL, 37.226529, -80.423784, false
UNION ALL SELECT 'payne_hall', 'Payne Hall', 'Payne', NULL, 37.230531, -80.422258, false
UNION ALL SELECT 'slusher_hall', 'Slusher Hall', NULL, NULL, 37.224897, -80.426815, false
UNION ALL SELECT 'squires_bus', 'Squires Bus Stop', NULL, NULL, 37.227582, -80.425180, false
UNION ALL SELECT 'squires_center', 'Squires Student Center', 'Squires', NULL, 37.227612, -80.424927, false
UNION ALL SELECT 'torgersen_hall', 'Torgersen Hall', 'Torgersen', NULL, 37.228073, -80.421661, false
UNION ALL SELECT 'upper_quad', 'Upper Quad', NULL, NULL, 37.229523, -80.425571, false
UNION ALL SELECT 'vet_med', 'Vet Med', NULL, NULL, 37.222517, -80.429015, false;

-- Step 3: Create outdoor_measurements with mixed real + synthetic data
-- Real bus stop data from CSV measurements + synthetic for all other zones/hours
CREATE TABLE IF NOT EXISTS vt_connectivity.campus.outdoor_measurements AS
WITH hours AS (
  SELECT explode(sequence(7, 23)) AS hour_of_day
),
real_data AS (
  SELECT 'maroon_bay_1' AS zone_id, 'Bus Stop Bay 1' AS zone_label, 37.232077 AS lat, -80.424637 AS lon,
         20 AS hour_of_day, 80.6 AS signal_pct, 78.7 AS bandwidth_mbps, 18.4 AS latency_ms, 10.0 AS jitter_ms, 0.0 AS packet_loss_pct, 'good' AS teams_quality
  UNION ALL SELECT 'maroon_bay_5', 'Bus Stop Bay 5', 37.231420, -80.424917, 20, 83.0, 0.0, 35.4, 10.0, 0.0, 'poor'
  UNION ALL SELECT 'maroon_bay_5', 'Bus Stop Bay 5', 37.231420, -80.424917, 21, 55.0, 59.5, 17.1, 10.0, 0.0, 'good'
),
synthetic AS (
  SELECT
    z.zone_id, z.zone_label, z.lat, z.lon, h.hour_of_day,
    CASE
      WHEN z.is_indoor THEN 60 + rand() * 35
      WHEN ABS(z.lat - 37.2285) < 0.003 AND ABS(z.lon + 80.4235) < 0.003
        THEN 35 + rand() * 50
      ELSE 15 + rand() * 40
    END AS signal_pct,
    CASE
      WHEN rand() < 0.15 THEN 0.0
      ELSE rand() * CASE WHEN z.is_indoor THEN 80 ELSE 60 END
    END AS bandwidth_mbps,
    10 + (100 - CASE WHEN z.is_indoor THEN 75 WHEN ABS(z.lat - 37.2285) < 0.003 THEN 55 ELSE 30 END) * 0.3
         + rand() * 20 AS latency_ms,
    1 + rand() * 14 AS jitter_ms,
    CASE WHEN rand() < 0.7 THEN 0.0 ELSE rand() * 20 END AS packet_loss_pct
  FROM vt_connectivity.campus.zone_reference z
  CROSS JOIN hours h
  LEFT ANTI JOIN real_data r ON z.zone_id = r.zone_id AND h.hour_of_day = r.hour_of_day
),
synthetic_q AS (
  SELECT *,
    CASE
      WHEN bandwidth_mbps >= 1.5 AND latency_ms < 150 AND packet_loss_pct < 1.0 THEN 'good'
      WHEN bandwidth_mbps >= 0.5 AND latency_ms < 300 THEN 'moderate'
      ELSE 'poor'
    END AS teams_quality
  FROM synthetic
)
SELECT * FROM real_data
UNION ALL
SELECT zone_id, zone_label, lat, lon, hour_of_day, signal_pct, bandwidth_mbps, latency_ms, jitter_ms, packet_loss_pct, teams_quality
FROM synthetic_q;

-- Step 4: Create empty bronze tables for the Auto Loader pipeline
-- These get populated when you run pipeline_notebook.py with real CSV uploads
CREATE TABLE IF NOT EXISTS vt_connectivity.campus.bronze_measurements (
  test_id STRING, timestamp STRING, location_name STRING, latitude DOUBLE, longitude DOUBLE,
  source STRING, ssid STRING, bssid STRING, signal_pct INT, estimated_rssi_dbm DOUBLE,
  band STRING, channel STRING, radio_type STRING, rx_link_mbps DOUBLE, tx_link_mbps DOUBLE,
  wifi_interface STRING, adapter_description STRING, ping_host STRING,
  latency_ms DOUBLE, jitter_ms DOUBLE, packet_loss_pct DOUBLE,
  ping_packets_received INT, ping_packets_sent INT,
  download_mbps DOUBLE, upload_mbps DOUBLE,
  zone_id STRING, ingested_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS vt_connectivity.campus.bronze_iperf (
  test_id STRING, timestamp STRING, zone_id STRING,
  bandwidth_mbps DOUBLE, jitter_ms DOUBLE, packet_loss_pct DOUBLE,
  direction STRING, ingested_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS vt_connectivity.campus.silver_measurements (
  zone_id STRING, zone_label STRING, lat DOUBLE, lon DOUBLE,
  signal_pct DOUBLE, bandwidth_mbps DOUBLE, latency_ms DOUBLE,
  jitter_ms DOUBLE, packet_loss_pct DOUBLE,
  download_mbps DOUBLE, upload_mbps DOUBLE,
  hour_of_day INT, teams_quality STRING, ingested_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS vt_connectivity.campus.gold_zone_summary (
  zone_id STRING, building STRING, floor STRING,
  hour_of_day INT, avg_signal_pct DOUBLE, avg_tcp_down_mbps DOUBLE,
  avg_inet_latency_ms DOUBLE, avg_jitter_ms DOUBLE, avg_packet_loss_pct DOUBLE,
  teams_quality STRING, sample_count INT
);

-- Step 5: Create the CSV upload volume
CREATE VOLUME IF NOT EXISTS vt_connectivity.campus.csv_uploads;

-- Step 6 (optional): Grant access to teammates
-- Uncomment and replace emails if you have GRANT permissions
-- GRANT USE SCHEMA ON SCHEMA vt_connectivity.campus TO `teammate@email.edu`;
-- GRANT SELECT ON ALL TABLES IN SCHEMA vt_connectivity.campus TO `teammate@email.edu`;
-- GRANT READ VOLUME ON VOLUME vt_connectivity.campus.csv_uploads TO `teammate@email.edu`;

-- ============================================================
-- Verification queries
-- ============================================================
SELECT 'zone_reference' AS table_name, COUNT(*) AS rows FROM vt_connectivity.campus.zone_reference
UNION ALL SELECT 'outdoor_measurements', COUNT(*) FROM vt_connectivity.campus.outdoor_measurements
UNION ALL SELECT 'bronze_measurements', COUNT(*) FROM vt_connectivity.campus.bronze_measurements
UNION ALL SELECT 'silver_measurements', COUNT(*) FROM vt_connectivity.campus.silver_measurements
UNION ALL SELECT 'gold_zone_summary', COUNT(*) FROM vt_connectivity.campus.gold_zone_summary;

-- Expected output:
-- zone_reference: 21 rows
-- outdoor_measurements: ~357 rows (21 zones x 17 hours)
-- bronze/silver/gold: 0 rows (empty until pipeline runs with real data)
