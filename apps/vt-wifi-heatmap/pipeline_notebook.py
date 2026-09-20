# Databricks notebook source
# DBTITLE 1,VT Connectivity Pipeline
# VT Campus WiFi Connectivity Heatmap

## Project: Virginia Tech eduroam Connectivity Monitoring

This notebook implements the full data pipeline:
1. **Bronze layer** - Raw CSV ingestion from `collector.py` (iPerf3 + passive ping logger)
2. **Silver layer** - Cleaned, typed, enriched with building/zone mapping + Teams suitability
3. **Gold layer** - Zone-level aggregations by hour with Teams quality classification
4. **ML Model** - Classifies zones as good/moderate/poor for video calls
5. **Agent queries** - Functions the AI agent uses to recommend best connectivity zones

### Collector CSV Schema
| Table | Description |
|-------|-------------|
| `measurements` | Passive light ticks (signal, ping to gateway + internet) |
| `iperf` | Heavy bandwidth tests (TCP up/down, UDP up) |
| `scans` | Nearby AP scans |
| `taps` | Checkpoint markers |

# COMMAND ----------

# DBTITLE 1,Step 1: UC Infrastructure
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- Step 1: Unity Catalog Infrastructure
# MAGIC -- ============================================================
# MAGIC
# MAGIC CREATE CATALOG IF NOT EXISTS vt_connectivity
# MAGIC   COMMENT 'Virginia Tech campus WiFi connectivity heatmap project';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS vt_connectivity.campus
# MAGIC   COMMENT 'Campus connectivity data';
# MAGIC
# MAGIC CREATE VOLUME IF NOT EXISTS vt_connectivity.campus.csv_uploads
# MAGIC   COMMENT 'Raw CSV files uploaded by the eduroam collector script';
# MAGIC
# MAGIC -- Bronze tables (match collector.py CSV schemas exactly)
# MAGIC CREATE TABLE IF NOT EXISTS vt_connectivity.campus.bronze_measurements (
# MAGIC   ts_utc STRING, device_id STRING, session_id STRING, ssid STRING, bssid STRING,
# MAGIC   signal_pct INT, rssi_est_dbm DOUBLE, channel INT, band STRING, radio_type STRING,
# MAGIC   rx_rate_mbps DOUBLE, tx_rate_mbps DOUBLE, gw_ip STRING,
# MAGIC   gw_latency_ms DOUBLE, gw_jitter_ms DOUBLE, gw_loss_pct DOUBLE,
# MAGIC   inet_latency_ms DOUBLE, inet_jitter_ms DOUBLE, inet_loss_pct DOUBLE,
# MAGIC   last_checkpoint STRING, next_checkpoint STRING, note STRING,
# MAGIC   _ingested_at TIMESTAMP, _source_file STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS vt_connectivity.campus.bronze_iperf (
# MAGIC   ts_utc STRING, device_id STRING, session_id STRING, test STRING,
# MAGIC   server_ip STRING, server_port INT, seconds INT, ok BOOLEAN,
# MAGIC   mbps DOUBLE, retransmits INT, jitter_ms DOUBLE, lost_pct DOUBLE,
# MAGIC   lost_packets INT, packets INT, error STRING,
# MAGIC   bssid STRING, signal_pct INT, last_checkpoint STRING,
# MAGIC   _ingested_at TIMESTAMP, _source_file STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS vt_connectivity.campus.bronze_scans (
# MAGIC   ts_utc STRING, device_id STRING, session_id STRING, ssid STRING, bssid STRING,
# MAGIC   signal_pct INT, rssi_est_dbm DOUBLE, channel INT, band STRING, radio_type STRING,
# MAGIC   _ingested_at TIMESTAMP, _source_file STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS vt_connectivity.campus.bronze_taps (
# MAGIC   ts_utc STRING, device_id STRING, session_id STRING,
# MAGIC   checkpoint_id STRING, source STRING,
# MAGIC   _ingested_at TIMESTAMP, _source_file STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC -- Silver table (cleaned, typed, enriched)
# MAGIC CREATE TABLE IF NOT EXISTS vt_connectivity.campus.silver_measurements (
# MAGIC   ts_utc TIMESTAMP, device_id STRING, session_id STRING, ssid STRING, bssid STRING,
# MAGIC   signal_pct INT, rssi_est_dbm DOUBLE, channel INT, band STRING, radio_type STRING,
# MAGIC   rx_rate_mbps DOUBLE, tx_rate_mbps DOUBLE, gw_ip STRING,
# MAGIC   gw_latency_ms DOUBLE, gw_jitter_ms DOUBLE, gw_loss_pct DOUBLE,
# MAGIC   inet_latency_ms DOUBLE, inet_jitter_ms DOUBLE, inet_loss_pct DOUBLE,
# MAGIC   checkpoint STRING, building STRING, floor STRING, zone_id STRING,
# MAGIC   teams_suitable BOOLEAN, _ingested_at TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC -- Gold table (zone-level aggregations)
# MAGIC CREATE TABLE IF NOT EXISTS vt_connectivity.campus.gold_zone_summary (
# MAGIC   zone_id STRING, building STRING, floor STRING, hour_of_day INT,
# MAGIC   measurement_count INT, avg_signal_pct DOUBLE, avg_rssi_dbm DOUBLE,
# MAGIC   avg_gw_latency_ms DOUBLE, avg_inet_latency_ms DOUBLE, avg_jitter_ms DOUBLE,
# MAGIC   avg_packet_loss_pct DOUBLE, avg_tcp_down_mbps DOUBLE, avg_tcp_up_mbps DOUBLE,
# MAGIC   teams_quality STRING, last_updated TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC SELECT 'Infrastructure ready' AS status;

# COMMAND ----------

# DBTITLE 1,Step 2: Synthetic Data Generation
import random
import datetime as dt
from pyspark.sql import SparkSession
from pyspark.sql.types import *

# ============================================================
# Step 2: Synthetic Data Generation (Torgersen Hall)
# ============================================================
# Generates realistic WiFi connectivity data for 10 zones across
# 4 floors of Torgersen Hall over 3 days, with time-of-day congestion.

zones = [
    {"checkpoint": "torg_f1_lobby",   "building": "Torgersen", "floor": "1", "base_signal": 75, "base_latency": 8,  "base_jitter": 2,  "base_loss": 0.5, "base_mbps": 45},
    {"checkpoint": "torg_f1_cafe",    "building": "Torgersen", "floor": "1", "base_signal": 65, "base_latency": 12, "base_jitter": 4,  "base_loss": 1.0, "base_mbps": 30},
    {"checkpoint": "torg_f2_lab",     "building": "Torgersen", "floor": "2", "base_signal": 80, "base_latency": 6,  "base_jitter": 1,  "base_loss": 0.2, "base_mbps": 55},
    {"checkpoint": "torg_f2_hall",    "building": "Torgersen", "floor": "2", "base_signal": 45, "base_latency": 25, "base_jitter": 8,  "base_loss": 3.0, "base_mbps": 12},
    {"checkpoint": "torg_f3_bridge",  "building": "Torgersen", "floor": "3", "base_signal": 85, "base_latency": 5,  "base_jitter": 1,  "base_loss": 0.1, "base_mbps": 60},
    {"checkpoint": "torg_f3_class_a", "building": "Torgersen", "floor": "3", "base_signal": 55, "base_latency": 15, "base_jitter": 5,  "base_loss": 1.5, "base_mbps": 22},
    {"checkpoint": "torg_f3_class_b" "building": "Torgersen", "floor": "3", "base_signal": 35, "base_latency": 40, "base_jitter": 15, "base_loss": 5.0, "base_mbps": 5},
    {"checkpoint": "torg_f4_study",   "building": "Torgersen", "floor": "4", "base_signal": 70, "base_latency": 10, "base_jitter": 3,  "base_loss": 0.8, "base_mbps": 38},
    {"checkpoint": "torg_f4_corner",  "building": "Torgersen", "floor": "4", "base_signal": 50, "base_latency": 20, "base_jitter": 7,  "base_loss": 2.5, "base_mbps": 15},
    {"checkpoint": "torg_f4_stairs",  "building": "Torgersen", "floor": "4", "base_signal": 25, "base_latency": 55, "base_jitter": 20, "base_loss": 8.0, "base_mbps": 2},
]

def congestion_factor(hour):
    if 8 <= hour <= 10:  return 1.4
    if 11 <= hour <= 13: return 1.3
    if 14 <= hour <= 16: return 1.5
    if 17 <= hour <= 19: return 1.2
    if 20 <= hour <= 22: return 1.0
    return 0.7

random.seed(42)
spark = SparkSession.builder.getOrCreate()

# Check if data already exists
existing = spark.sql("SELECT COUNT(*) as cnt FROM vt_connectivity.campus.bronze_measurements").collect()[0].cnt
if existing == 0:
    measurements, iperf_rows, scans, taps = [], [], [], []
    bssid_pool = [f"02:00:00:00:{i:02x}:01" for i in range(20)]
    current = dt.datetime(2026, 9, 17, 7, 0, 0)
    end_date = dt.datetime(2026, 9, 19, 23, 0, 0)
    session_id, device_id = "20260917-070000", "win1"

    while current < end_date:
        hour = current.hour
        cong = congestion_factor(hour)
        for zone in zones:
            if hour < 7 or hour > 23:
                continue
            signal = max(5, min(100, int(zone["base_signal"] * (1.0 / cong) + random.gauss(0, 5))))
            rssi = round(signal / 2 - 100, 1)
            latency = max(1, zone["base_latency"] * cong + random.gauss(0, 2))
            jitter = max(0.1, zone["base_jitter"] * cong + random.gauss(0, 1))
            loss = max(0.0, min(100, zone["base_loss"] * cong + random.gauss(0, 0.3)))
            bssid = random.choice(bssid_pool)
            channel = random.choice([36, 40, 44, 48, 149, 153, 157, 161])
            rx_rate = max(1, zone["base_mbps"] * (signal/100) * (1.0/cong) + random.gauss(0, 5))
            tx_rate = max(1, rx_rate * 0.7 + random.gauss(0, 3))
            ts_str = current.isoformat(timespec="milliseconds")
            measurements.append((ts_str, device_id, session_id, "eduroam", bssid, signal, rssi,
                channel, "5 GHz", "802.11ac", round(rx_rate, 2), round(tx_rate, 2),
                "10.110.205.1", round(latency, 2), round(jitter, 2), round(loss, 1),
                round(latency + 5, 2), round(jitter + 1, 2), round(loss + 0.2, 1),
                zone["checkpoint"], None, None if loss < 5 else "high loss", current, "synthetic.csv"))
            if current.minute % 5 == 0:
                base_mbps = zone["base_mbps"] * (signal/100) * (1.0/cong)
                for test_name, mbps_base in [("tcp_up", base_mbps*0.6), ("tcp_down", base_mbps), ("udp_up", min(base_mbps*0.3, 3))]:
                    iperf_mbps = max(0.5, mbps_base + random.gauss(0, 3))
                    iperf_rows.append((ts_str, device_id, session_id, test_name, "10.110.205.57", 5201,
                        3 if "tcp" in test_name else 5, iperf_mbps > 1.0, round(iperf_mbps, 2),
                        random.randint(0, 50), round(jitter, 3), round(loss, 2),
                        random.randint(0, 10), random.randint(100, 200),
                        None if iperf_mbps > 1 else "timeout", bssid, signal, zone["checkpoint"], current, "synthetic.csv"))
            if current.minute % 30 == 0:
                for _ in range(random.randint(3, 6)):
                    ss = max(5, min(100, signal + random.gauss(0, 15)))
                    scans.append((ts_str, device_id, session_id, "eduroam", random.choice(bssid_pool),
                        int(ss), round(ss/2-100, 1), random.choice([36,40,44,48,149,153]), "5 GHz", "802.11ac", current, "synthetic.csv"))
        if current.minute == 0:
            for zone in zones:
                taps.append((current.isoformat(timespec="milliseconds"), device_id, session_id, zone["checkpoint"], "scheduled", current, "synthetic.csv"))
        current += dt.timedelta(seconds=30)

    m_schema = StructType([StructField(f"ts_utc", StringType()), StructField("device_id", StringType()), StructField("session_id", StringType()), StructField("ssid", StringType()), StructField("bssid", StringType()), StructField("signal_pct", IntegerType()), StructField("rssi_est_dbm", DoubleType()), StructField("channel", IntegerType()), StructField("band", StringType()), StructField("radio_type", StringType()), StructField("rx_rate_mbps", DoubleType()), StructField("tx_rate_mbps", DoubleType()), StructField("gw_ip", StringType()), StructField("gw_latency_ms", DoubleType()), StructField("gw_jitter_ms", DoubleType()), StructField("gw_loss_pct", DoubleType()), StructField("inet_latency_ms", DoubleType()), StructField("inet_jitter_ms", DoubleType()), StructField("inet_loss_pct", DoubleType()), StructField("last_checkpoint", StringType()), StructField("next_checkpoint", StringType()), StructField("note", StringType()), StructField("_ingested_at", TimestampType()), StructField("_source_file", StringType())])
    spark.createDataFrame(measurements, m_schema).write.mode("append").saveAsTable("vt_connectivity.campus.bronze_measurements")
    i_schema = StructType([StructField("ts_utc", StringType()), StructField("device_id", StringType()), StructField("session_id", StringType()), StructField("test", StringType()), StructField("server_ip", StringType()), StructField("server_port", IntegerType()), StructField("seconds", IntegerType()), StructField("ok", BooleanType()), StructField("mbps", DoubleType()), StructField("retransmits", IntegerType()), StructField("jitter_ms", DoubleType()), StructField("lost_pct", DoubleType()), StructField("lost_packets", IntegerType()), StructField("packets", IntegerType()), StructField("error", StringType()), StructField("bssid", StringType()), StructField("signal_pct", IntegerType()), StructField("last_checkpoint", StringType()), StructField("_ingested_at", TimestampType()), StructField("_source_file", StringType())])
    spark.createDataFrame(iperf_rows, i_schema).write.mode("append").saveAsTable("vt_connectivity.campus.bronze_iperf")
    s_schema = StructType([StructField("ts_utc", StringType()), StructField("device_id", StringType()), StructField("session_id", StringType()), StructField("ssid", StringType()), StructField("bssid", StringType()), StructField("signal_pct", IntegerType()), StructField("rssi_est_dbm", DoubleType()), StructField("channel", IntegerType()), StructField("band", StringType()), StructField("radio_type", StringType()), StructField("_ingested_at", TimestampType()), StructField("_source_file", StringType())])
    spark.createDataFrame(scans, s_schema).write.mode("append").saveAsTable("vt_connectivity.campus.bronze_scans")
    t_schema = StructType([StructField("ts_utc", StringType()), StructField("device_id", StringType()), StructField("session_id", StringType()), StructField("checkpoint_id", StringType()), StructField("source", StringType()), StructField("_ingested_at", TimestampType()), StructField("_source_file", StringType())])
    spark.createDataFrame(taps, t_schema).write.mode("append").saveAsTable("vt_connectivity.campus.bronze_taps")
    print(f"Generated {len(measurements)} measurements, {len(iperf_rows)} iperf, {len(scans)} scans, {len(taps)} taps")
else:
    print(f"Data already exists: {existing} rows in bronze_measurements")

# COMMAND ----------

# DBTITLE 1,Step 3: Bronze to Silver
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- Step 3: Bronze → Silver Transformation
# MAGIC -- ============================================================
# MAGIC
# MAGIC INSERT OVERWRITE vt_connectivity.campus.silver_measurements
# MAGIC SELECT
# MAGIC   CAST(ts_utc AS TIMESTAMP) AS ts_utc,
# MAGIC   device_id, session_id, ssid, bssid, signal_pct, rssi_est_dbm,
# MAGIC   channel, band, radio_type, rx_rate_mbps, tx_rate_mbps, gw_ip,
# MAGIC   gw_latency_ms, gw_jitter_ms, gw_loss_pct,
# MAGIC   inet_latency_ms, inet_jitter_ms, inet_loss_pct,
# MAGIC   last_checkpoint AS checkpoint,
# MAGIC   SPLIT(last_checkpoint, '_')[0] AS building,
# MAGIC   SPLIT(last_checkpoint, '_')[1] AS floor,
# MAGIC   last_checkpoint AS zone_id,
# MAGIC   -- Teams: >=1.5 Mbps down, <150ms latency, <1% packet loss
# MAGIC   CASE WHEN rx_rate_mbps >= 1.5
# MAGIC          AND COALESCE(inet_latency_ms, 999) < 150
# MAGIC          AND COALESCE(inet_loss_pct, 100) < 1.0
# MAGIC        THEN true ELSE false END AS teams_suitable,
# MAGIC   current_timestamp() AS _ingested_at
# MAGIC FROM vt_connectivity.campus.bronze_measurements
# MAGIC WHERE last_checkpoint IS NOT NULL;
# MAGIC
# MAGIC SELECT COUNT(*) AS silver_rows FROM vt_connectivity.campus.silver_measurements;

# COMMAND ----------

# DBTITLE 1,Step 4: Silver to Gold
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- Step 4: Silver → Gold (Zone Aggregations)
# MAGIC -- ============================================================
# MAGIC
# MAGIC INSERT OVERWRITE vt_connectivity.campus.gold_zone_summary
# MAGIC SELECT
# MAGIC   s.zone_id, s.building, s.floor,
# MAGIC   HOUR(s.ts_utc) AS hour_of_day,
# MAGIC   COUNT(*) AS measurement_count,
# MAGIC   AVG(s.signal_pct) AS avg_signal_pct,
# MAGIC   AVG(s.rssi_est_dbm) AS avg_rssi_dbm,
# MAGIC   AVG(s.gw_latency_ms) AS avg_gw_latency_ms,
# MAGIC   AVG(s.inet_latency_ms) AS avg_inet_latency_ms,
# MAGIC   AVG(COALESCE(s.gw_jitter_ms, s.inet_jitter_ms)) AS avg_jitter_ms,
# MAGIC   AVG(COALESCE(s.inet_loss_pct, s.gw_loss_pct)) AS avg_packet_loss_pct,
# MAGIC   AVG(CASE WHEN i.test = 'tcp_down' THEN i.mbps END) AS avg_tcp_down_mbps,
# MAGIC   AVG(CASE WHEN i.test = 'tcp_up' THEN i.mbps END) AS avg_tcp_up_mbps,
# MAGIC   CASE
# MAGIC     WHEN AVG(CASE WHEN i.test = 'tcp_down' THEN i.mbps END) >= 1.5
# MAGIC      AND AVG(s.inet_latency_ms) < 150
# MAGIC      AND AVG(COALESCE(s.inet_loss_pct, s.gw_loss_pct)) < 1.0
# MAGIC     THEN 'good'
# MAGIC     WHEN AVG(CASE WHEN i.test = 'tcp_down' THEN i.mbps END) >= 0.5
# MAGIC      AND AVG(s.inet_latency_ms) < 300
# MAGIC     THEN 'moderate'
# MAGIC     ELSE 'poor'
# MAGIC   END AS teams_quality,
# MAGIC   MAX(s.ts_utc) AS last_updated
# MAGIC FROM vt_connectivity.campus.silver_measurements s
# MAGIC LEFT JOIN (
# MAGIC   SELECT CAST(ts_utc AS TIMESTAMP) AS ts_utc, last_checkpoint, test, mbps
# MAGIC   FROM vt_connectivity.campus.bronze_iperf WHERE ok = true
# MAGIC ) i ON s.zone_id = i.last_checkpoint
# MAGIC     AND ABS(TIMESTAMPDIFF(MINUTE, s.ts_utc, i.ts_utc)) <= 3
# MAGIC GROUP BY s.zone_id, s.building, s.floor, HOUR(s.ts_utc);
# MAGIC
# MAGIC -- Show zone quality summary
# MAGIC SELECT zone_id, floor, hour_of_day,
# MAGIC   ROUND(avg_signal_pct, 1) AS signal_pct,
# MAGIC   ROUND(avg_inet_latency_ms, 1) AS latency_ms,
# MAGIC   ROUND(avg_tcp_down_mbps, 1) AS down_mbps,
# MAGIC   teams_quality
# MAGIC FROM vt_connectivity.campus.gold_zone_summary
# MAGIC ORDER BY zone_id, hour_of_day
# MAGIC LIMIT 20;

# COMMAND ----------

# DBTITLE 1,Step 5: Train ML Model
import mlflow
import mlflow.spark
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# ============================================================
# Step 5: Train Connectivity Quality Model
# ============================================================
# Predicts whether a zone supports a Teams video call

# Prepare training data from silver + iperf
train_df = spark.sql("""
SELECT
  s.signal_pct, s.rssi_est_dbm, s.gw_latency_ms, s.inet_latency_ms,
  s.gw_jitter_ms, s.inet_jitter_ms, s.gw_loss_pct, s.inet_loss_pct,
  s.rx_rate_mbps, s.tx_rate_mbps, HOUR(s.ts_utc) AS hour_of_day,
  CASE WHEN s.teams_suitable = true THEN 1.0 ELSE 0.0 END AS label
FROM vt_connectivity.campus.silver_measurements s
WHERE s.signal_pct IS NOT NULL AND s.inet_latency_ms IS NOT NULL
""")

feature_cols = ["signal_pct", "rssi_est_dbm", "gw_latency_ms", "inet_latency_ms",
                "gw_jitter_ms", "inet_jitter_ms", "gw_loss_pct", "inet_loss_pct",
                "rx_rate_mbps", "tx_rate_mbps", "hour_of_day"]

assembler = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="skip")
rf = RandomForestClassifier(featuresCol="features", labelCol="label", numTrees=50, maxDepth=8, seed=42)
pipeline = Pipeline(stages=[assembler, rf])

train, test = train_df.randomSplit([0.8, 0.2], seed=42)

with mlflow.start_run(run_name="vt_connectivity_teams_v1") as run:
    model = pipeline.fit(train)
    preds = model.transform(test)
    
    evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction")
    accuracy = evaluator.evaluate(preds)
    f1 = evaluator.evaluate(preds, {evaluator.metricName: "f1"})
    
    mlflow.log_metric("accuracy", accuracy)
    mlflow.log_metric("f1_score", f1)
    mlflow.spark.log_model(model, "teams_connectivity_model")
    
    print(f"Model trained! Accuracy: {accuracy:.3f}, F1: {f1:.3f}")
    print(f"MLflow Run ID: {run.info.run_id}")
    
    # Feature importance
    rf_model = model.stages[-1]
    importances = rf_model.featureImportances
    for i, col in enumerate(feature_cols):
        print(f"  {col}: {importances[i]:.4f}")

# COMMAND ----------

# DBTITLE 1,Step 6: Agent Query Functions
# ============================================================
# Step 6: Agent Query Functions
# ============================================================
# These functions power the AI agent that helps students find
# the best connectivity zones for Teams/Zoom calls.

def get_zone_quality(zone_id=None, hour=None):
    """Get current quality for a specific zone or all zones."""
    where = []
    if zone_id:
        where.append(f"zone_id = '{zone_id}'")
    if hour is not None:
        where.append(f"hour_of_day = {hour}")
    where_clause = " AND ".join(where) if where else "1=1"
    return spark.sql(f"""
        SELECT zone_id, floor, hour_of_day, teams_quality,
               ROUND(avg_signal_pct,1) as signal_pct,
               ROUND(avg_inet_latency_ms,1) as latency_ms,
               ROUND(avg_tcp_down_mbps,1) as down_mbps
        FROM vt_connectivity.campus.gold_zone_summary
        WHERE {where_clause}
        ORDER BY avg_signal_pct DESC
    """).toPandas()

def can_i_take_teams_call(zone_id, current_hour=None):
    """Check if a specific zone can support a Teams call right now."""
    import datetime
    h = current_hour or datetime.datetime.now().hour
    result = spark.sql(f"""
        SELECT zone_id, teams_quality, avg_signal_pct, avg_inet_latency_ms,
               avg_tcp_down_mbps, avg_packet_loss_pct
        FROM vt_connectivity.campus.gold_zone_summary
        WHERE zone_id = '{zone_id}' AND hour_of_day = {h}
    """).toPandas()
    if result.empty:
        return f"No data for zone '{zone_id}' at hour {h}."
    row = result.iloc[0]
    quality = row['teams_quality']
    if quality == 'good':
        return f"✅ Yes! Zone {zone_id} is great for a Teams call. Signal: {row['avg_signal_pct']:.0f}%, Latency: {row['avg_inet_latency_ms']:.0f}ms, Down: {row['avg_tcp_down_mbps']:.0f} Mbps"
    elif quality == 'moderate':
        return f"⚠️  Zone {zone_id} is moderate. You might experience some lag. Signal: {row['avg_signal_pct']:.0f}%, Latency: {row['avg_inet_latency_ms']:.0f}ms"
    else:
        return f"❌ Zone {zone_id} is too poor for a Teams call. Find a better spot!"

def find_best_zone_near(current_zone, current_hour=None):
    """Find the best nearby zone for a Teams call."""
    import datetime
    h = current_hour or datetime.datetime.now().hour
    current_floor = current_zone.split('_')[1] if '_' in current_zone else 'f1'
    result = spark.sql(f"""
        SELECT zone_id, floor, teams_quality, avg_signal_pct, avg_inet_latency_ms
        FROM vt_connectivity.campus.gold_zone_summary
        WHERE hour_of_day = {h}
          AND teams_quality = 'good'
          AND zone_id != '{current_zone}'
        ORDER BY avg_signal_pct DESC
        LIMIT 3
    """).toPandas()
    if result.empty:
        return f"No good zones found at hour {h}. Try later or move to a different building."
    recs = []
    for _, r in result.iterrows():
        recs.append(f"  → {r['zone_id']} (Floor {r['floor']}): Signal {r['avg_signal_pct']:.0f}%, Latency {r['avg_inet_latency_ms']:.0f}ms")
    return f"Best zones for a Teams call near {current_zone} at hour {h}:\n" + "\n".join(recs)

# Test the agent functions
print("=== Zone Quality Summary ===")
print(get_zone_quality(hour=14).to_string())
print("\n=== Can I take a Teams call at torg_f3_class_b? ===")
print(can_i_take_teams_call('torg_f3_class_b', 14))
print("\n=== Find best zone near torg_f3_class_b ===")
print(find_best_zone_near('torg_f3_class_b', 14))

# COMMAND ----------

