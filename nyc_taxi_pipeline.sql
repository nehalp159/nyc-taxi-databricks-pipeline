-- Databricks notebook source
-- MAGIC %md
-- MAGIC # NYC Taxi Trip Data Pipeline (Databricks, Unity Catalog)
-- MAGIC
-- MAGIC Bronze-silver-gold pipeline on NYC Yellow Taxi data for January 2024 (2.96M rows), built in Databricks Free Edition with SQL and run as a Databricks Job.
-- MAGIC
-- MAGIC ## Layers
-- MAGIC - **Bronze** (`workspace.nyc_taxi.bronze_trips`): raw Parquet file loaded from a Unity Catalog volume, no changes.
-- MAGIC - **Silver** (`silver_trips`): removed exact duplicate rows, rows missing pickup time or fare, fares at or below zero, and pickups outside January 2024.
-- MAGIC - **Gold** (`gold_daily`): one row per day with trip count, total fares, and average distance.
-- MAGIC
-- MAGIC ## Reconciliation (bronze to silver)
-- MAGIC | Rule | Rows removed | Fare change |
-- MAGIC |---|---|---|
-- MAGIC | Exact duplicates | 0 | 0 |
-- MAGIC | Missing pickup or fare | 0 | 0 |
-- MAGIC | Fare zero or negative | 38,341 | +714,257.24 |
-- MAGIC | Pickup outside Jan 2024 | 17 | -308.60 |
-- MAGIC | **Total** | **38,358 (1.3%)** | **+713,948.64** |
-- MAGIC
-- MAGIC Bronze: 2,964,624 rows, 53,882,224.76 in fares. Silver: 2,926,266 rows, 54,596,173.40.
-- MAGIC Gold totals match silver exactly.
-- MAGIC
-- MAGIC ## Decisions
-- MAGIC - **Removed negative fares.** These are mostly refunds and voided trips, so removing them raises total fares. Silver measures completed trip activity, not net revenue. For a revenue view, refunds should be kept and netted.
-- MAGIC - **Duplicates removed only as exact full-row matches.** The data has no trip ID, so near-duplicates can't be safely identified.
-- MAGIC - **No type changes needed.** The Parquet source already stores correct types.
-- MAGIC - **Summarized by day, not month.** One month of data gives one monthly row, so daily is more useful.
-- MAGIC
-- MAGIC ## Rerun check
-- MAGIC All tables use `CREATE OR REPLACE`, so rerunning rebuilds instead of appending. Ran as a Databricks Job twice; gold totals were identical both times.

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS workspace.nyc_taxi;
CREATE VOLUME IF NOT EXISTS workspace.nyc_taxi.raw;

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.nyc_taxi.bronze_trips AS
SELECT *
FROM read_files(
  '/Volumes/workspace/nyc_taxi/raw/yellow_tripdata_2024-01.parquet',
  format => 'parquet'
);

-- COMMAND ----------

DESCRIBE TABLE workspace.nyc_taxi.bronze_trips;

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.nyc_taxi.silver_trips AS
SELECT *
FROM (SELECT DISTINCT * FROM workspace.nyc_taxi.bronze_trips)
WHERE tpep_pickup_datetime IS NOT NULL
  AND fare_amount IS NOT NULL
  AND fare_amount > 0
  AND tpep_pickup_datetime >= '2024-01-01'
  AND tpep_pickup_datetime <  '2024-02-01';

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.nyc_taxi.gold_daily AS
SELECT
  DATE(tpep_pickup_datetime)    AS trip_date,
  COUNT(*)                      AS trip_count,
  ROUND(SUM(fare_amount), 2)    AS total_fares,
  ROUND(AVG(trip_distance), 2)  AS avg_distance
FROM workspace.nyc_taxi.silver_trips
GROUP BY DATE(tpep_pickup_datetime);

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.nyc_taxi.recon AS
SELECT 1 AS step, 'bronze' AS stage, COUNT(*) AS row_count, ROUND(SUM(fare_amount),2) AS total_fares
FROM workspace.nyc_taxi.bronze_trips
UNION ALL
SELECT 2, 'after removing duplicates', COUNT(*), ROUND(SUM(fare_amount),2)
FROM (SELECT DISTINCT * FROM workspace.nyc_taxi.bronze_trips)
UNION ALL
SELECT 3, 'after removing missing pickup or fare', COUNT(*), ROUND(SUM(fare_amount),2)
FROM (SELECT DISTINCT * FROM workspace.nyc_taxi.bronze_trips)
WHERE tpep_pickup_datetime IS NOT NULL AND fare_amount IS NOT NULL
UNION ALL
SELECT 4, 'after removing fares at or below zero', COUNT(*), ROUND(SUM(fare_amount),2)
FROM (SELECT DISTINCT * FROM workspace.nyc_taxi.bronze_trips)
WHERE tpep_pickup_datetime IS NOT NULL AND fare_amount IS NOT NULL AND fare_amount > 0
UNION ALL
SELECT 5, 'silver (after removing dates outside Jan 2024)', COUNT(*), ROUND(SUM(fare_amount),2)
FROM workspace.nyc_taxi.silver_trips;

SELECT * FROM workspace.nyc_taxi.recon ORDER BY step;

-- COMMAND ----------

SELECT SUM(trip_count) AS gold_rows, ROUND(SUM(total_fares),2) AS gold_fares
FROM workspace.nyc_taxi.gold_daily;