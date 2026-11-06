# NYC Taxi Trip Data Pipeline (Databricks, Unity Catalog)

Bronze-silver-gold pipeline on NYC Yellow Taxi data for January 2024 (2.96M rows), built in Databricks Free Edition with SQL and run as a Databricks Job.

## Layers
- **Bronze** (`workspace.nyc_taxi.bronze_trips`): raw Parquet file loaded from a Unity Catalog volume, no changes.
- **Silver** (`silver_trips`): removed exact duplicate rows, rows missing pickup time or fare, fares at or below zero, and pickups outside January 2024.
- **Gold** (`gold_daily`): one row per day with trip count, total fares, and average distance.

## Reconciliation (bronze to silver)
| Rule | Rows removed | Fare change |
|---|---|---|
| Exact duplicates | 0 | 0 |
| Missing pickup or fare | 0 | 0 |
| Fare zero or negative | 38,341 | +714,257.24 |
| Pickup outside Jan 2024 | 17 | -308.60 |
| **Total** | **38,358 (1.3%)** | **+713,948.64** |

Bronze: 2,964,624 rows, 53,882,224.76 in fares. Silver: 2,926,266 rows, 54,596,173.40.
Gold totals match silver exactly.

## Decisions
- **Removed negative fares.** These are mostly refunds and voided trips, so removing them raises total fares. Silver measures completed trip activity, not net revenue. For a revenue view, refunds should be kept and netted.
- **Duplicates removed only as exact full-row matches.** The data has no trip ID, so near-duplicates can't be safely identified.
- **No type changes needed.** The Parquet source already stores correct types.
- **Summarized by day, not month.** One month of data gives one monthly row, so daily is more useful.

## Rerun check
All tables use `CREATE OR REPLACE`, so rerunning rebuilds instead of appending. Ran as a Databricks Job twice; gold totals were identical both times.
