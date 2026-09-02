# Transformation Engine - PySpark

## 1. Source Data

| File                                    | Location                | Role                                    |
| --------------------------------------- | ----------------------- | --------------------------------------- |
| `fhvhv_tripdata_2024-01-snappy.parquet` | `hdfs:///tlc/raw/hvfhv` | Basis for `Fact_Trip`                   |
| `part-00000-...-c000.snappy.parquet`    | `hdfs:///tlc/raw/zones` | Basis for `Dim_Location` (attributes)   |
| `taxi_zones.parquet`                    | `hdfs:///tlc/raw/zones` | Geometry for geospatial keys (Member 3) |

Full dataset: **19,663,930** trips, 24 columns (from EDA).

## 2. Dev mode

The notebook runs with a `DEV_MODE` / `DEV_SAMPLE_SIZE` toggle:

```python
DEV_MODE = True
DEV_SAMPLE_SIZE = 500_000
```

When `DEV_MODE = True`, `read_raw_trips()` calls `.limit(DEV_SAMPLE_SIZE)` on
the raw read so the whole pipeline runs fast during development.

**Caveat worth knowing before you trust any aggregation output while
`DEV_MODE` is on:** `.limit(n)` takes whichever `n` rows Spark reads first —
it is **not** a random or representative sample. The current run's output
shows this clearly: with 19,663,930 rows spread across 31 days
(~634k/day on average), a 500k-row `.limit()` doesn't even cover one full
day. The `day_of_week` aggregation demo in the latest run only shows
`day_of_week = 2` and `3` (Monday and Tuesday, Jan 1–2 2024) — every other
day of the month is simply absent from the sample, not because of any
filter or bug.

## 3. Schema — Before ETL (raw HVFHV)

```
root
 |-- hvfhs_license_num: string (nullable = true)
 |-- dispatching_base_num: string (nullable = true)
 |-- originating_base_num: string (nullable = true)      -- 5,218,737 nulls
 |-- request_datetime: timestamp (nullable = true)
 |-- on_scene_datetime: timestamp (nullable = true)       -- 5,218,737 nulls
 |-- pickup_datetime: timestamp (nullable = true)
 |-- dropoff_datetime: timestamp (nullable = true)
 |-- PULocationID: integer (nullable = true)
 |-- DOLocationID: integer (nullable = true)
 |-- trip_miles: double (nullable = true)
 |-- trip_time: long (nullable = true)
 |-- base_passenger_fare: double (nullable = true)
 |-- tolls: double (nullable = true)
 |-- bcf: double (nullable = true)
 |-- sales_tax: double (nullable = true)
 |-- congestion_surcharge: double (nullable = true)
 |-- airport_fee: double (nullable = true)
 |-- tips: double (nullable = true)
 |-- driver_pay: double (nullable = true)
 |-- shared_request_flag: string (nullable = true)        -- Y/N
 |-- shared_match_flag: string (nullable = true)           -- Y/N
 |-- access_a_ride_flag: string (nullable = true)          -- Y/N
 |-- wav_request_flag: string (nullable = true)            -- Y/N
 |-- wav_match_flag: string (nullable = true)              -- Y/N
```

Key EDA findings that drove the transformation design:

- No `dropoff_datetime < pickup_datetime` violations (kept as a standing safety-net filter anyway).
- `on_scene_datetime` and `originating_base_num` are null for the exact same ~5.2M rows — a legitimate "no dispatch-match event" state, not a data quality defect.
- All 5 flag columns are confirmed exactly 2 distinct values (Y/N), 0 nulls.
- `hvfhs_license_num` has only 2 distinct values present this month (of 4 known operator codes) — the validation rule checks against all 4 so it doesn't need editing in future months.
- Zones table (265 rows): `Borough` has 8 distinct values, including `Unknown`/`N/A` — legitimate categories, not join failures.

## 4. Schema — After ETL (clean `fact_trip_clean`)

This is the actual schema produced by the current notebook run (column order reflects the join in `enrich_with_zones`, which brings the location ID columns to the front):

```
root
 |-- do_location_id: integer (nullable = true)
 |-- pu_location_id: integer (nullable = true)
 |-- license_num: string (nullable = true)
 |-- dispatching_base_num: string (nullable = true)
 |-- originating_base_num: string (nullable = false)      -- nulls filled with 'UNKNOWN'
 |-- request_datetime: timestamp (nullable = true)
 |-- on_scene_datetime: timestamp (nullable = true)        -- left null where no match event
 |-- pickup_datetime: timestamp (nullable = true)
 |-- dropoff_datetime: timestamp (nullable = true)
 |-- trip_miles: double (nullable = true)
 |-- trip_time: long (nullable = true)
 |-- base_passenger_fare: double (nullable = false)        -- nulls filled with 0.0
 |-- tolls: double (nullable = false)
 |-- bcf: double (nullable = false)
 |-- sales_tax: double (nullable = false)
 |-- congestion_surcharge: double (nullable = false)
 |-- airport_fee: double (nullable = false)
 |-- tips: double (nullable = false)
 |-- driver_pay: double (nullable = false)
 |-- shared_request_flag: boolean (nullable = true)        -- was Y/N string
 |-- shared_match_flag: boolean (nullable = true)
 |-- access_a_ride_flag: boolean (nullable = true)
 |-- wav_request_flag: boolean (nullable = true)
 |-- wav_match_flag: boolean (nullable = true)
 |-- was_on_scene_matched: boolean (nullable = false)       -- NEW
 |-- trip_duration_sec: long (nullable = true)              -- NEW (derived)
 |-- pickup_date: date (nullable = true)                    -- NEW
 |-- pickup_hour: integer (nullable = true)                 -- NEW
 |-- dropoff_hour: integer (nullable = true)                -- NEW
 |-- day_of_week: integer (nullable = true)                 -- NEW (1=Sun...7=Sat)
 |-- day: integer (nullable = true)                         -- NEW (day of month, partition key)
 |-- month: integer (nullable = true)                       -- NEW
 |-- year: integer (nullable = true)                        -- NEW
 |-- is_weekend: boolean (nullable = true)                  -- NEW
 |-- is_peak_hour: boolean (nullable = true)                -- NEW
 |-- total_fare: double (nullable = false)                  -- NEW (sum of fare components)
 |-- pu_borough: string (nullable = true)                   -- NEW (joined from zones)
 |-- pu_zone: string (nullable = true)                      -- NEW (joined from zones)
 |-- pu_service_zone: string (nullable = true)              -- NEW (joined from zones)
 |-- do_borough: string (nullable = true)                   -- NEW (joined from zones)
 |-- do_zone: string (nullable = true)                      -- NEW (joined from zones)
 |-- do_service_zone: string (nullable = true)              -- NEW (joined from zones)
```

Output is written to `hdfs:///tlc/staging/fact_trip_clean`, partitioned by `year`/`month`/`day`, parquet format. Rejected rows (failed validation) are written separately to `hdfs:///tlc/staging/fact_trip_rejects`.

## 5. Transformation Steps & Rationale

| Step                            | What                                                                                                                                                 | Why                                                                                                                                                                                            |
| ------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1. Extract                      | Read raw HVFHV + zones parquet (optionally `.limit()`'d in dev mode)                                                                                 | Lazy — builds the read plan only                                                                                                                                                               |
| 2. Standardize & cast           | Rename `PULocationID`/`DOLocationID`/`hvfhs_license_num`; explicit cast every column                                                                 | Locks in a type contract; protects against schema drift in future monthly loads                                                                                                                |
| 3. Null handling                | Fill `originating_base_num`→`UNKNOWN`, flag `was_on_scene_matched`, fill fare nulls→0.0, drop rows missing hard-required fields                      | Distinguishes "legitimately absent" from "unusable"                                                                                                                                            |
| 4. Duplicate removal            | Drop exact-row duplicates, then window-dedupe on business key keeping latest `request_datetime`                                                      | Two different duplicate shapes need two different strategies                                                                                                                                   |
| 5. Filtering / cleansing        | Drop rows with negative miles/time/pay, invalid license, or dropoff-before-pickup; convert Y/N flags to booleans                                     | Enforces business rules; keeps a reject audit trail instead of silently discarding                                                                                                             |
| 6. Derived fields               | `trip_duration_sec`, `pickup_date`, `pickup_hour`, `dropoff_hour`, `day_of_week`, `day`, `month`, `year`, `is_weekend`, `is_peak_hour`, `total_fare` | Required analytical fields for the fact table and downstream BI                                                                                                                                |
| 7. Join with zones              | Broadcast join (pickup + dropoff) against the 265-row zones table, pulling Borough/Zone/service_zone                                                 | Adds human-readable location context; broadcast avoids a shuffle                                                                                                                               |
| 7b. Referential integrity check | Left-anti join of distinct `pu_location_id`/`do_location_id` against the zones table                                                                 | Confirms (and future-proofs) that every trip location ID resolves to a known zone                                                                                                              |
| 8. Spark SQL + aggregation demo | Temp view + `spark.sql(...)`, plus `groupBy().agg()`                                                                                                 | Demonstrates both APIs on the same clean dataset                                                                                                                                               |
| 9. Write                        | `partitionBy("year", "month", "day")`, `repartition(4, "year", "month", "day")` before write                                                         | With only one month loaded, `year`/`month` alone would give a single partition (no pruning benefit); adding `day` gives real, prunable partitions now and scales cleanly once more months land |

## 6. Latest Run Results (DEV_MODE = True, sample size 500,000)

| Metric                           | Value                                     |
| -------------------------------- | ----------------------------------------- |
| Rows read (dev sample)           | 500,000                                   |
| After exact + business-key dedup | 499,998 (2 duplicates dropped)            |
| Rejected by validation rules     | 8 (written to `fact_trip_rejects`)        |
| Final clean row count            | 499,990                                   |
| Referential integrity            | 0 orphan pickup IDs, 0 orphan dropoff IDs |

Sample SQL aggregation (trips & avg fare by borough/service zone) — top result was Brooklyn/Boro Zone with 149,747 trips at a $33.50 average fare; Manhattan/Yellow Zone followed close behind. An "N/A" borough with 21 trips also appears — a legitimate zone-lookup category, not a join failure (see EDA notes above).

Because of the `.limit()` dev-sampling caveat above, the day-of-week/peak-hour aggregation only reflects Monday and Tuesday (Jan 1–2) — this will fill out once you either turn off `DEV_MODE` or switch to `.sample()`.

## 7. Spark Concepts — Where They Show Up

- **DataFrame API**: every transformation step (`select`, `withColumn`, `filter`, `join`, `groupBy`).
- **Spark SQL**: `createOrReplaceTempView` + `spark.sql(...)` borough/service-zone aggregation query.
- **Transformations vs. actions / lazy evaluation**: the whole pipeline (steps 1–7) is transformations only; `.count()` calls and `.write` are the actions that trigger execution. `df.cache()` is used before the two actions (SQL demo, write) that reuse the same DataFrame.
- **Joins**: broadcast join with the zones dimension source (twice, pickup and dropoff); left-anti join for the referential-integrity check.
- **Aggregations**: SQL `GROUP BY` + DataFrame `groupBy().agg()`.
- **Partitioning**: `repartition()` before write to control file count; `partitionBy("year", "month", "day")` for on-disk partition pruning.

## 8. Handoff to Geospatial Processing

- **Member 3 (dimensional modeling)**: `fact_trip_clean` carries `pu_location_id`/`do_location_id` as clean integer FKs, plus `pu_borough`/`pu_zone`/`pu_service_zone`/`do_borough`/`do_zone`/`do_service_zone` for convenience — build `Dim_Location` from the zones + `taxi_zones.parquet` geometry file (note: the geometry shapefile has 263 rows vs. 265 in the attribute zones table, worth resolving).
