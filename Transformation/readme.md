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

---

# Geospatial Engine & Advanced Transformations — PySpark (Part 2)

## 9. Geospatial Source & Geometry Data

| File | Location | Role |
| :--- | :--- | :--- |
| `trip_clean_data` | `hdfs:///tlc/silver/trip_clean_data` | Cleaned trip records from the previous ETL phase (499,999 rows). |
| `taxi_zones.parquet` | `hdfs:///tlc/raw/zones/taxi_zones.parquet` | Official NYC TLC polygon shapefile for zone centroids and spatial hashing. |

## 10. Geospatial Architecture & Spatial Indexing Choices

The cleaned HVFHV data carries zone IDs but lacks physical coordinates. To make the data analysis-ready for spatial modeling and ML feature stores, we engineered two complementary spatial indexes:

- **Coordinate System Reprojection (`EPSG:2263` → `EPSG:4326`):** The raw shapefile uses the NY State Plane projection (measured in feet). We reprojected it to WGS84 latitude/longitude degrees before computing centroids.
- **Uber H3 Hexagonal Grid (Resolution 8):** Hexagons have 6 equidistant neighbors with zero directional distance distortion. At Resolution 8, cell radius is ~461m (~0.737 km²), matching NYC city block dimensions. Essential for Gold-layer surge pricing and demand hotspots.
- **Geohash (Precision 6):** Rectangular bounding boxes (~1.2 km × 0.6 km) optimized for fast spatial prefix-matching and cache lookups.
- **Precomputation Pattern:** Rather than invoking Python UDFs on 20M+ trip rows (which causes severe JVM-Python serialization bottlenecks), spatial keys were precomputed on the 263 zone polygons in memory (< 1 sec) and attached via a broadcast join.

## 11. Schema — Enriched Silver (`staging_rides_geo`)

This is the final schema produced by the geospatial enrichment pipeline, saved in Parquet format partitioned by `year`/`month`/`day`:

```
root
 |-- trip_id: string (nullable = false)                    -- NEW: Synthetic primary key
 |-- pu_location_id: integer (nullable = true)
 |-- do_location_id: integer (nullable = true)
 |-- license_num: string (nullable = true)
 |-- dispatching_base_num: string (nullable = true)
 |-- originating_base_num: string (nullable = false)
 |-- request_datetime: timestamp (nullable = true)
 |-- on_scene_datetime: timestamp (nullable = true)
 |-- pickup_datetime: timestamp (nullable = true)
 |-- dropoff_datetime: timestamp (nullable = true)
 |-- trip_miles: double (nullable = true)
 |-- trip_time: long (nullable = true)
 |-- trip_duration_sec: long (nullable = true)
 |-- avg_speed_mph: double (nullable = false)              -- NEW: Calculated velocity
 |-- is_speed_anomaly: boolean (nullable = false)          -- NEW: Flag for speed > 120 mph
 |-- base_passenger_fare: double (nullable = false)
 |-- tolls: double (nullable = false)
 |-- bcf: double (nullable = false)
 |-- sales_tax: double (nullable = false)
 |-- congestion_surcharge: double (nullable = false)
 |-- airport_fee: double (nullable = false)
 |-- tips: double (nullable = false)
 |-- driver_pay: double (nullable = false)
 |-- total_fare: double (nullable = false)
 |-- shared_request_flag: boolean (nullable = true)
 |-- shared_match_flag: boolean (nullable = true)
 |-- access_a_ride_flag: boolean (nullable = true)
 |-- wav_request_flag: boolean (nullable = true)
 |-- wav_match_flag: boolean (nullable = true)
 |-- was_on_scene_matched: boolean (nullable = false)
 |-- pu_borough: string (nullable = true)
 |-- pu_zone: string (nullable = true)
 |-- pu_service_zone: string (nullable = true)
 |-- do_borough: string (nullable = true)
 |-- do_zone: string (nullable = true)
 |-- do_service_zone: string (nullable = true)
 |-- pu_lat: double (nullable = true)                      -- NEW: Pickup centroid latitude (WGS84)
 |-- pu_lon: double (nullable = true)                      -- NEW: Pickup centroid longitude (WGS84)
 |-- start_geo_hash: string (nullable = true)              -- NEW: Pickup Geohash (Precision 6)
 |-- pu_h3_res8: string (nullable = true)                  -- NEW: Pickup Uber H3 (Resolution 8)
 |-- do_lat: double (nullable = true)                      -- NEW: Dropoff centroid latitude (WGS84)
 |-- do_lon: double (nullable = true)                      -- NEW: Dropoff centroid longitude (WGS84)
 |-- end_geo_hash: string (nullable = true)                -- NEW: Dropoff Geohash (Precision 6)
 |-- do_h3_res8: string (nullable = true)                  -- NEW: Dropoff Uber H3 (Resolution 8)
 |-- is_weekend: boolean (nullable = true)
 |-- is_peak_hour: boolean (nullable = true)
 |-- year: integer (nullable = true)                       -- Partition key
 |-- month: integer (nullable = true)                      -- Partition key
 |-- day: integer (nullable = true)                        -- Partition key
```

Output is written to `hdfs:///tlc/silver/staging_rides_geo`, partitioned by `year`/`month`/`day`.

## 12. Geospatial Transformation Steps & Rationale

| Step | What | Why |
| :--- | :--- | :--- |
| 1. Read Geometry & Reproject | Read `taxi_zones.parquet` via Spark, parse WKB/WKT, reproject `EPSG:2263` → `EPSG:4326` | Resolves the absence of raw coordinates and aligns with WGS84 standards |
| 2. Centroid Extraction | Calculate `centroid.x` (lon) and `centroid.y` (lat) for all 263 zone polygons | Derives representative point coordinates for each taxi zone |
| 3. Precompute Spatial Keys | Encode centroids into Uber H3 (Res 8) and Geohash (Precision 6) | Generates standardized spatial indexing keys for both pickup and dropoff |
| 4. Broadcast Join (Zero Shuffle) | `F.broadcast()` spatial lookup table joined on `pu_location_id` and `do_location_id` | Prevents shuffling millions of trip rows across executors, speeding up the pipeline |
| 5. Primary Key Generation | Synthesize `trip_id` using `license_num` + locations + timestamp + `monotonically_increasing_id` | Establishes a deterministic join key for downstream payments and driver tables |
| 6. Velocity & Anomaly Checks | Calculate `avg_speed_mph`; flag speeds > 120 mph with `is_speed_anomaly` | Detects GPS/meter anomalies while preserving data lineage for downstream modeling |
| 7. Partitioned Write | `repartition(4, "year", "month", "day")` before writing Parquet | Controls HDFS block distribution (~128MB) and enables query partition pruning |

## 13. Latest Run Results (Verification Run, 499,999 rows)

| Metric | Value |
| :--- | :--- |
| Input clean rows | 499,999 |
| Output staging rows | 499,999 (0 rows dropped during join) |
| `trip_id` nulls | 0 (0.00%) |
| `start_geo_hash` / `pu_h3` nulls | 21 (0.00%) — exactly matches N/A zone IDs (264/265) identified in EDA |
| `end_geo_hash` / `do_h3` nulls | 18,801 (3.76%) — trips terminating outside NYC borders (Zone 265) |
| `avg_speed_mph` nulls | 0 (0.00%) |
| Average speed range observed | 5.44 – 11.86 mph (consistent with typical NYC urban traffic density) |

## 14. Advanced Spark Concepts — Where They Show Up

- **Broadcast Joins (`F.broadcast`)**: Applied twice (for pickup and dropoff spatial keys). By broadcasting the compact 263-row spatial table to all executors, we eliminated cluster-wide network shuffles.
- **Precomputation over Row-Level UDFs**: Replaced costly PySpark UDF execution on 20M rows with vectorized precomputation on reference zones.
- **Adaptive Query Execution (AQE)**: Configured `spark.sql.adaptive.enabled=true` and `coalescePartitions.enabled=true` for dynamic shuffle partition coalescing.
- **Partition Pruning**: Enforced `partitionBy("year", "month", "day")` on write so downstream Gold queries can prune unneeded daily partitions.

## 15. Handoff to Dimensional Modeling (Gold Layer)

`staging_rides_geo` is the authoritative consolidated Silver table ready for Star Schema modeling:
- **`Fact_Trip`**: Uses `trip_id` as primary key, `pu_h3_res8` / `do_h3_res8` for spatial facts, and `avg_speed_mph` for performance metrics.
- **`Dim_Location`**: Can be derived directly from distinct `(pu_location_id, pu_borough, pu_zone, pu_lat, pu_lon, start_geo_hash, pu_h3_res8)`.
- **Downstream Integration**: Downstream tables (Drivers, Payments, and Customer Support) join directly to this table via `trip_id` and `license_num`.
