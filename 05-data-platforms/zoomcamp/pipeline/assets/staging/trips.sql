/* @bruin

name: staging.trips
type: duckdb.sql

materialization:
  type: table
  strategy: create+replace

depends:
  - ingestion.trips
  - ingestion.payment_lookup



@bruin */

-- Staging layer: clean, deduplicate, and enrich raw trip data
WITH deduplicated_trips AS (
  -- Use ROW_NUMBER to deduplicate on composite key
  -- Keep the first occurrence of each unique trip (lowest extracted_at)
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY
        CAST(tpep_pickup_datetime AS DATE),
        CAST(tpep_dropoff_datetime AS DATE),
        PULocationID,
        DOLocationID,
        fare_amount
      ORDER BY extracted_at ASC
    ) as rn
  FROM ingestion.trips
  WHERE tpep_pickup_datetime >= '{{ start_datetime }}'
    AND tpep_pickup_datetime < '{{ end_datetime }}'
)

SELECT
  d.VendorID,
  d.tpep_pickup_datetime,
  d.tpep_dropoff_datetime,
  d.passenger_count,
  d.trip_distance,
  d.RatecodeID,
  d.store_and_fwd_flag,
  d.PULocationID,
  d.DOLocationID,
  d.payment_type,
  d.fare_amount,
  d.extra,
  d.mta_tax,
  d.tip_amount,
  d.tolls_amount,
  d.total_amount,
  d.taxi_type,
  COALESCE(p.payment_type_name, 'Unknown') as payment_type_name,
  d.extracted_at
FROM deduplicated_trips d
LEFT JOIN ingestion.payment_lookup p
  ON d.payment_type = p.payment_type_id
WHERE d.rn = 1
  -- Filter out obviously invalid rows
  AND d.tpep_pickup_datetime IS NOT NULL
  AND d.tpep_dropoff_datetime IS NOT NULL
  AND d.PULocationID IS NOT NULL
  AND d.DOLocationID IS NOT NULL
  AND d.trip_distance >= 0
  AND d.fare_amount >= 0
