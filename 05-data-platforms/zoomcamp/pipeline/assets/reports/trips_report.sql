/* @bruin

# Docs:
# - SQL assets: https://getbruin.com/docs/bruin/assets/sql
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks: https://getbruin.com/docs/bruin/quality/available_checks

# trips Report - Aggregates tipping patterns and metrics
name: reports.trips_report

# Platform type
type: duckdb.sql

# Declare dependency on the staging asset(s) this report reads from
depends:
  - staging.trips

# Choose materialization strategy
# time_interval rebuilds only the relevant time window for efficiency
materialization:
  type: table
  incremental_key: pickup_datetime

# Define report columns
columns:
  - name: date
    type: date
    description: Date of the trip
  - name: payment_type
    type: string
    description: Payment type
  - name: total_trips
    type: float
    description: Total trips collected
  - name: average_tip
    type: float
    description: Average tip amount
  - name: max_tip
    type: float
    description: Maximum tip amount
  - name: trip_count
    type: int
    description: Number of trips with trips

@bruin */

-- Purpose: Aggregate tip data for analysis and monitoring
-- - Filter using {{ start_datetime }} / {{ end_datetime }} for incremental runs
-- - GROUP BY dimension and date columns for reporting

SELECT
  DATE(tpep_pickup_datetime) as date,
  payment_type,
  SUM(tip_amount) as total_trips,
  AVG(tip_amount) as average_tip,
  MAX(tip_amount) as max_tip,
  COUNT(*) as trip_count
FROM staging.trips
WHERE tpep_pickup_datetime >= '{{ start_datetime }}'
  AND tpep_pickup_datetime < '{{ end_datetime }}'
  AND tip_amount > 0
GROUP BY
  DATE(tpep_pickup_datetime),
  payment_type
ORDER BY date DESC, total_trips DESC
