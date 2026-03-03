/* @bruin
name: transform.trips_cleaned
type: duckdb.sql
connection: duckdb_default
depends:
  - ingestion.trips

materialization:
  type: table
  strategy: replace

columns:
  - name: pickup_datetime
    type: timestamp
    checks:
      - name: not_null
  - name: trip_duration_minutes
    type: float
    checks:
      - name: positive
  - name: taxi_type
    type: string
    checks:
      - name: not_null
*/

SELECT
    vendor_id,
    pickup_datetime,
    dropoff_datetime,
    passenger_count,
    trip_distance,
    fare_amount,
    taxi_type,

    -- derived columns
    ROUND(
        EXTRACT(EPOCH FROM (dropoff_datetime - pickup_datetime)) / 60.0,
        2
    )                                          AS trip_duration_minutes,

    DATE_TRUNC('month', pickup_datetime)       AS pickup_month,
    EXTRACT(HOUR FROM pickup_datetime)         AS pickup_hour,
    DAYNAME(pickup_datetime)                   AS pickup_day_of_week

FROM ingestion.trips
WHERE
    pickup_datetime  IS NOT NULL
    AND dropoff_datetime IS NOT NULL
    AND trip_distance    >  0
    AND fare_amount      >= 0
    AND passenger_count  >  0
