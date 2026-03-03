/* @bruin
name: reports.monthly_summary
type: duckdb.sql
connection: duckdb_default
depends:
  - transform.trips_cleaned

materialization:
  type: table
  strategy: replace
*/

SELECT
    pickup_month,
    taxi_type,

    COUNT(*)                                     AS total_trips,
    ROUND(AVG(trip_distance),          2)        AS avg_distance_miles,
    ROUND(AVG(fare_amount),            2)        AS avg_fare_usd,
    ROUND(AVG(trip_duration_minutes),  2)        AS avg_duration_minutes,
    ROUND(SUM(fare_amount),            2)        AS total_revenue_usd,
    ROUND(AVG(passenger_count),        2)        AS avg_passengers

FROM transform.trips_cleaned
GROUP BY
    pickup_month,
    taxi_type
ORDER BY
    pickup_month,
    taxi_type
