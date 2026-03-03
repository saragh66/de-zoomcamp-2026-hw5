/* @bruin
name: reports.hourly_demand
type: duckdb.sql
connection: duckdb_default
depends:
  - transform.trips_cleaned

materialization:
  type: table
  strategy: replace
*/

SELECT
    pickup_hour,
    pickup_day_of_week,
    taxi_type,
    COUNT(*)                            AS total_trips,
    ROUND(AVG(fare_amount), 2)          AS avg_fare_usd
FROM transform.trips_cleaned
GROUP BY
    pickup_hour,
    pickup_day_of_week,
    taxi_type
ORDER BY
    pickup_hour,
    pickup_day_of_week
