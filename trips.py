"""
@bruin asset
name: ingestion.trips
type: python
connection: duckdb_default

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: day

columns:
  - name: vendor_id
    type: integer
    description: "Vendor identifier"
  - name: pickup_datetime
    type: timestamp
    description: "Trip pickup timestamp"
    checks:
      - name: not_null
  - name: dropoff_datetime
    type: timestamp
    description: "Trip dropoff timestamp"
    checks:
      - name: not_null
  - name: passenger_count
    type: integer
    description: "Number of passengers"
  - name: trip_distance
    type: float
    description: "Trip distance in miles"
    checks:
      - name: positive
  - name: fare_amount
    type: float
    description: "Fare amount in USD"
  - name: taxi_type
    type: string
    description: "Taxi type: yellow or green"
    checks:
      - name: not_null
      - name: accepted_values
        value: ["yellow", "green"]
"""

import urllib.request
import duckdb

def materialize(context):
    taxi_types = context.variables.get("taxi_types", ["yellow", "green"])
    start_date = context.variables.get("start_date", "2025-01-01")
    end_date   = context.variables.get("end_date",   "2025-12-31")

    conn = duckdb.connect(context.connections.duckdb_default.path)

    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"

    for taxi_type in taxi_types:
        # Determine date range months
        from datetime import datetime, timedelta
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end   = datetime.strptime(end_date,   "%Y-%m-%d")
        current = start.replace(day=1)

        while current <= end:
            month_str = current.strftime("%Y-%m")
            url = f"{base_url}/{taxi_type}_tripdata_{month_str}.parquet"
            print(f"Loading {taxi_type} taxi data for {month_str} from:\n  {url}")

            conn.execute(f"""
                INSERT INTO ingestion.trips
                SELECT
                    VendorID            AS vendor_id,
                    tpep_pickup_datetime   AS pickup_datetime,
                    tpep_dropoff_datetime  AS dropoff_datetime,
                    passenger_count,
                    trip_distance,
                    fare_amount,
                    '{taxi_type}'       AS taxi_type
                FROM read_parquet('{url}')
                WHERE tpep_pickup_datetime >= '{start_date}'
                  AND tpep_pickup_datetime <  '{end_date}'
            """)

            # Advance one month
            if current.month == 12:
                current = current.replace(year=current.year + 1, month=1)
            else:
                current = current.replace(month=current.month + 1)

    conn.close()
    print("✅ ingestion.trips complete")
