WITH raw AS (
    SELECT *
    FROM RAW.TLC_YELLOW_TRIPS_2021
),

vendor_map AS (
    SELECT *
    FROM {{ ref('vendor_mapping') }}
)

SELECT
    r.vendor_id,
    v."vendor_name" AS vendor_name,
    TO_TIMESTAMP_TZ(REPLACE(r.pickup_datetime, ' UTC', ''), 'YYYY-MM-DD HH24:MI:SS') AS pickup_datetime_utc,
    TO_TIMESTAMP_TZ(REPLACE(r.dropoff_datetime, ' UTC', ''), 'YYYY-MM-DD HH24:MI:SS') AS dropoff_datetime_utc,
    r.passenger_count,
    r.trip_distance,
    r.rate_code,
    r.store_and_fwd_flag,
    r.payment_type,
    r.fare_amount,
    r.extra,
    r.mta_tax,
    r.tip_amount,
    r.tolls_amount,
    r.imp_surcharge,
    r.airport_fee,
    r.total_amount,
    r.pickup_location_id,
    r.dropoff_location_id,
    r.data_file_year,
    r.data_file_month
FROM raw r
LEFT JOIN vendor_map v
  ON r.vendor_id = v."vendor_id"
