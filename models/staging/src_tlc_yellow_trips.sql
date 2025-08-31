{{ config(
    materialized='table',
    unique_key='trip_id'
) }}

WITH raw AS (
    SELECT *
    FROM {{ source('raw', 'TLC_YELLOW_TRIPS_2021') }}
),

vendor_map AS (
    SELECT *
    FROM {{ ref('vendor_mapping') }}
),
rate_code_map AS (
    SELECT *
    FROM {{ ref('rate_code_mapping') }}
),

store_and_fwd_flag_map AS (
    SELECT *
    FROM {{ ref('store_and_fwd_mapping') }}
),

payment_map AS (
    SELECT *
    FROM {{ ref('payment_mapping') }}
)

SELECT
   {{ generate_trip_surrogate_key([
    'vendor_id',
    'pickup_location_id',
    'rate_code',
    'payment_type',
    'store_and_fwd_flag',
    'pickup_datetime',
    'dropoff_datetime'
]) }} AS trip_id,
    r.vendor_id,
    v."vendor_name" AS vendor_name,
    TO_TIMESTAMP_TZ(REPLACE(r.pickup_datetime, ' UTC', ''), 'YYYY-MM-DD HH24:MI:SS') AS pickup_datetime_utc,
    TO_TIMESTAMP_TZ(REPLACE(r.dropoff_datetime, ' UTC', ''), 'YYYY-MM-DD HH24:MI:SS') AS dropoff_datetime_utc,
    r.passenger_count,
    r.trip_distance,
    r.rate_code,
    rc."rate_type" as rate_type,
    r.store_and_fwd_flag,
    sf."flag_description" AS store_and_fwd_description,
    r.payment_type as payment_code,
    p."payment_code" AS payment_type,
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
LEFT JOIN rate_code_map rc
  ON r.rate_code = rc."rate_code"
LEFT JOIN store_and_fwd_flag_map sf
  ON r.store_and_fwd_flag = sf."store_and_fwd_flag"
LEFT JOIN payment_map p
  ON r.payment_type = p."payment_code"

WHERE  
              r.fare_amount IS NOT NULL 
              AND dropoff_datetime >= pickup_datetime
              AND r.trip_distance IS NOT NULL
              AND r.payment_type IS NOT NULL
              AND r.rate_code IS NOT NULL
              AND r.store_and_fwd_flag IS NOT NULL
              AND  r.pickup_datetime IS NOT NULL
            



