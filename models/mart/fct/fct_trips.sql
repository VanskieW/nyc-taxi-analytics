WITH trips AS (
    SELECT
        r.trip_id,
        r.vendor_id,
        r.pickup_datetime_utc,
        r.dropoff_datetime_utc,
        r.passenger_count,
        r.trip_distance,
        r.rate_code,
        r.store_and_fwd_flag,
        r.payment_code,
        r.payment_type,
        r.fare_amount,
        r.extra,
        r.mta_tax,
        r.tip_amount,
        r.tolls_amount,
        r.imp_surcharge,
        r.airport_fee,
        r.total_amount,
        DATEDIFF(minute, r.pickup_datetime_utc, r.dropoff_datetime_utc) AS trip_duration_minutes,
        (r.trip_distance / NULLIF(DATEDIFF(minute, r.pickup_datetime_utc, r.dropoff_datetime_utc)/60.0,0)) AS average_speed_mph,
        r.pickup_location_id,
        r.data_file_year,
        r.data_file_month
    FROM {{ ref('src_tlc_yellow_trips') }} r
)

SELECT
    t.data_file_year,
    t.data_file_month,
    t.vendor_id,
    v.vendor_name,
    t.pickup_location_id,
    r.rate_type,
    p.payment_code,
    p.payment_type,
    sf.flag_description AS store_and_fwd_desc,
    COUNT(*) AS total_trips,
    COUNT_IF(t.payment_code='6') AS voided_trips,
    COUNT_IF(t.payment_code='4') AS disputed_trips,
    COUNT_IF(t.store_and_fwd_flag='Y') AS store_and_forward_trips,
    COUNT_IF(t.store_and_fwd_flag='Y')*1.0/COUNT(*) AS pct_store_and_forward,
    SUM(t.passenger_count) AS total_passengers,
    AVG(t.passenger_count) AS avg_passengers_per_trip,
    SUM(t.trip_distance) AS total_distance,
    AVG(t.trip_distance) AS avg_trip_distance,
    SUM(t.trip_duration_minutes) AS total_duration_minutes,
    AVG(t.trip_duration_minutes) AS avg_trip_duration_minutes,
    AVG(t.average_speed_mph) AS avg_speed_mph,
    SUM(t.fare_amount) AS total_fare,
    SUM(t.extra) AS total_extras,
    SUM(t.mta_tax) AS total_mta_tax,
    SUM(t.imp_surcharge) AS total_imp_surcharge,
    SUM(t.airport_fee) AS total_airport_fee,
    SUM(t.tolls_amount) AS total_tolls,
    SUM(t.mta_tax + t.imp_surcharge + t.airport_fee + t.tolls_amount) AS total_surcharges,
    SUM(t.tip_amount) AS total_tips,
    SUM(t.total_amount) AS total_revenue,
    AVG(t.total_amount) AS avg_revenue_per_trip,
    SUM(t.tip_amount)/NULLIF(SUM(t.total_amount),0) AS avg_tip_pct,
    COUNT_IF(t.total_amount > 100) AS high_value_trips, -- Example threshold
    SUM(CASE WHEN t.total_amount > 100 THEN t.total_amount ELSE 0 END) AS high_value_revenue
FROM trips t
LEFT JOIN {{ ref('dim_vendor') }} v ON t.vendor_id = v.vendor_id
LEFT JOIN {{ ref('dim_rate') }} r ON t.rate_code = r.rate_code
LEFT JOIN {{ ref('dim_store_flag') }} sf ON t.store_and_fwd_flag = sf.store_and_fwd_flag
LEFT JOIN {{ ref('dim_payment') }} p ON t.payment_code = p.payment_code
GROUP BY 1,2,3,4,5,6,7,8,9
