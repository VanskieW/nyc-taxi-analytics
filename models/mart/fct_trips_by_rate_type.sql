WITH trips AS (
    SELECT
        r.trip_id,
        r.vendor_id,
        r.pickup_location_id,
        r.rate_code,
        r.payment_code,
        r.payment_type,
        r.fare_amount,
        r.tip_amount,
        r.total_amount,
        DATEDIFF(minute, r.pickup_datetime_utc, r.dropoff_datetime_utc) AS trip_duration_minutes
    FROM {{ ref('src_tlc_yellow_trips') }} r
)

SELECT
    t.vendor_id,
    v.vendor_name,
    t.pickup_location_id,
    t.rate_code,
    rt.rate_type,
    COUNT(*) AS total_trips,
    COUNT_IF(t.payment_code='6') AS voided_trips,
    COUNT_IF(t.payment_code='4') AS disputed_trips,
    SUM(t.fare_amount) AS total_fare,
    SUM(t.tip_amount) AS total_tips,
    SUM(t.total_amount) AS total_revenue,
    AVG(t.total_amount) AS avg_revenue_per_trip,
    SUM(t.trip_duration_minutes) AS total_duration_minutes,
    AVG(t.trip_duration_minutes) AS avg_trip_duration_minutes,
    COUNT_IF(t.total_amount > 100) AS high_value_trips,
    SUM(CASE WHEN t.total_amount > 100 THEN t.total_amount ELSE 0 END) AS high_value_revenue
FROM trips t
LEFT JOIN {{ ref('dim_vendor') }} v ON t.vendor_id = v.vendor_id
LEFT JOIN {{ ref('dim_rate') }} rt ON t.rate_code = rt.rate_code
GROUP BY 1,2,3,4,5
ORDER BY vendor_name, pickup_location_id, rate_type
