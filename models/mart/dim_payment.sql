
SELECT DISTINCT
    payment_code,
    payment_type
FROM {{ ref('src_tlc_yellow_trips') }}
