SELECT DISTINCT
    rate_code,
    rate_type
FROM {{ ref('src_tlc_yellow_trips') }}
WHERE rate_code IS NOT NULL