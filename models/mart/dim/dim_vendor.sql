SELECT DISTINCT
    vendor_id,
    vendor_name
FROM {{ ref('src_tlc_yellow_trips') }}
WHERE vendor_id IS NOT NULL