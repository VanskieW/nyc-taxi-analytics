SELECT DISTINCT
    store_and_fwd_flag,
    store_and_fwd_description AS flag_description
FROM {{ ref('src_tlc_yellow_trips') }}
