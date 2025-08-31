WITH dates AS (
    SELECT
        DATEADD(day, SEQ4(), '2021-01-01') AS date_day
    FROM TABLE(GENERATOR(ROWCOUNT => 365*5))  -- Generate 5 years of dates
)
SELECT
    date_day::DATE AS date,
    EXTRACT(year FROM date_day) AS year,
    EXTRACT(month FROM date_day) AS month,
    EXTRACT(week FROM date_day) AS week,
    EXTRACT(day FROM date_day) AS day,
    CASE WHEN EXTRACT(dow FROM date_day) IN (0,6) THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    TO_CHAR(date_day, 'DY') AS day_name,         -- Mon, Tue, etc.
    TO_CHAR(date_day, 'MON') AS month_name,      -- JAN, FEB, etc.
    EXTRACT(quarter FROM date_day) AS quarter
FROM dates
ORDER BY date_day
