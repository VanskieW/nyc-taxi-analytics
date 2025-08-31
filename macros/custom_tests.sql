{# Check for duplicate trip_id #}
{% test no_duplicate_trip_id(model, column_name) %}
    SELECT {{ column_name }}, COUNT(*) AS dup_count
    FROM {{ model }}
    GROUP BY {{ column_name }}
    HAVING COUNT(*) > 1
{% endtest %}

 {# Fares should be non-negative #}
{% test non_negative_fare(model, column_name) %}
    SELECT *
    FROM {{ model }}
    WHERE {{ column_name }} < 0
{% endtest %}

{# Trip duration must be positive #}
{% test positive_duration(model) %}
    SELECT *
    FROM {{ model }}
    WHERE TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND) <= 0
{% endtest %}