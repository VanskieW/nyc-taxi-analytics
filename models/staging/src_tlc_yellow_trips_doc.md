# src_tlc_yellow_trips

{% docs src_tlc_yellow_trips_doc %}


This staging table cleans and standardizes raw NYC Yellow Taxi trip data for 2021.  

**Purpose:**
- Convert raw timestamps to UTC
- Map vendor IDs to vendor names
- Prepare data for fact table aggregation

**Columns:**
- `trip_id`: surrogate key
- `vendor_id`: raw vendor code
- `pickup_datetime`, `dropoff_datetime`: timestamps in UTC
- `fare_amount`, `tip_amount`, `total_amount`: monetary values

{% enddocs %}
