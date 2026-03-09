{{ config(materialized='incremental') }}

select
    row_number() over (
        order by pickup_ts, dropoff_ts, pu_zone_id, do_zone_id, vendor_id
    ) as trip_key,

    to_char(pickup_ts::date, 'YYYYMMDD')::int as pickup_date_key,
    pickup_ts::date as pickup_date,

    pu_zone_id as pu_zone_key,
    do_zone_id as do_zone_key,

    service_type,
    payment_type,
    vendor_id,

    pickup_ts,
    dropoff_ts,

    passenger_count,
    trip_distance,

    fare_amount,
    tip_amount,
    total_amount

from {{ ref('silver_trips') }}

where pickup_ts >= timestamp '2022-01-01'
  and pickup_ts <  timestamp '2026-01-01'