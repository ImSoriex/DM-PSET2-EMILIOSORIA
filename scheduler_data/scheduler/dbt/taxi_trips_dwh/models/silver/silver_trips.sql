{{ config(materialized='view') }}

with yellow as (

    select
        vendorid::int as vendor_id,
        tpep_pickup_datetime::timestamp as pickup_ts,
        tpep_dropoff_datetime::timestamp as dropoff_ts,
        passenger_count::int as passenger_count,
        trip_distance::numeric(10,2) as trip_distance,
        pulocationid::int as pu_location_id,
        dolocationid::int as do_location_id,
        payment_type::int as payment_type,
        fare_amount::numeric(10,2) as fare_amount,
        tip_amount::numeric(10,2) as tip_amount,
        total_amount::numeric(10,2) as total_amount,
        source_month,
        service_type
    from bronze.yellow_trips

),

green as (

    select
        vendorid::int as vendor_id,
        lpep_pickup_datetime::timestamp as pickup_ts,
        lpep_dropoff_datetime::timestamp as dropoff_ts,
        passenger_count::int as passenger_count,
        trip_distance::numeric(10,2) as trip_distance,
        pulocationid::int as pu_location_id,
        dolocationid::int as do_location_id,
        payment_type::int as payment_type,
        fare_amount::numeric(10,2) as fare_amount,
        tip_amount::numeric(10,2) as tip_amount,
        total_amount::numeric(10,2) as total_amount,
        source_month,
        service_type
    from bronze.green_trips

),

unioned as (

    select * from yellow
    union all
    select * from green

),

cleaned as (

    select *
    from unioned
    where pickup_ts is not null
      and dropoff_ts is not null
      and pickup_ts <= dropoff_ts
      and trip_distance >= 0
      and total_amount >= 0
      and pickup_ts::date >= date '2022-01-01'
      and pickup_ts::date <  date '2026-01-01'
      and dropoff_ts::date >= date '2022-01-01'
      and dropoff_ts::date <  date '2026-01-01'

)

select
    c.vendor_id,
    c.pickup_ts,
    c.dropoff_ts,
    c.passenger_count,
    c.trip_distance,
    c.pu_location_id,
    c.do_location_id,
    c.payment_type,
    c.fare_amount,
    c.tip_amount,
    c.total_amount,
    c.source_month,
    c.service_type,

    pu.location_id as pu_zone_id,
    pu.borough as pu_borough,
    pu.zone as pu_zone,
    pu.service_zone as pu_service_zone,

    doz.location_id as do_zone_id,
    doz.borough as do_borough,
    doz.zone as do_zone,
    doz.service_zone as do_service_zone

from cleaned c
inner join {{ ref('silver_taxi_zones') }} pu
    on c.pu_location_id = pu.location_id
inner join {{ ref('silver_taxi_zones') }} doz
    on c.do_location_id = doz.location_id