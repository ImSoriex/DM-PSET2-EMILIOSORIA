{{ config(materialized='incremental') }}

with dates as (

    select distinct
        pickup_ts::date as pickup_date
    from {{ ref('silver_trips') }}
    where pickup_ts is not null

)

select
    to_char(pickup_date, 'YYYYMMDD')::int as date_key,
    pickup_date,
    extract(year from pickup_date)::int as year,
    extract(month from pickup_date)::int as month,
    extract(day from pickup_date)::int as day,
    extract(dow from pickup_date)::int as day_of_week,
    extract(quarter from pickup_date)::int as quarter
from dates