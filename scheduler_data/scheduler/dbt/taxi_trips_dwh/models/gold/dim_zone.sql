{{ config(materialized='incremental') }}

select
    location_id as zone_key,
    borough,
    zone,
    service_zone
from {{ ref('silver_taxi_zones') }} s
where not exists (

    select 1
    from gold.dim_zone d
    where d.zone_key = s.location_id

)