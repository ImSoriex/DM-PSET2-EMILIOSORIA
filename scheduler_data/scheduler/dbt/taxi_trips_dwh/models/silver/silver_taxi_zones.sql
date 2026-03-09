{{ config(materialized='view') }}

select
    locationid::int as location_id,
    trim(borough) as borough,
    trim(_zone) as zone,
    trim(service_zone) as service_zone
from bronze.taxi_zone_lookup
where locationid is not null