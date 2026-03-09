{{ config(materialized='incremental') }}

select distinct
    service_type
from {{ ref('silver_trips') }}
where service_type in ('yellow', 'green')