{{ config(materialized='incremental') }}

with payment_types as (

    select distinct
        payment_type
    from {{ ref('silver_trips') }}
    where payment_type is not null

)

select
    payment_type as payment_type_key,
    case
        when payment_type = 1 then 'card'
        when payment_type = 2 then 'cash'
        else 'other'
    end as payment_type
from payment_types