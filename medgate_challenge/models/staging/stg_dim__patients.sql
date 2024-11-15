with base as (

    select
        *
    from {{ ref('patients') }}
)

select 
    *
from base