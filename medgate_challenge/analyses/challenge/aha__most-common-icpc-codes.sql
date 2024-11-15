{% set topn = 10 %}

with base as (

    select 
        icpc_code
        , count(distinct icpc_code_id) as number_of_cases
    from {{ ref('mas_fct__cases-with-icpc-codes') }}

    group by 
        1
)

, result as (

    select 
        *
    from base
    order by 
        2 DESC
    limit {{ topn }}
)

select 
    * 
from result