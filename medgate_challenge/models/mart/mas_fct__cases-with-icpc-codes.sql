{{
  config(
    materialized = 'incremental',
    unique_key = 'icpc_code_id',
    incremental_strategy = 'merge',
    on_schema_change = 'fail',
    tags = [
        'daily'
        , 'cases'
    ]
    )
}}

select 
    *
from {{ ref('stg_fct__cases-with-icpc-codes') }}

{% if is_incremental() %}
where 
    updated_at >= coalesce((select max(updated_at) from {{ this }}), '1900-01-01')
{% endif %}