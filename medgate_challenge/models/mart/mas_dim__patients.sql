{{
  config(
    materialized = 'incremental',
    unique_key = 'patient_id',
    incremental_strategy = 'merge',
    on_schema_change = 'fail',
    tags = [
        'daily'
        , 'patients'
    ]
    )
}}

select 
    *
from {{ ref('stg_dim__patients') }}

{% if is_incremental() %}
where 
    updated_at >= coalesce((select max(updated_at) from {{ this }}), '1900-01-01')
{% endif %}