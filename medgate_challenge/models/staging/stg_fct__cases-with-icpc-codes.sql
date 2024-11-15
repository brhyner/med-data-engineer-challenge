with base as (

    select
        -- had to add the default version, otherwise it would have thrown an error, because it could not find the postgres version of the macro
        {{ dbt_utils.default__generate_surrogate_key(['case_id', 'icpc_codes']) }} as icpc_code_id
        , case_id
        , icpc_codes as icpc_code
        , to_timestamp(updated_at / 1000) AT TIME ZONE 'UTC' as updated_at -- assuming utc time zone
    from {{ ref('cases') }}
)

select 
    *
from base