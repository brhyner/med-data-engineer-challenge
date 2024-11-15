with base as (

    select 
        distinct
        case_id
        , case_type
        , patient_id
        , to_timestamp(case_datetime / 1000) AT TIME ZONE 'UTC' as case_datetime -- assuming utc time zone
        , case_closed
        , to_timestamp(case_closed_datetime / 1000) AT TIME ZONE 'UTC' as case_closed_datetime
        , case_closed_reason
        , to_timestamp(updated_at / 1000) AT TIME ZONE 'UTC' as updated_at
    from {{ ref('cases') }}
)

select 
    *
from base