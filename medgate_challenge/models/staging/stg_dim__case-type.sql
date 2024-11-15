with base as (

    select
        1 as case_type
        , 'Triage' as case_type_name
    
    union all 

    select 
        2 as case_type
        , 'Non-Medical' as case_type_name

    union all 

    select 
        3 as case_type
        , 'Medical' as case_type_name

)

select 
    *
from base