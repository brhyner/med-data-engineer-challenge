with base as (

    select 
        DATE_PART('year', AGE(patient_date_of_birth)) as patient_age
    from {{ ref('mas_dim__patients') }}
    where 
        patient_date_of_birth is not null 
        -- make sure that there are no negative ages
        AND patient_date_of_birth < CURRENT_DATE
)

select 
    AVG(patient_age) as average_age
from base

-- average age
    -- 59

