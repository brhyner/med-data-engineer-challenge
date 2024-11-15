with patients as (

    select 
        patient_id
    from {{ ref('mas_dim__patients') }}
)

, cases as (

    select 
        case_id
        , patient_id
        , case_closed

    from {{ ref('mas_fct__cases') }}
    where 
        case_closed is not null 

)

, joined as (

    select 
        p.patient_id
        , c.case_id
        , c.case_closed
    from patients as p 
    left join cases as c 
    on 
        p.patient_id = c.patient_id
)

, aggregation as (

    select 
        patient_id 
        , count(
            distinct 
            CASE 
                WHEN not case_closed THEN case_id
            END
        ) as open_cases
        , count(
            distinct 
            CASE 
                WHEN case_closed THEN case_id
            END
        ) as closed_cases
        , count(distinct case_id) as total_cases
    from joined 

    group by 
        1
)

select 
    *
    -- not really clean, but depends what exactly the goal is
        -- number of cases per patient as a rollup or overall number of cases per patient (i.e. average cases per patient)
        -- from an analytical point of view, the average is more useful. 
        -- operationally, the per patient numbers are more useful.
    , AVG(total_cases) OVER () as average_cases_per_patient_overall
from aggregation 