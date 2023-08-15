import psycopg2
import json

# Database connection parameters
db_params = {
    'dbname': 'db_name',
    'user': 'db_user',
    'password': 'db_password',
    'host': 'localhost',
    'port': '5433'
}

# Connect to the PostgreSQL database
connection = psycopg2.connect(**db_params)
cursor = connection.cursor()

create_view_query = """
CREATE OR REPLACE VIEW v_patient_stat AS
with average_age as (
SELECT 
    patient_id
    ,age(current_date,patient_date_of_birth) as patient_age
    from business_dv_patients
    where (current_date - patient_date_of_birth) > 0
    and patient_date_of_birth is not null
),
patient_cases as (
SELECT
    patient_id
    ,count(distinct case_id) nr_cases
    from public.link_patient_case
    group by patient_id
)
SELECT 
    bdv_p.patient_id
    ,patient_age
    ,nr_cases
    FROM business_dv_patients bdv_p
        JOIN patient_cases pc
            on bdv_p.patient_id=pc.patient_id
        LEFT JOIN average_age aa
            on aa.patient_id = bdv_p.patient_id
     

;
CREATE OR REPLACE VIEW v_patients_overview AS 
SELECT
    count(distinct patient_id)
    ,avg(patient_age)
    from v_patient_stat
;
CREATE OR REPLACE VIEW v_icpc_overview AS 
SELECT
    icpc_codes
    ,count(distinct case_id)
    from link_case_icpc
    group by icpc_codes
    order by 2 desc
;
"""
cursor.execute(create_view_query)
connection.commit()


# Close the database connection
cursor.close()
connection.close()