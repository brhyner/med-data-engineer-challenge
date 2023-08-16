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
WITH average_age AS (
SELECT 
    patient_id
    ,age(current_date,patient_date_of_birth) AS patient_age
    FROM business_dv_patients
    WHERE (current_date - patient_date_of_birth) > 0
    AND patient_date_of_birth is not null
),
patient_cases AS (
SELECT
    patient_id
    ,COUNT(DISTINCT case_id) nr_cases
    FROM link_patient_case
    GROUP BY patient_id
)
SELECT 
    bdv_p.patient_id
    ,patient_age
    ,nr_cases
    FROM business_dv_patients bdv_p
        JOIN patient_cases pc
            ON bdv_p.patient_id=pc.patient_id
        LEFT JOIN average_age aa
            ON aa.patient_id = bdv_p.patient_id
     

;
CREATE OR REPLACE VIEW v_patients_overview AS 
SELECT
    COUNT(DISTINCT patient_id)
    ,avg(patient_age)
    FROM v_patient_stat
;
CREATE OR REPLACE VIEW v_icpc_overview AS 
SELECT
    icpc_codes
    ,count(distinct case_id)
    FROM link_case_icpc
    GROUP BY icpc_codes
    ORDER BY 2 DESC
;
"""
cursor.execute(create_view_query)
connection.commit()


# Close the database connection
cursor.close()
connection.close()