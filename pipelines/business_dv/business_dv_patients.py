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

create_table_query = '''
CREATE TABLE IF NOT EXISTS business_dv_patients (
    sys_hash VARCHAR 
    ,sys_load_id NUMERIC(20)
    ,sys_loaded_from VARCHAR
    ,patient_id VARCHAR PRIMARY KEY
    ,patient_name VARCHAR
    ,patient_email VARCHAR
    ,patient_phone VARCHAR
    ,patient_address VARCHAR
    ,patient_city VARCHAR
    ,patient_state VARCHAR
    ,patient_zip VARCHAR
    ,patient_country VARCHAR
    ,patient_date_of_birth DATE
    ,updated_at TIMESTAMP  
);

'''
# Execute query
cursor.execute(create_table_query)
connection.commit()

business_dv_query = """
INSERT INTO business_dv_patients
SELECT distinct
    sys_hash
    ,sys_load_id
    ,sys_loaded_from
    ,patient_id
    ,patient_name
    ,patient_email 
    ,patient_phone
    ,patient_address
    ,patient_city
    ,patient_state
    ,patient_zip
    ,patient_country
    ,CASE
        WHEN patient_date_of_birth != ''
            THEN patient_date_of_birth::date
        ELSE NULL
        END AS patient_date_of_birth
    ,updated_at::timestamp
FROM sat_address
"""

# Execute query
cursor.execute(business_dv_query)
connection.commit()


# Close the database connection
cursor.close()
connection.close()