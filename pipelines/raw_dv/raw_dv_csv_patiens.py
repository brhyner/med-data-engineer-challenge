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
CREATE TABLE IF NOT EXISTS hub_patients (
    sys_hash VARCHAR ,
    sys_load_id NUMERIC(20),
    sys_loaded_from VARCHAR,
    patient_id VARCHAR PRIMARY KEY
);
CREATE TABLE IF NOT EXISTS sat_address (
    sys_hash VARCHAR,
    sys_load_id NUMERIC(20),
    sys_loaded_from VARCHAR,
    patient_id VARCHAR REFERENCES hub_patients(patient_id),
    patient_name VARCHAR,
    patient_email VARCHAR,
    patient_phone VARCHAR,
    patient_address VARCHAR,
    patient_city VARCHAR,
    patient_state VARCHAR,
    patient_zip VARCHAR,
    patient_country VARCHAR,
    patient_date_of_birth VARCHAR,
    updated_at VARCHAR	  
);

'''
# Execute query
cursor.execute(create_table_query)
connection.commit()


raw_dv_query = """
INSERT INTO hub_patients
SELECT 
    sys_hash
    ,sys_load_id
    ,sys_loaded_from
    ,json_string->>'patient_id' as patient_id
FROM psa_patients src
WHERE 1=1
AND src.sys_load_id > COALESCE((SELECT MAX(trg.sys_load_id) FROM hub_patients trg),0)
AND NOT EXISTS (SELECT 1 FROM hub_patients trg WHERE src.json_string->>'patient_id'=trg.patient_id);

INSERT INTO sat_address
SELECT 
    sys_hash
    ,sys_load_id
    ,sys_loaded_from
    ,json_string->>'patient_id' as patient_id
    ,json_string->>'patient_name' as patient_name
    ,json_string->>'patient_email' as patient_email
    ,json_string->>'patient_phone' as patient_phone
    ,json_string->>'patient_address' as patient_address
    ,json_string->>'patient_city' as patient_city
    ,json_string->>'patient_state' as patient_stat
    ,json_string->>'patient_zip' as patient_zip
    ,json_string->>'patient_country' as patient_country
    ,json_string->>'patient_date_of_birth' as patient_date_of_birth
    ,json_string->>'updated_at' as updated_at
FROM psa_patients src
WHERE 1=1
AND src.sys_load_id > COALESCE((SELECT MAX(trg.sys_load_id) FROM sat_address trg),0)
AND NOT EXISTS (SELECT 1 FROM sat_address trg WHERE src.json_string->>'patient_id'=trg.patient_id);


"""
# Execute query
cursor.execute(raw_dv_query)
connection.commit()
# Close the database connection
cursor.close()
connection.close()