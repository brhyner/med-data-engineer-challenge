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
CREATE TABLE IF NOT EXISTS hub_cases (
    sys_hash VARCHAR,
    sys_load_id NUMERIC(20),
    sys_loaded_from VARCHAR,
    case_id VARCHAR PRIMARY KEY
);
CREATE TABLE IF NOT EXISTS sat_cases (
    sys_hash VARCHAR,
    sys_load_id NUMERIC(20),
    sys_loaded_from VARCHAR,
    case_id VARCHAR REFERENCES hub_cases(case_id),           
    case_type VARCHAR,        
    patient_id VARCHAR,          
    case_datetime VARCHAR,       
    case_closed VARCHAR,         
    case_closed_datetime VARCHAR,
    case_closed_reason VARCHAR,  
    icpc_codes VARCHAR,          
    updated_at VARCHAR
	   
);
CREATE TABLE IF NOT EXISTS link_patient_case (
sys_load_id NUMERIC(20),
sys_loaded_from VARCHAR,
case_id VARCHAR REFERENCES hub_cases(case_id),
patient_id VARCHAR REFERENCES hub_patients(patient_id)   
);
CREATE TABLE IF NOT EXISTS link_case_icpc (
sys_load_id NUMERIC(20),
sys_loaded_from VARCHAR,
case_id VARCHAR REFERENCES hub_cases(case_id),
patient_id VARCHAR
)

'''
# Execute query
cursor.execute(create_table_query)
connection.commit()




raw_dv_query = """

INSERT INTO hub_cases
SELECT 
    sys_hash
    ,sys_load_id
    ,sys_loaded_from
    ,json_string->>'case_id' as case_id
FROM psa_cases src
WHERE 1=1
AND src.sys_load_id > COALESCE((SELECT MAX(trg.sys_load_id) FROM hub_cases trg),0)
AND NOT EXISTS (SELECT 1 FROM hub_cases trg WHERE src.json_string->>'case_id'=trg.case_id);

INSERT INTO sat_cases
SELECT 
    sys_hash
    ,sys_load_id
    ,sys_loaded_from
    ,json_string->>'case_id' as case_id           
    ,json_string->>'case_type' as case_type             
    ,json_string->>'case_datetime' as case_datetime       
    ,json_string->>'case_closed' as case_closed         
    ,json_string->>'case_closed_datetime' as case_closed_datetime
    ,json_string->>'case_closed_reason' as case_closed_reason          
    ,json_string->>'updated_at' as updated_at
FROM psa_cases src
WHERE 1=1
AND src.sys_load_id > COALESCE((SELECT MAX(trg.sys_load_id) FROM sat_cases trg),0)
AND NOT EXISTS (SELECT 1 FROM sat_cases trg WHERE src.json_string->>'case_id'=trg.case_id);

INSERT INTO link_patient_case
SELECT 
    sys_load_id
    ,sys_loaded_from
    ,json_string->>'case_id' as case_id                  
    ,json_string->>'patient_id' as patient_id
FROM psa_cases src
WHERE 1=1
AND src.sys_load_id > COALESCE((SELECT MAX(trg.sys_load_id) FROM link_patient_case trg),0)
AND NOT EXISTS (SELECT 1 FROM link_patient_case trg WHERE src.json_string->>'case_id'=trg.case_id);

INSERT INTO link_case_icpc
SELECT
    sys_load_id
    ,sys_loaded_from
    ,json_string->>'case_id' as case_id           
    ,unnest(string_to_array(json_string->>'icpc_codes', ' ')) AS icpc_code
FROM psa_cases src
WHERE 1=1
AND src.sys_load_id > COALESCE((SELECT MAX(trg.sys_load_id) FROM link_case_icpc trg),0)
AND NOT EXISTS (SELECT 1 FROM link_case_icpc trg WHERE src.json_string->>'case_id'=trg.case_id);

"""
# Execute query
cursor.execute(raw_dv_query)
connection.commit()
# Close the database connection
cursor.close()
connection.close()