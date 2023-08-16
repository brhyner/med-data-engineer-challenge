import psycopg2
import json
import csv
import time
from datetime import datetime

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
CREATE TABLE IF NOT EXISTS NDJSON_CASES (
    case_id VARCHAR PRIMARY KEY,           
    case_type VARCHAR,        
    patient_id VARCHAR,          
    case_datetime VARCHAR,       
    case_closed VARCHAR,         
    case_closed_datetime VARCHAR,
    case_closed_reason VARCHAR,  
    icpc_codes VARCHAR,          
    updated_at VARCHAR          
);
CREATE TABLE IF NOT EXISTS PSA_CASES (
    sys_hash VARCHAR PRIMARY KEY,
    sys_load_id NUMERIC(20),
    sys_loaded_from VARCHAR,
    json_string JSONB
);
'''
file_path = 'data/cases.ndjson'
cursor.execute(create_table_query)
connection.commit()

# Read and transform data from CSV
with open(file_path, 'r') as ndjson_file:
    timestamp = time.time()
    date_time = datetime.fromtimestamp(timestamp)
    str_date_time = date_time.strftime("%Y%m%d%H%M%S")

    for line in ndjson_file:
        raw_data = json.loads(line)
       
        insert_query ="""
        INSERT INTO PSA_CASES(
        sys_hash,
        sys_load_id,
        sys_loaded_from,
        json_string
        )
        values(
        %s,
        %s,
        'patients.ndjson',
        %s
        )
        """
        psa_data = (
        hash(str(raw_data)),
        str_date_time,
        json.dumps(raw_data)
        )
        cursor.execute(insert_query, (psa_data))
        connection.commit()

# Close the database connection
cursor.close()
connection.close()
