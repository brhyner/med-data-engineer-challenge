import psycopg2
import json
import csv

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
# Create the table
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
'''

cursor.execute(create_table_query)
connection.commit()

# Read and transform data from CSV
with open('../data/cases.ndjson', 'r') as ndjson_file:
    for line in ndjson_file:
        data = json.loads(line)
        transformed_data = {

        'case_id'               : data['case_id'],
        'case_type'             : data['case_type'],
        'patient_id'            : data['patient_id'],
        'case_datetime'         : data['case_datetime'],
        'case_closed'           : data['case_closed'],
        'case_closed_datetime'  : data['case_closed_datetime'],
        'case_closed_reason'    : data['case_closed_reason'],
        'icpc_codes'            : data['icpc_codes'],
        'updated_at'            : data['updated_at']
        
        }
        
        
        # Insert data into PostgreSQL
        #insert_query = "Select top(10) From "
        insert_query ="""
        INSERT INTO NDJSON_CASES (
        case_id,             
        case_type,           
        patient_id,          
        case_datetime,       
        case_closed,         
        case_closed_datetime,
        case_closed_reason,  
        icpc_codes,          
        updated_at          
        ) 
        VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s
        );
        """
        data = (
        transformed_data['case_id'],             
        transformed_data['case_type'],           
        transformed_data['patient_id'],          
        transformed_data['case_datetime'],       
        transformed_data['case_closed'],         
        transformed_data['case_closed_datetime'],
        transformed_data['case_closed_reason'],  
        transformed_data['icpc_codes'],          
        transformed_data['updated_at']          
        )
        cursor.execute(insert_query, (data))
        connection.commit()

# Close the database connection
cursor.close()
connection.close()
