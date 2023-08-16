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

create_table_query = """
CREATE TABLE IF NOT EXISTS business_dv_cases (
    sys_hash VARCHAR
    ,sys_load_id NUMERIC(20)
    ,sys_loaded_from VARCHAR
    ,case_id VARCHAR
    ,case_type VARCHAR            
    ,case_datetime TIMESTAMP   
    ,case_closed BOOLEAN         
    ,case_closed_datetime TIMESTAMP
    ,case_closed_reason VARCHAR  
    ,updated_at TIMESTAMP
);
"""
cursor.execute(create_table_query)
connection.commit()

insert_table_query = """
INSERT INTO business_dv_cases
    SELECT DISTINCT
        sys_hash
        ,sys_load_id
        ,sys_loaded_from
        ,case_id
        ,case_type        
        ,to_timestamp((case_datetime::numeric/1000.0)::numeric) as case_datetime
        ,case_closed::boolean     
        ,to_timestamp((case_closed_datetime::numeric/1000.0)::numeric) as case_closed_datetime
        ,case_closed_reason   
        ,to_timestamp((updated_at::numeric/1000.0)::numeric) as updated_at
    FROM sat_cases
    WHERE sys_load_id = (SELECT MAX(sys_load_id) FROM sat_cases src)
"""
cursor.execute(insert_table_query)
connection.commit()

# Close the database connection
cursor.close()
connection.close()