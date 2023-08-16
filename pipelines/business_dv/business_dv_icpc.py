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
CREATE TABLE IF NOT EXISTS business_dv_icpc (
    sys_load_id NUMERIC(20)
    ,sys_loaded_from VARCHAR
    ,case_id VARCHAR
    ,icpc_code VARCHAR   

);
"""
cursor.execute(create_table_query)
connection.commit()

insert_table_query = """
INSERT INTO business_dv_icpc
    SELECT DISTINCT
        sys_load_id
        ,sys_loaded_from
        ,case_id
        ,icpc_codes
    FROM link_case_icpc
    WHERE sys_load_id = (SELECT MAX(sys_load_id) FROM link_case_icpc src)
"""
cursor.execute(insert_table_query)
connection.commit()

# Close the database connection
cursor.close()
connection.close()