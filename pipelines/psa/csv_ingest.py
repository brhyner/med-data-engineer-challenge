import psycopg2
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
CREATE TABLE IF NOT EXISTS CSV_PATIENTS (
    patient_id VARCHAR PRIMARY KEY,
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

cursor.execute(create_table_query)
connection.commit()

# Read and transform data from CSV
with open('../data/patients.csv', 'r') as csv_file:
    csv_reader = csv.reader(csv_file)
    
    next(csv_reader)  # Skip header

    for row in csv_reader:
        # Apply transformation logic here if needed
        #transformed_data = row[0].upper()  # Example transformation
        
        patient_id = row[0]
        patient_name = row[1]
        patient_email = row[2]
        patient_phone = row[3]
        patient_address = row[4]
        patient_city = row[5]
        patient_state = row[6]
        patient_zip = row[7]
        patient_country = row[8]
        patient_date_of_birth = row[9]
        updated_at = row[10]
        
        # Insert data into PostgreSQL
        #insert_query = "Select top(10) From "
        insert_query ="""
        INSERT INTO CSV_PATIENTS (
        patient_id, patient_name, patient_email, patient_phone,
        patient_address, patient_city, patient_state, patient_zip,
        patient_country, patient_date_of_birth, updated_at
        ) 
        VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        );
        """
        data = (
        patient_id,
        patient_name, 
        patient_email,
        patient_phone, 
        patient_address, 
        patient_city,
        patient_state, 
        patient_zip, 
        patient_country,
        patient_date_of_birth, 
        updated_at
        )
        cursor.execute(insert_query, (data))
        connection.commit()

# Close the database connection
cursor.close()
connection.close()
