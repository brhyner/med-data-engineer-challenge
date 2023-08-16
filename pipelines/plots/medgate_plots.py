import psycopg2
import matplotlib.pyplot as plt
from collections import Counter

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

# Query to retrieve data for analysis
cases_per_patient_query = "SELECT patient_id, nr_cases FROM v_patient_stat;"
common_icpc_codes_query = "SELECT icpc_codes, count FROM v_icpc_overview LIMIT 10"

# Execute queries
cursor.execute(cases_per_patient_query)
cases_per_patient = cursor.fetchall()

cursor.execute(common_icpc_codes_query)
icpc_codes_result = cursor.fetchall()

# Close the database connection
cursor.close()
connection.close()

# Plot the number of cases per patient
patient_ids = [row[0] for row in cases_per_patient]
num_cases = [row[1] for row in cases_per_patient]
plt.figure(figsize=(10, 6))
plt.violinplot(num_cases,showmeans=True, showmedians=True)
plt.xlabel('Nr')
plt.ylabel('Number of Cases')
plt.title('Number of Cases per Patient')
plt.xticks(rotation=45)
plt.grid(True)
plt.show()

# Plot the most common ICPC codes
icpc_codes = [row[0] for row in icpc_codes_result]
icpc_code_counts = [row[1] for row in icpc_codes_result]
plt.figure(figsize=(10, 6))
plt.bar(icpc_codes, icpc_code_counts)
plt.xlabel('ICPC Code')
plt.ylabel('Counts')
plt.title('Most Common ICPC Codes')
plt.xticks(rotation=45)
plt.grid(True)
plt.show()
