## Medgate Coding Challenge: Data Engineering

Welcome to our coding challenge for data engineers! In this challenge, you'll be given a dataset containing information about a fictional digital health company. Your task is to extract the necessary data from this dataset, transform it, and load it into a PostgreSQL or Microsoft SQL Server database running in a Docker container.

### Instructions

To complete this challenge, follow the steps below:

1. Fork this repository
2. Clone the forked repository to your local machine
3. Create a virtual environment and activate it (use `pipenv` or `conda`)
4. Write Python code to extract, transform, and load the data into a PostgreSQL or Microsoft SQL Server database running in a Docker container. Choose the database you are more comfortable with. 
   - The goal is to create a database schema that Data Analysts can use to analyze the data
   - You can use any Python packages or Python-based workflow management tool (e.g., Airflow, Prefect) of your choice to complete this task. If you use a workflow management tool, please add it to the docker-compose file.
5. Optional: Write tests for your code to ensure it's working correctly
6. Analyze the number of cases per patient, the average age, and the most common ICPC codes
7. Add the used packages to the `requirements.txt` or `environment.yml` file
8. Commit your changes and push them to your forked repository
9. Create a pull request into the main branch

### Datasets

#### Patients

The dataset you'll be working with contains information about a fictional digital health company's patients. The data is stored in a CSV file called `patients.csv`, located in the `data` directory.

The file has the following columns:

- `patient_id`: The unique ID of the patient
- `patient_name`: The name of the patient
- `patient_email`: The email of the patient
- `patient_phone`: The phone number of the patient
- `patient_address`: The address of the patient
- `patient_city`: The city of the patient
- `patient_state`: The state of the patient
- `patient_zip`: The zip code of the patient
- `patient_country`: The country of the patient
- `patient_date_of_birth`: The date of birth of the patient
- `updated_at`: The datetime the patient was last updated

#### Cases

The dataset you'll be working with contains information about a fictional digital health company's cases. The data is stored in a CSV file called `cases.ndjson`, located in the `data` directory.

The file has the following columns:

 - `case_id`: The unique ID of the case
 - `case_type`: The type of case, 1 for triage, 2 for non-medical, 3 for medical
 - `patient_id`: The unique ID of the patient
 - `case_datetime`: The datetime the case was created
 - `case_closed`: Whether the case is closed or not
 - `case_closed_datetime`: The datetime the case was closed
 - `case_closed_reason`: The reason the case was closed
 - `icpc_codes`: The ICPC codes associated with the case, whitespace separated
 - `updated_at`: The datetime the case was last updated


### Requirements

Your code should satisfy the following requirements:

1. Use the provided `docker-compose.yml` file to spin up either a PostgreSQL or a Microsoft SQL Server container
2. Create a database schema to store the data
3. Extract the necessary data from the cases & patients file
4. Transform the data to fit the schema
5. Load the data into the database
Optional:
6. Write tests for your code
7. Create a Dockerfile to build a Docker image containing your code + integrate it in the `docker-compose.yml` file


### Docker

Here's an example `docker-compose.yml` file that can be used to spin up either a PostgreSQL or a Microsoft SQL Server container:

```yaml
version: "3.9"

services:
  db:
    image: postgres:13-alpine # Use "mcr.microsoft.com/mssql/server:2019-latest" for MSSQL
    restart: always
    environment:
      POSTGRES_USER: db_user # Use "SA_PASSWORD" for MSSQL
      POSTGRES_PASSWORD: db_password # Use your desired password for MSSQL
      POSTGRES_DB: db_name # Use your desired database name for MSSQL
    volumes:
      - ./init:/docker-entrypoint-initdb.d # Use "./init:/var/opt/mssql/init" for MSSQL
    ports:
      - "5432:5432" # Use "1433:1433" for MSSQL
```

To use this `docker-compose.yml` file, simply run `docker-compose up -d` in the same directory as the file. This will spin up a container running either PostgreSQL or Microsoft SQL Server, depending on which image is used.

Note that for Microsoft SQL Server, you will need to create an `init` directory with a SQL script to create your database and any necessary tables. The script should be named `init.sql`. For PostgreSQL, this step is not necessary, as the `POSTGRES_DB` environment variable will automatically create a database with the specified name.

Once the container is running, you can connect to it using a database client such as `psql` or `sqlcmd`, depending on which database you chose.

### Submission

When you're finished, create a PR to the main branch of this repository. We'll review your code and get back to you as soon as possible.
