from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import json
import pandas as pd

def preprocess_data(cases_path_json: str, cases_path_csv: str):
    """
    Processes the NDJSON file, splits the ICPC codes, and explodes the DataFrame.
    
    :param cases_path_json: Path to the input NDJSON file.
    :param cases_path_csv: Path to the output CSV file.
    :return: None (saves the processed DataFrame as a CSV)
    """
    # open files
    with open(cases_path_json, 'r') as cases_file:
        # read each line as a JSON object and load it into a list
        data = [json.loads(line) for line in cases_file]
    
    # Convert the list of JSON objects to a DataFrame
    df = pd.DataFrame(data)
    
    # Split the whitespace-separated values in 'icpc_codes' into a list
    df['icpc_codes'] = df['icpc_codes'].str.split()
    
    # Explode the 'icpc_codes' column so each code gets its own row
    df_exploded = df.explode('icpc_codes', ignore_index=True)
    
    # Save the exploded DataFrame to a CSV
    df_exploded.to_csv(cases_path_csv, index=False)


# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

# Define the DAG
with DAG(
    'medgate_pipeline',
    default_args=default_args,
    schedule_interval=None, # for continuous scheduling use e.g. '@daily' or directly a cron expression
    start_date=datetime(2024, 1, 1),
) as dag:
    
    # Task to install dependencies
    install_dependencies = BashOperator(
        task_id='install_dependencies',
        bash_command="/opt/airflow/scripts/install_dependencies.sh ",
        dag=dag
    )
    
    # Task to run the Python script for preprocessing
    run_python_script = PythonOperator(
        task_id='run_python_script',
        python_callable=preprocess_data,  # Call the preprocess_data function
        op_args=['/opt/airflow/data/cases.ndjson', '/opt/airflow/data/cases.csv'],  # Pass file paths as arguments
    )
    
    # Task to move all CSV files to the dbt container
    move_csv_files_to_dbt = BashOperator(
        task_id='move_csv_files_to_dbt',
        bash_command=(
            "/opt/airflow/scripts/move_csv.sh "
        )
    )
    
    # Task to run dbt commands
    # technically, I could run this using DbtTaskGroup() from dbt-airflow package
    # the main difference would be that each model is run separately which generates a more detailed dag in airflow. 
    # Without the detail, the task is just one step in the whole process and the individual models cannot be inspected. 
    # of course, one can always check the logs, there all the information will be present.
    run_dbt_commands = BashOperator(
        task_id='run_dbt_commands',
        bash_command=(
            "docker exec dbt "
            "/bin/bash -c 'cd /usr/app && dbt deps && dbt build' "
        )
    )

    # Define task dependencies
    install_dependencies >> run_python_script >> move_csv_files_to_dbt >> run_dbt_commands
