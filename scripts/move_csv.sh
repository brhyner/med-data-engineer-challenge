#!/bin/bash

# Variables
SOURCE_DIR="/opt/airflow/data/"  
CONTAINER_NAME="dbt"                   
DESTINATION_PATH="/usr/app/seeds" 

# Loop through each .csv file in the source directory and copy it to the dbt container
for csv_file in "$SOURCE_DIR"*.csv; do
    docker cp "$csv_file" "$CONTAINER_NAME:$DESTINATION_PATH"
    echo "Copied $csv_file to $CONTAINER_NAME:$DESTINATION_PATH"
done

