#!/bin/bash

# Run PSA scripts
echo "PSA start"
python3 pipelines/psa/psa_csv_patients_ingest.py
python3 pipelines/psa/psa_ndjson_cases_ingest.py
echo "PSA created"

# Run RAW DV scripts
echo "RAW DV start"
python3 pipelines/raw_dv/raw_dv_csv_patiens.py
python3 pipelines/raw_dv/raw_dv_ndjson_cases.py
echo "RAW DV created"

# Run Business DV scripts
echo "Business DV start"
python3 pipelines/business_dv/business_dv_patients.py
python3 pipelines/business_dv/business_dv_cases.py
python3 pipelines/business_dv/business_dv_icpc.py
echo "Business DV created"

# Build View
echo "View start"
python3 pipelines/views/views.py
echo "View created"

# Create Plots
echo "Plot start"
python3 pipelines/plots/medgate_plots.py
echo "Plot created"

echo "All scripts have been executed."
