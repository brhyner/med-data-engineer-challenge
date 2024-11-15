#!/bin/bash

# Install dependencies from requirements.txt
if [ -f /requirements.txt ]; then
    echo "Installing dependencies from requirements.txt..."
    pip install -r /requirements.txt
else
    echo "requirements.txt not found!"
    exit 1
fi
