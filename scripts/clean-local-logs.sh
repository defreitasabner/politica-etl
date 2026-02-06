#!/bin/bash

LOG_DIR="../airflow_pipeline/logs/"
if [ -d "$LOG_DIR" ]; then
    rm -rf "${LOG_DIR:?}"/*
    echo "All log files and subdirectories in '$LOG_DIR' have been removed."
else
    echo "Directory '$LOG_DIR' does not exist."
fi