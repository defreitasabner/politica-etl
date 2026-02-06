#!/bin/bash

DATA_DIR="../data/"
if [ -d "$DATA_DIR" ]; then
    rm -rf "${DATA_DIR:?}"/{*,.*}
    echo "All data files and subdirectories in '$DATA_DIR' have been removed."
else
    echo "Directory '$DATA_DIR' does not exist."
fi