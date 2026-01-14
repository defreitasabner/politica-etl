FROM apache/airflow:2.8.1-python3.11

USER root
RUN apt-get update && apt-get install -y git && apt-get clean

USER airflow

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.1/constraints-3.11.txt"