#!/bin/bash
set -e

# Initialize the database if it hasn't been initialized yet
if [ ! -f "/opt/airflow/airflow.db" ]; then
  airflow db init && \
  airflow users create \
    --username admin \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email admin@airscholar.com \
    --password admin
fi

$(command -v airflow) db upgrade

# Start the Airflow webserver
exec airflow webserver &

# Start the Airflow scheduler
exec airflow scheduler