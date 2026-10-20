# Airflow 3 with dbt. dbt lives in its own virtualenv so its dependencies can never
# break Airflow's (both pull in many of the same libraries at different versions).
FROM apache/airflow:3.3.2

USER root
RUN python -m venv /opt/dbt-venv \
 && /opt/dbt-venv/bin/pip install --no-cache-dir "dbt-postgres>=1.9,<2" \
 && /opt/dbt-venv/bin/dbt --version \
 && mkdir -p /opt/airflow/landing \
 && chown airflow:0 /opt/airflow/landing
USER airflow
