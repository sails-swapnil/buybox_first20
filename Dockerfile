FROM apache/airflow:2.5.0  

# Install dbt
RUN pip install dbt-snowflake

