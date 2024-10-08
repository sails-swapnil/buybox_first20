from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Set default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'dbt_seed_snowflake',
    default_args=default_args,
    description='Seed data into Snowflake using dbt',
    schedule_interval=None,  # Manual trigger
    catchup=False,
) as dag:

    # BashOperator to run the dbt seed command
    dbt_seed = BashOperator(
        task_id='dbt_seed',
        bash_command='dbt seed --profiles-dir .',
        env={
            'SNOWFLAKE_USER': 'sravanibogadi',
            'SNOWFLAKE_PASSWORD': 'Bogadi@1',
            'SNOWFLAKE_ACCOUNT': 'yuuikmo-mw43808',
            'SNOWFLAKE_ROLE': 'accountadmin',
            'SNOWFLAKE_WAREHOUSE': 'COMPUTE_WH',
        },
    )

    dbt_seed
