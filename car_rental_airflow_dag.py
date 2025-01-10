from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitPySparkJobOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python import PythonOperator
from airflow.models.param import Param

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'car_rental_data_pipeline',
    default_args=default_args,
    description='Pipeline for processing car rental data',
    schedule_interval=None,  # Manual execution only
    start_date=datetime(2024, 8, 2),  # DAG's start date
    catchup=False,  # Do not backfill missing runs
    tags=['dev'],  # Tags for categorization
    params={
        'execution_date': Param(default='NA', type='string', description='Execution date in yyyymmdd format'),
    }
)

# Function to determine the execution date
def get_execution_date(ds_nodash, **kwargs):
    """
    Determine the execution date to use in downstream tasks.
    
    Args:
        ds_nodash (str): Default Airflow execution date in 'yyyymmdd' format.
        **kwargs: Additional context provided by Airflow.
    
    Returns:
        str: Execution date to use in downstream tasks.
    """
    execution_date = kwargs['params'].get('execution_date', 'NA')
    if execution_date == 'NA':
        execution_date = ds_nodash
    return execution_date

# Task: Determine the execution date
get_execution_date_task = PythonOperator(
    task_id='get_execution_date',
    python_callable=get_execution_date,
    provide_context=True,  # Provides Airflow's context to the Python callable
    op_kwargs={'ds_nodash': '{{ ds_nodash }}'},
    dag=dag,
)

# Task: Perform SCD2 merge on the customer dimension
merge_customer_dim = SnowflakeOperator(
    task_id='merge_customer_dim',
    snowflake_conn_id='snowflake_conn_v2',  # Connection ID for Snowflake
    sql="""
        MERGE INTO customer_dim AS target
        USING (
            SELECT
                $1 AS customer_id,
                $2 AS name,
                $3 AS email,
                $4 AS phone
            FROM @car_rental_data_stage/customers_{{ ti.xcom_pull(task_ids='get_execution_date') }}.csv (FILE_FORMAT => 'csv_format')
        ) AS source
        ON target.customer_id = source.customer_id AND target.is_current = TRUE
        WHEN MATCHED AND (
            target.name != source.name OR
            target.email != source.email OR
            target.phone != source.phone
        ) THEN
            UPDATE SET target.end_date = CURRENT_TIMESTAMP(), target.is_current = FALSE;
    """,
    dag=dag,
)

# Task: Insert new records into the customer dimension table
insert_customer_dim = SnowflakeOperator(
    task_id='insert_customer_dim',
    snowflake_conn_id='snowflake_conn_v2',
    sql="""
        INSERT INTO customer_dim (customer_id, name, email, phone, effective_date, end_date, is_current)
        SELECT
            $1 AS customer_id,
            $2 AS name,
            $3 AS email,
            $4 AS phone,
            CURRENT_TIMESTAMP() AS effective_date,
            NULL AS end_date,
            TRUE AS is_current
        FROM @car_rental_data_stage/customers_{{ ti.xcom_pull(task_ids='get_execution_date') }}.csv (FILE_FORMAT => 'csv_format');
    """,
    dag=dag,
)

# Constants for Dataproc configuration
CLUSTER_NAME = 'spark-job'
PROJECT_ID = '..............'
REGION = 'us-central1'
pyspark_job_file_path = 'gs://snowflake-projects_test/spark_job/spark_job.py'

# Task: Submit a PySpark job to Dataproc
submit_pyspark_job = DataprocSubmitPySparkJobOperator(
    task_id='submit_pyspark_job',
    main=pyspark_job_file_path,
    arguments=['--date={{ ti.xcom_pull(task_ids=\'get_execution_date\') }}'],  # Pass execution date to PySpark job
    cluster_name=CLUSTER_NAME,
    region=REGION,
    project_id=PROJECT_ID,
    dag=dag,
    dataproc_jars=[
        'gs://snowflake-projects_test/snowflake_jars/spark-snowflake_2.12-2.15.0-spark_3.4.jar',
        'gs://snowflake-projects_test/snowflake_jars/snowflake-jdbc-3.16.0.jar'
    ],
)

# Setting task dependencies
get_execution_date_task >> merge_customer_dim >> insert_customer_dim >> submit_pyspark_job