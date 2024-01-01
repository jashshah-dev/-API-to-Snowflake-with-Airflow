import logging
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from data_extraction_code import runner  # Import the 'runner' function from data_extraction_code

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define default arguments for the DAG
args = {"owner": "Airflow", "start_date": airflow.utils.dates.days_ago(2)}

# Create a new Airflow DAG
dag = DAG(
    dag_id="snowflake_automation_dag",  # Unique identifier for the DAG
    default_args=args,  # Default arguments for the DAG
    schedule_interval=None  # Set the schedule interval for periodic execution (None for manual execution)
)

# Define the tasks within the DAG
with dag:
    # Task to extract stocks information using the 'runner' function
	#this will load file in local file system, returning file name with complete path
    extract_stocks_info = PythonOperator(
        task_id='extract_stocks_info',  # Unique identifier for the task
        python_callable=runner,  # Specify the Python function to execute
        dag=dag,  # Reference to the DAG
    )

    # Task to move the extracted file to Amazon S3
	#move data between ec2 to s3
    move_file_to_s3 = BashOperator(
        task_id="move_file_to_s3",  # Unique identifier for the task
        bash_command='aws s3 mv {{ ti.xcom_pull("extract_stocks_info")}}  s3://demouserdatascriptairflow',  # Bash command to execute
    )

    # Task to create a Snowflake table
    snowflake_create_table = SnowflakeOperator(
        task_id="snowflake_create_table",  
        sql="""create or replace table helloparquet
using template (
    select array_agg(object_construct(*))
    from table(infer_schema(
        location=>'@stock_data.PUBLIC.snow_simple',
        file_format=>'parquet_format'
    ))
)""",  
        snowflake_conn_id="snowflake_conn",  
    )

    # Task to copy data into the Snowflake table
    snowflake_copy = SnowflakeOperator(
        task_id="snowflake_copy",  
        sql="""copy into stock_data.PUBLIC.helloparquet from @stock_data.PUBLIC.snow_simple MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE FILE_FORMAT=parquet_format""", 
        snowflake_conn_id="snowflake_conn",  
    )

# Define task dependencies
extract_stocks_info >> move_file_to_s3 >> snowflake_create_table >> snowflake_copy

# Multi-line comment explaining the purpose of XCom
"""
- XCom (cross-communication) is a feature in Apache Airflow that allows tasks to exchange data.
- In this DAG, XCom is used to pass data (the output_file path) from the 'extract_stocks_info' task to the 'move_file_to_s3' task.
- The 'ti.xcom_pull' method is used to retrieve the output of a task, in this case, the 'extract_stocks_info' task.
- The result of 'runner' function is stored as XCom data and can be accessed in other tasks.
- XCom enables communication and data sharing between tasks in a DAG.
"""
