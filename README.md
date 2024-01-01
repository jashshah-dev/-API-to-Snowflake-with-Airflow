# API-to-Snowflake-with-Airflow
 Leverage Apache Airflow to effortlessly extract data from Alpha Vantage API and load it into Snowflake, streamlining the process of integrating financial market data into your Snowflake data warehouse.

 # Snowflake Automation DAG with Data Extraction

## Project Overview

This Apache Airflow DAG automates tasks related to Snowflake and data extraction from the Alpha Vantage API. The primary functionalities include extracting stock data, moving it to Amazon S3, and interacting with Snowflake for data storage.

## DAG Structure

The DAG, named `snowflake_automation_dag`, consists of the following tasks:

1. **extract_stocks_info:**
   - Executes the `runner` function from the `data_extraction_code` module to fetch daily stock data using the Alpha Vantage API.
   - Generates a unique filename and writes the data to a Parquet file.
   - Utilizes the PythonOperator to run custom Python code.

2. **move_file_to_s3:**
   - Uses the BashOperator to move the extracted Parquet file to an Amazon S3 bucket named `demouserdatascriptairflow`.
   - Accesses the output file path from the previous task using XCom.

3. **snowflake_create_table:**
   - Creates a Snowflake table named `helloparquet` using a predefined SQL query.
   - Utilizes the SnowflakeOperator from the `contrib.operators.snowflake_operator` module.

4. **snowflake_copy:**
   - Copies data from an external stage (`@stock_data.PUBLIC.snow_simple`) to the Snowflake table (`stock_data.PUBLIC.helloparquet`).
   - Uses the SnowflakeOperator.

## XCom Usage
   - XCom (cross-communication) is employed to pass data (output file path) from the 'extract_stocks_info' task to 'move_file_to_s3.'
   - The 'ti.xcom_pull' method retrieves the output of the 'extract_stocks_info' task.
   - XCom enables seamless communication between tasks.

## Data Extraction Code (`data_extraction_code` module)
   - The `runner` function fetches daily stock data for a specified stock symbol using the Alpha Vantage API.
   - Creates a DataFrame from the extracted data and writes it to a Parquet file.
   - Returns the path to the Parquet file.

## Configuration
   - Replace the Alpha Vantage API key in the `runner` function with your own key.
   - Ensure correct configurations for the Snowflake connection in Airflow.

## Execution
   - The DAG is configured for manual triggering (`schedule_interval=None`).
   - Tasks are dependent on the successful completion of the preceding tasks.

## Logging
   - Logging is configured at the INFO level, providing detailed information on DAG execution.

## License
   - This project is provided under the MIT License - see the [LICENSE.md](LICENSE.md) file for details.

