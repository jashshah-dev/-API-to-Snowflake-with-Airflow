-- Drop the database if it already exists
DROP DATABASE IF EXISTS stock_data;

-- Create a new database named 'stock_data' if it doesn't exist
CREATE DATABASE IF NOT EXISTS stock_data;

-- Switch to the 'stock_data' database
USE DATABASE stock_data;

-- Create a file format named 'parquet_format' with the type 'parquet'
CREATE OR REPLACE FILE FORMAT parquet_format TYPE = parquet;

-- Create or replace a stage named 'stock_data.PUBLIC.snow_simple' pointing to the specified S3 location
CREATE OR REPLACE STAGE stock_data.PUBLIC.snow_simple
URL = "s3://demouserdatascriptairflow"
CREDENTIALS = (AWS_KEY_ID = '{}'
               AWS_SECRET_KEY = '{}');

-- List the data objects (files) present in the 'ramu.PUBLIC.snow_simple' stage
LIST @stock_data.PUBLIC.snow_simple;
