# Amazon Mobile Sales ETL with Snowpark and Snowflake

This project demonstrates an end-to-end ETL (Extract, Transform, Load) data flow using the powerful combination of Snowpark and Snowflake. The focus is on efficiently handling Amazon’s mobile sales order data from three regions (India/USA/France), starting from loading the data from a local machine to a Snowflake internal stage. The data is then transformed and cleansed using the Snowpark DataFrame API, ensuring quality and accuracy. Finally, the concept of dimensional modeling is explored to organize data into meaningful structures for analytical querying and reporting.

## Key Steps Involved

1. **Extract Data:** Load Amazon’s mobile sales order data from a local machine to a Snowflake internal stage.
2. **Transform Data:** Use the Snowpark DataFrame API to transform and cleanse the data.
3. **Load Data:** Organize data into meaningful structures using dimensional modeling.

## Data Source

The Amazon mobile sales data for three regions (NA/EU/APAC) is available [here](#).

## Architecture Overview

### End-to-End Data Flow Diagram

![Data Flow Diagram](link_to_diagram_image)

- **India Sales Order Data:** CSV Format
- **USA Sales Order Data:** Parquet File Format
- **France Sales Order Data:** JSON File Format

## Step-by-Step Guide

### Create User & Virtual Warehouse

Create a virtual warehouse and user account for running the Snowpark ETL workload.

```sql
-- Create a virtual warehouse
use role sysadmin;
create warehouse snowpark_etl_wh 
    with 
    warehouse_size = 'medium' 
    warehouse_type = 'standard' 
    auto_suspend = 60 
    auto_resume = true 
    min_cluster_count = 1
    max_cluster_count = 1 
    scaling_policy = 'standard';

-- Create a Snowpark user
use role accountadmin;
create user snowpark_user 
  password = 'Test@12$4' 
  comment = 'This is a Snowpark user' 
  default_role = sysadmin
  default_secondary_roles = ('ALL')
  must_change_password = false;

-- Grant permissions
grant role sysadmin to user snowpark_user;
grant USAGE on warehouse snowpark_etl_wh to role sysadmin;
```
### Validate Snowpark Snowflake Connectivity

Validate the connection using the Snowpark session.
```sql
from snowflake.snowpark import Session
import sys
import logging

# Initiate logging at info level
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%I:%M:%S')

# Snowpark session
def get_snowpark_session() -> Session:
    connection_parameters = {
       "ACCOUNT":"<sf-account>",
        "USER":"snowpark_user",
        "PASSWORD":"Test@12$4",
        "ROLE":"SYSADMIN",
        "DATABASE":"SNOWFLAKE_SAMPLE_DATA",
        "SCHEMA":"TPCH_SF1",
        "WAREHOUSE":"SNOWPARK_ETL_WH"
    }
    # Create Snowflake session object
    return Session.builder.configs(connection_parameters).create()   

def main():
    session = get_snowpark_session()

    context_df = session.sql("select current_role(), current_database(), current_schema(), current_warehouse()")
    context_df.show(2)

    customer_df = session.sql("select c_custkey, c_name, c_phone, c_mktsegment from snowflake_sample_data.tpch_sf1.customer limit 10")
    customer_df.show(5)

if __name__ == '__main__':
    main()
```
### Create Database & Schema Objects
Create sales_dwh database and schemas.
```sql
-- Create database
create database if not exists sales_dwh;

use database sales_dwh;

-- Create schemas
create schema if not exists source;
create schema if not exists curated;
create schema if not exists consumption;
create schema if not exists audit;
create schema if not exists common;
```
### Load Data to Internal Stage
Create Internal Stage
```sql
-- Create internal stage within source schema
use schema source;
create or replace stage my_internal_stg;

-- Example PUT commands for different formats
-- CSV
put file:///tmp/sales/source=IN/format=csv/date=2022-02-22/order-20220222.csv @sales_dwh.source.my_internal_stg/sales/source=IN/format=csv/date=2022-02-22 auto_compress=False overwrite=True, parallel=3;

-- JSON
put file:///tmp/sales/source=FR/format=json/date=2022-02-22/order-20220222.json @sales_dwh.source.my_internal_stg/sales/source=FR/format=json/date=2022-02-22 auto_compress=False overwrite=True, parallel=3;

-- Parquet
put file:///tmp/sales/source=US/format=parquet/date=2022-02-22/order-20220222.snappy.parquet @sales_dwh.source.my_internal_stg/sales/source=US/format=parquet/date=2022-02-22 auto_compress=False overwrite=True, parallel=3;
```

Load Data Using Snowpark File API

```sql
import os
from snowflake.snowpark import Session
import sys
import logging

# Initiate logging at info level
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%I:%M:%S')

# Snowpark session
def get_snowpark_session() -> Session:
    connection_parameters = {
       "ACCOUNT":"<your-snowflake-account>",
        "USER":"<your-user>",
        "PASSWORD":"<your-pwd>",
        "ROLE":"SYSADMIN",
        "DATABASE":"sales_dwh",
        "SCHEMA":"source"
    }
    # Create Snowflake session object
    return Session.builder.configs(connection_parameters).create()   

def traverse_directory(directory, file_extension) -> list:
    local_file_path = []
    file_name = []
    partition_dir = []
    print(directory)
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(file_extension):
                file_path = os.path.join(root, file)
                file_name.append(file)
                partition_dir.append(root.replace(directory, ""))
                local_file_path.append(file_path)
    return file_name, partition_dir, local_file_path

def main():
    directory_path = '/tmp/snowpark-e2e/'
    csv_file_name, csv_partition_dir, csv_local_file_path = traverse_directory(directory_path, '.csv')
    parquet_file_name, parquet_partition_dir, parquet_local_file_path = traverse_directory(directory_path, '.parquet')
    json_file_name, json_partition_dir, json_local_file_path = traverse_directory(directory_path, '.json')
    stage_location = '@sales_dwh.source.my_internal_stg'

    for idx, file_element in enumerate(csv_file_name):
        get_snowpark_session().file.put(csv_local_file_path[idx], stage_location + "/" + csv_partition_dir[idx], auto_compress=False, overwrite=True, parallel=10)

    for idx, file_element in enumerate(parquet_file_name):
        get_snowpark_session().file.put(parquet_local_file_path[idx], stage_location + "/" + parquet_partition_dir[idx], auto_compress=False, overwrite=True, parallel=10)

    for idx, file_element in enumerate(json_file_name):
        get_snowpark_session().file.put(json_local_file_path[idx], stage_location + "/" + json_partition_dir[idx], auto_compress=False, overwrite=True, parallel=10)
    
if __name__ == '__main__':
    main()

```
### File Format Objects Within Common Schema
Following file formats will be created under the common schema and will be used to read and process the data from the internal stage location.
```sql
use schema common;

-- Create file formats for csv (India), json (France), Parquet (USA)
create or replace file format my_csv_format
  type = csv
  field_delimiter = ','
  skip_header = 1
  null_if = ('null', 'null')
  empty_field_as_null = true
  field_optionally_enclosed_by = '\042'
  compression = auto;

-- JSON file format with strip outer array true
create or replace file format my_json_format
  type = json
  strip_outer_array = true
  compression = auto;

-- Parquet file format
create or replace file format my_parquet_format
  type = parquet
  compression = snappy;
```
