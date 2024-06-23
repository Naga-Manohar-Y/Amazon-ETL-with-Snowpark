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

### Video Tutorial

For a complete hands-on guide, watch the [video tutorial](https://youtu.be/1jC98XQwBZw) available on my YouTube channel.

## Step-by-Step Guide

### Step 1: Create User & Virtual Warehouse

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
