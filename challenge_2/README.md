ETL and MDM Process
Overview
This project consists of two main classes: ELT for Extract, Load, Transform (ELT) operations, and MDM_BUILDER for Master Data Management (MDM) processes. The ELT class is responsible for orchestrating the extraction, transformation, and loading of data from CSV files to Snowflake, while the MDM_BUILDER class handles the creation and transformation of dimension and fact tables for MDM purposes.

ELT Class
Description
The ELT class contains methods for extracting data from CSV files, staging data into Snowflake, performing data cleaning operations, and logging process steps.

Methods
extract_csv_to_df: Extracts data from a CSV file into a DataFrame.
stage_data: Stages DataFrame to Snowflake.
df_dedup: Deduplicates DataFrame based on ID column.
fill_na: Fills DataFrame NaN values with 0.
drop_negatives: Drops DataFrame values below 0.
login_filter: Filters DataFrame values according to login types.
column_rename: Renames DataFrame columns.
event_name: Creates DataFrame column Event Name.
uppercase_df_columns: Capitalizes DataFrame column names.
start_etl_process: Initiates the ELT process by iterating over input files and executing data operations.

Dependencies
pandas
numpy
snowflake-sqlalchemy
MDM_BUILDER Class
Description
The MDM_BUILDER class is responsible for building dimension and fact tables for Master Data Management (MDM) purposes. It interacts with Snowflake to retrieve data, build tables, and write data to CSV files.

Methods
table_dim_builder: Builds dimension table.
table_fact_builder: Builds fact table.
values_dict: Retrieves values dictionary for a given table.
write_csv: Writes DataFrame to CSV file.
table_retriever: Retrieves data from a table.
concat_dfs: Concatenates DataFrames.
concat_dicts: Concatenates dictionaries.
value_replace: Replaces values in DataFrame.
unique_values: Gets unique values from lists.
event_table_process: Processes event table.
login_type_table_process: Processes login type table.
currency_process: Processes currency table.
interface_process: Processes interface table.
tx_status_process: Processes transaction status table.
users_process: Processes users table.
user_activities_process: Processes user activities table.
mdm_process_start: Starts MDM transformation process.

Dependencies
pandas
snowflake-sqlalchemy

Configuration
Ensure that the necessary configuration files (config.yaml and variables.yaml) are set up before running the ETL and MDM processes. Update the configuration files with relevant credentials and parameters.

Usage
Instantiate the ELT class (ELT) with the required parameters (files_name, config_file, env_variables_files, source_path) and call the start_etl_process method to initiate the ETL process.
python

start_elt = ELT(files_name, config_file, env_variables_files, source_path)
start_elt.start_etl_process()

Instantiate the MDM_BUILDER class (MDM_BUILDER) with the required parameters (config_file, env_variables_files) and call the mdm_process_start method to start the MDM transformation process.
python

start_mdm = MDM_BUILDER(config_file, env_variables_files)
start_mdm.mdm_process_start()

Notes
Make sure to customize the configuration files (config.yaml and variables.yaml) according to your environment and requirements.
Ensure that Snowflake credentials are correctly configured and accessible.
Verify that the source CSV files and target Snowflake tables/schema/database names match the configurations.