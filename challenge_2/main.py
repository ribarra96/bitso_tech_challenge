import os
import yaml
import ast
#import etl
import pandas as pd
import numpy as np
from snowflake.snowpark.session import Session

#function definition

def snowflake_connection(connection_params):
    session = Session.builder.configs(connection_params).create()
    return session

def extract_csv_to_df(input_file, date_column_name):
    if "user" in input_file:
        df = pd.read_csv(input_file)
    else:
        df = pd.read_csv(input_file, parse_dates=[date_column_name])
    return df

def stage_data(session, df, staging_table_name, staging_database_name, staging_schema_name):
    df = session.write_pandas(df, staging_table_name, database = staging_database_name, schema = staging_schema_name, auto_create_table = True, overwrite = True, use_logical_type = True)

def df_dedup(session, df, id_column_name, table_name, staging_database_name, cleaning_schema_name):
    df = df.drop_duplicates(subset=[id_column_name])
    stage = session.write_pandas(df, f"C_{table_name}_DEDUP", database = staging_database_name, schema = cleaning_schema_name, auto_create_table = True, overwrite = True, use_logical_type = True)
    return df

def fill_na(session, df, column_name,  table_name, staging_database_name, cleaning_schema_name):
    df[column_name] = df[column_name].fillna(0)
    stage = session.write_pandas(df, f"C_{table_name}_FILLNA", database = staging_database_name, schema = cleaning_schema_name, auto_create_table = True, overwrite = True, use_logical_type = True)
    return df

def drop_negatives(session, df, column_name,  table_name, staging_database_name, cleaning_schema_name):
    df = df.where(df[column_name] >= 0).dropna()
    stage = session.write_pandas(df, f"C_{table_name}_FILTER_AMOUNT", database = staging_database_name, schema = cleaning_schema_name, auto_create_table = True, overwrite = True, use_logical_type = True)
    return df

def login_filter(session, df, filter_column_name,login_types, table_name, staging_database_name, cleaning_schema_name):
    df = df[df.event_name.isin(login_types)]
    stage = session.write_pandas(df, f"C_{table_name}_PURGE_LOGIN_TYPES", database = staging_database_name, schema = cleaning_schema_name, auto_create_table = True, overwrite = True, use_logical_type = True)
    return df

def column_rename(session, df, column_dict, table_name, staging_database_name, cleaning_schema_name):
    df = df.rename(columns=column_dict)
    stage = session.write_pandas(df,f"C_{table_name}_COLUMN_RENAME", database = staging_database_name, schema = cleaning_schema_name, auto_create_table = True, overwrite = True, use_logical_type = True)
    return df

def event_name(session, df, event_name, table_name, staging_database_name, cleaning_schema_name):
    df['event_name'] = event_name
    stage = session.write_pandas(df,f"C_{table_name}_EVENT_NAME", database = staging_database_name, schema = cleaning_schema_name, auto_create_table = True, overwrite = True, use_logical_type = True)

def uniques_to_list(df, unique_column_name):
    df = df
    concat_list = []
    for item in unique_column_name:
        string = f"df.{item}.unique().tolist()"
        u_list = string
        concat_list.append(u_list)
    return concat_list

def uppercase_df_columns(df):
    df = df.columns = [x.upper for x in df.columns]
    return df

def integrate_lists(*lists):
    concatenated_list = []
    for l in lists:
        concatenated_list + l
    u_list = list(set(concatenated_list))
    return concatenated_list

#mdm builder
def table_dim_builder(u_list, table_name):
    pass

config_file = 'config.yaml'
env_variables = 'variables.yaml'

with open(config_file, "r") as config:
    credentials = yaml.safe_load(config)

connection_params = {
    "account":credentials["snowflake"]["account"],
    "user": credentials["snowflake"]["user"],
    "password":credentials["snowflake"]["password"],
    "role":credentials["snowflake"]["role"],
    "warehouse":credentials["snowflake"]["warehouse"],
    "database":credentials["snowflake"]["database"],
    "schema": credentials["snowflake"]["schema"]
}

session = snowflake_connection(connection_params)

source_path = '~/bitso_tech_challenge/challenge_2/source_files/'
files = os.listdir(os.path.expanduser(source_path))

with open(env_variables, "r") as config:
    variables = yaml.safe_load(config)

date_column_name = variables["variables"]["date_column_name"]
staging_database_name = variables["variables"]["staging_database_name"]
staging_schema_name = variables["variables"]["staging_schema_name"]
cleaning_schema_name = variables["variables"]["cleaning_schema_name"]
config_database = variables["variables"]["config_database"]
config_schema = variables["variables"]["config_schema"]
elt_md_table = variables["variables"]["elt_metadata"]
login_types = variables["variables"]["login_types"]

for file in files:
    print("ELT process start, file name: " + file)
    try:
        table_metadata = session.sql(f"SELECT * FROM {config_database}.{config_schema}.{elt_md_table} WHERE FILE_NAME = '{file.strip()}'").to_pandas()
        id_column_name = table_metadata['ID_COLUMN_NAME'][0]
        staging_table_name = table_metadata['STAGING_TABLE_NAME'][0]
        table_name = table_metadata['TABLE_NAME'][0]
        conditions = table_metadata['CONDITIONS'][0]
        filter_columns = table_metadata['FILTER_COLUMNS'][0]
        column_dict_str = table_metadata['RENAME_COLUMNS'][0]
        event_name_value = table_metadata['EVENT_NAME'][0]
        column_unique_string = table_metadata['UNIQUE_COLUMNS_LIST'][0]
        column_unique_list = ast.literal_eval(column_unique_string)
        column_dict = ast.literal_eval(column_dict_str)
        print("Table_name: " + table_name)
    except Exception as e: 
        print(e)
    df = extract_csv_to_df(source_path + file, date_column_name)
    staging_data = stage_data(session, df, staging_table_name, staging_database_name, staging_schema_name)
    print("Staging data done")
    if "dedup" in conditions:
        df = df_dedup(session, df, id_column_name, table_name, staging_database_name, cleaning_schema_name)
        print("Data duplicates removed")
    if table_name == 'EVENT':
        if "login_filter" in conditions:
            df = login_filter(session, df, filter_columns, login_types, table_name, staging_database_name, cleaning_schema_name)
            print("Login records filtered")
        if "column_rename" in conditions:
            df = column_rename(session, df,column_dict,table_name, staging_database_name, cleaning_schema_name)
            print("DataFrame columns renamed")
        if "event_name" in conditions:
            df = event_name(session, df, event_name_value, table_name, staging_database_name, cleaning_schema_name)
            print("Event name column created")
        table_uniques = uniques_to_list(df, column_unique_list)
        event_name_unique = table_uniques[0]
        login_type_unique = table_uniques[1]
        print("Lists of uniques created")
    elif table_name == 'DEPOSIT':
        if "fill_na" in conditions:
            df = fill_na(session, df, filter_columns, table_name, staging_database_name, cleaning_schema_name)
            print("Null values replaced")
        if "drop_negatives" in conditions:
            df = drop_negatives(session, df, filter_columns, table_name, staging_database_name, cleaning_schema_name)
            print("Negative amounts removed")
        if "event_name" in conditions:
            df = event_name(session, df, event_name_value, table_name, staging_database_name, cleaning_schema_name)
            print("Event name column created")
        table_uniques = uniques_to_list(df, column_unique_list)
        currency_uniques = table_uniques[0]
        tx_status_uniques = table_uniques[1]
        event_name_uniques = table_uniques[2]
        print("Lists of uniques created")       
    elif table_name == 'WITHDRAWAL':
        if "fill_na" in conditions:
            df = fill_na(session, df, filter_columns, table_name, staging_database_name, cleaning_schema_name)
            print("Null values replaced")
        if "drop_negatives" in conditions:
            df = drop_negatives(session, df, filter_columns, table_name, staging_database_name, cleaning_schema_name)
            print("Negative amounts removed")
        if "event_name" in conditions:
            df = event_name(session, df, event_name_value, table_name, staging_database_name, cleaning_schema_name)
            print("Event name column created")
        table_uniques = uniques_to_list(df, column_unique_list)
        currency_uniques = table_uniques[0]
        tx_status_uniques = table_uniques[1]
        event_name_uniques = table_uniques[2]
        print("Lists of uniques created")       
    else:
        pass
    

    

