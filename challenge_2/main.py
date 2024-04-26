import os
import yaml
import ast
import pandas as pd
import numpy as np
from snowflake.snowpark.session import Session

#function definition
class ELT:

    def __init__(self, files_name, config_file, env_variables_files, source_path):
        self.file_name = files_name
        self.config_file = config_file
        self.env_variables_files = env_variables_files
        self.source_path = source_path

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

        self.session = Session.builder.configs(connection_params).create()

        with open(env_variables_files, "r") as config:
            variables = yaml.safe_load(config)

        self.date_column_name = variables["variables"]["date_column_name"]
        self.staging_database_name = variables["variables"]["staging_database_name"]
        self.staging_schema_name = variables["variables"]["staging_schema_name"]
        self.cleaning_schema_name = variables["variables"]["cleaning_schema_name"]
        self.config_database = variables["variables"]["config_database"]
        self.config_schema = variables["variables"]["config_schema"]
        self.log_database_name = variables["variables"]["log_database_name"]
        self.log_schema_name = variables["variables"]["log_schema_name"]
        self.elt_md_table = variables["variables"]["elt_metadata"]
        self.login_types = variables["variables"]["login_types"]


    def extract_csv_to_df(self, input_file, date_column_name):
        if "user" in input_file:
            df = pd.read_csv(input_file)
        else:
            df = pd.read_csv(input_file, parse_dates=[date_column_name])
        return df

    def stage_data(self, df, staging_table_name):
        df = self.session.write_pandas(df, staging_table_name, database = self.staging_database_name, schema = self.staging_schema_name, auto_create_table = True, overwrite = True, use_logical_type = True)

    def df_dedup(self, df, id_column_name, table_name):
        df = df.drop_duplicates(subset=[id_column_name])
        stage = self.session.write_pandas(df, f"C_{table_name}_DEDUP", database = self.staging_database_name, schema = self.cleaning_schema_name, auto_create_table = True, overwrite = True, use_logical_type = True)
        return df

    def fill_na(self, df, column_name,  table_name):
        df[column_name] = df[column_name].fillna(0)
        stage = self.session.write_pandas(df, f"C_{table_name}_FILLNA", database = self.staging_database_name, schema = self.cleaning_schema_name, auto_create_table = True, overwrite = True, use_logical_type = True)
        return df

    def drop_negatives(self, df, column_name,  table_name):
        df = df.where(df[column_name] >= 0).dropna()
        stage = self.session.write_pandas(df, f"C_{table_name}_FILTER_AMOUNT", database = self.staging_database_name, schema = self.cleaning_schema_name, auto_create_table = True, overwrite = True, use_logical_type = True)
        return df

    def login_filter(self, df, table_name):
        df = df[df.event_name.isin(self.login_types)]
        stage = self.session.write_pandas(df, f"C_{table_name}_PURGE_LOGIN_TYPES", database = self.staging_database_name, schema = self.cleaning_schema_name, auto_create_table = True, overwrite = True, use_logical_type = True)
        return df

    def column_rename(self, df, column_dict, table_name):
        df = df.rename(columns=column_dict)
        stage = self.session.write_pandas(df,f"C_{table_name}_COLUMN_RENAME", database = self.staging_database_name, schema = self.cleaning_schema_name, auto_create_table = True, overwrite = True, use_logical_type = True)
        return df

    def event_name(self, df, event_name, table_name):
        df['event_name'] = event_name
        stage = self.session.write_pandas(df,f"C_{table_name}_EVENT_NAME", database = self.staging_database_name, schema = self.cleaning_schema_name, auto_create_table = True, overwrite = True, use_logical_type = True)
        return df

    def uppercase_df_columns(self, df, table_name):
        df.columns = [x.upper() for x in df.columns]
        stage = self.session.write_pandas(df, f"C_{table_name}_FINAL", database = self.staging_database_name, schema = self.cleaning_schema_name, auto_create_table = True, overwrite = True, use_logical_type = True)
        return df
    
    def log_writter(self, step, status):
        log = self.session.sql(f"INSERT INTO ")
    
    def start_etl_process(self):
        for file in self.file_name:
            print("ELT process start, file name: " + file)
            try:
                table_metadata = self.session.sql(f"SELECT * FROM {self.config_database}.{self.config_schema}.{self.elt_md_table} WHERE FILE_NAME = '{file.strip()}'").to_pandas()
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
            df = self.extract_csv_to_df(self.source_path + file, self.date_column_name)
            if df.empty:
                print("DataFrame does not contain data")
                pass
            else: 
                staging_data = self.stage_data(df, staging_table_name)
                print("Staging data done")

                if "dedup" in conditions:
                    df = self.df_dedup(df, id_column_name, table_name)
                    print("Data duplicates removed")
                if table_name == 'EVENT':
                    if "login_filter" in conditions:
                        df = self.login_filter(df, table_name)
                        print("Login records filtered")
                    if "column_rename" in conditions:
                        df = self.column_rename(df,column_dict,table_name)
                        print("DataFrame columns renamed")
                    if "event_name" in conditions:
                        df = self.event_name(df, event_name_value, table_name)
                        print("Event name column created")
                elif table_name == 'DEPOSIT' or table_name == 'WITHDRAWAL':
                    if "fill_na" in conditions:
                        df = self.fill_na(df, filter_columns, table_name)
                        print("Null values replaced")
                    if "drop_negatives" in conditions:
                        df = self.drop_negatives(df, filter_columns, table_name)
                        print("Negative amounts removed")
                    if "event_name" in conditions:
                        df = self.event_name(df, event_name_value, table_name)
                        print("Event name column created")      
                else:
                    pass
                df = self.uppercase_df_columns(df, table_name)
                print("Uppercased columns success")
            

#mdm builder
class MDM_BUILDER:
    def __init__(self, config_file, env_variables_files):
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

        self.session = Session.builder.configs(connection_params).create()

        with open(env_variables_files, "r") as config:
            variables = yaml.safe_load(config)

        self.staging_database_name = variables["variables"]["staging_database_name"]
        self.cleaning_schema_name = variables["variables"]["cleaning_schema_name"]
        self.mdm_database_name = variables["variables"]["mdm_database_name"]
        self.mdm_schema_name = variables["variables"]["mdm_schema_name"]
        self.log_database_name = variables["variables"]["log_database_name"]
        self.log_schema_name = variables["variables"]["log_schema_name"]
        self.source_users_table_name = variables["variables"]["source_users_table_name"]
        self.source_deposit_table_name = variables["variables"]["source_deposit_table_name"]
        self.source_withdrawal_table_name = variables["variables"]["source_withdrawal_table_name"]
        self.source_event_table_name = variables["variables"]["source_event_table_name"]
        self.target_users_table_name = variables["variables"]["target_users_table_name"]
        self.event_name_table_name = variables["variables"]["event_name_table_name"]
        self.login_type_table_name = variables["variables"]["login_type_table_name"]
        self.currency_table_name = variables["variables"]["currency_table_name"]
        self.interface_table_name = variables["variables"]["interface_table_name"]
        self.tx_table_name = variables["variables"]["tx_table_name"]
        self.user_activities_table_name = variables["variables"]["user_activities_table_name"]
        self.concat_df = variables["variables"]["concat_df"]
        self.replace_values_string = variables["variables"]["values_dict"]
        self.convert_timestamp = variables["variables"]["convert_timestamp"]
        self.convert_string = variables["variables"]["convert_string"]
        self.convert_int = variables["variables"]["convert_int"]
        self.drop_columns_string = variables["variables"]["drop_columns"]

    def table_dim_builder(self, df, table_name):
        get_values_query = self.session.sql(f"SELECT * FROM {self.mdm_database_name}.{self.mdm_schema_name}.{table_name}").to_pandas()
        get_column_values = get_values_query.columns.values.tolist()
        if "EVENT" in get_column_values[-1]:
            column = get_column_values[-1]
        else:
            column = get_column_values[-1]
            column = column.replace("_NAME", "")
        get_values_query = get_values_query.rename(columns={get_column_values[-1]: column})
        merged_df = df.merge(get_values_query, on=column, how='outer', indicator=True)
        new_rows_df = merged_df[merged_df['_merge'] == 'left_only'][[column]]
        if new_rows_df.empty:
            pass
        else:
            insert = self.session.write_pandas(new_rows_df, table_name, database = self.mdm_database_name, schema = self.mdm_schema_name, overwrite = False)
        return new_rows_df
    
    def table_fact_builder(self, df, table_name):
        insert_df = self.session.write_pandas(df, table_name, database = self.mdm_database_name, schema=self.mdm_schema_name, overwrite = False, use_logical_type = True)

    def values_dict(self, table_name):
        data = self.session.sql(f"SELECT * FROM {self.mdm_database_name}.{self.mdm_schema_name}.{table_name}").to_pandas()
        data_list = data.values.tolist()
        print(data_list)
        data_dict = {}
        data_values = {}
        for item in data_list:
            data_values.update({item[1]: item[0]}) 
        name = table_name.replace('_DIM', '')
        data_dict[name] = data_values
        print(data_dict)
        return data_dict
    
    def write_csv(self, table_name):
        data = self.session.sql(f"SELECT * FROM {self.mdm_database_name}.{self.mdm_schema_name}.{table_name}").to_pandas()
        output_file = data.to_csv(f'~/bitso_tech_challenge/challenge_2/target_files/{table_name}.csv')
        return ("Successfull file creation: " + table_name)
    
    def table_retriever(self, database_name, schema_name, table_name):
        df = self.session.sql(f"SELECT * FROM {database_name}.{schema_name}.{table_name}").to_pandas()
        return df
    
    @staticmethod
    def concat_dfs(*dfs):
        df_list = []
        for dataframe in dfs:
            df_list.append(dataframe)
        df = pd.concat(df_list, ignore_index=True)
        return df
    
    @staticmethod
    def concat_dicts(*dicts):
        dicts_list = []
        for d in dicts:
            dicts_list.append(d)
        return dicts_list
    
    @staticmethod
    def value_replace(df, values_dict):
        df = df.replace(values_dict)
        return df

    @staticmethod
    def unique_values(*lists):
        u_list = []
        for l in lists:
            u_list = u_list + l
        unique_list = list(set(u_list))
        return unique_list

    def event_table_process(self):
        print("Start Event table creation")
        clean_event_df = self.table_retriever(self.staging_database_name, self.cleaning_schema_name, self.source_event_table_name)
        clean_deposit_df = self.table_retriever(self.staging_database_name, self.cleaning_schema_name, self.source_deposit_table_name)
        clean_withdrawal_df = self.table_retriever(self.staging_database_name, self.cleaning_schema_name, self.source_withdrawal_table_name)
        
        print("Datasets loaded")
        event_event_name = clean_event_df[['EVENT_NAME']].drop_duplicates()
        deposit_event_name = clean_deposit_df[['EVENT_NAME']].drop_duplicates()
        withdrawal_event_name = clean_withdrawal_df[['EVENT_NAME']].drop_duplicates()
        
        event_name_df = pd.concat([event_event_name, deposit_event_name, withdrawal_event_name]).drop_duplicates(keep=False)
        print("Event name uniques generated")

        print(event_name_df)

        if event_name_df.empty:
            pass
        else:
            create_event_table = self.table_dim_builder(event_name_df, self.event_name_table_name)
            event_dim_csv = self.write_csv(self.event_name_table_name)
        return f"{self.login_type_table_name} table successfully created"

    def login_type_table_process(self):
        clean_event_df = self.table_retriever(self.staging_database_name, self.cleaning_schema_name, self.source_event_table_name)
        
        login_type_name = clean_event_df[['LOGIN_TYPE']].drop_duplicates()
        print(login_type_name)
        create_login_type_table = self.table_dim_builder(login_type_name,self.login_type_table_name)
        dim_csv = self.write_csv(self.login_type_table_name)
        return f"{self.login_type_table_name} table successfully created"

    def currency_process(self):
        clean_deposit_df = self.table_retriever(self.staging_database_name, self.cleaning_schema_name, self.source_deposit_table_name)
        clean_withdrawal_df = self.table_retriever(self.staging_database_name, self.cleaning_schema_name, self.source_withdrawal_table_name)
        
        deposit_currency = clean_deposit_df[['CURRENCY']].drop_duplicates()
        withdrawal_currency = clean_withdrawal_df[['CURRENCY']].drop_duplicates()

        currency_df = pd.concat([deposit_currency, withdrawal_currency]).drop_duplicates(keep=False)

        create_event_table = self.table_dim_builder(currency_df, self.currency_table_name)
        dim_csv = self.write_csv(self.currency_table_name)
        return f"{self.login_type_table_name} table successfully created"
    def interface_process(self):
        
        clean_withdrawal_df = self.table_retriever(self.staging_database_name, self.cleaning_schema_name, self.source_withdrawal_table_name)
        
        withdrawal_event_name = clean_withdrawal_df[['INTERFACE']].drop_duplicates()

        create_event_table = self.table_dim_builder(withdrawal_event_name, self.interface_table_name)
        dim_csv = self.write_csv(self.event_name_table_name)
        return f"{self.login_type_table_name} table successfully created"

    def tx_status_process(self):

        clean_deposit_df = self.table_retriever(self.staging_database_name, self.cleaning_schema_name, self.source_deposit_table_name)
        clean_withdrawal_df = self.table_retriever(self.staging_database_name, self.cleaning_schema_name, self.source_withdrawal_table_name)
        
        deposit_event_name = clean_deposit_df[['TX_STATUS']].drop_duplicates()
        withdrawal_event_name = clean_withdrawal_df[['TX_STATUS']].drop_duplicates()

        tx_status_df = pd.concat([deposit_event_name, withdrawal_event_name]).drop_duplicates(keep=False)

        create_tx_table = self.table_dim_builder(tx_status_df, self.tx_table_name)
        dim_csv = self.write_csv(self.event_name_table_name)
        return f"{self.login_type_table_name} table successfully created"

    def users_process(self):
        
        clean_users_df = self.table_retriever(self.staging_database_name, self.cleaning_schema_name, self.source_users_table_name)
        
        create_users_table = self.table_dim_builder(clean_users_df, self.target_users_table_name)
        event_dim_csv = self.write_csv(self.target_users_table_name)
        return f"{self.login_type_table_name} table successfully created"
    
    def user_activities_process(self):

        deposit_df = self.table_retriever(self.staging_database_name, self.cleaning_schema_name, self.source_deposit_table_name)
        withdrawal_df = self.table_retriever(self.staging_database_name, self.cleaning_schema_name, self.source_withdrawal_table_name)
        event_df = self.table_retriever(self.staging_database_name, self.cleaning_schema_name, self.source_event_table_name)

        concat_df = self.concat_dfs(deposit_df, withdrawal_df, event_df)
        
        interface_dict = self.values_dict(self.interface_table_name)
        event_name_dict = self.values_dict(self.event_name_table_name)
        currency_dict = self.values_dict(self.currency_table_name)
        tx_status_dict = self.values_dict(self.tx_table_name)
        login_type_dict = self.values_dict(self.login_type_table_name)

        concat_df = concat_df.fillna(0)

        replace_values_list = [interface_dict,event_name_dict, currency_dict, tx_status_dict, login_type_dict]
        
        replace_dict = {}
        for d in replace_values_list:
            replace_dict.update(d)
            
        concat_df = self.value_replace(concat_df, replace_dict)
        
        print(concat_df)

        rename_dict = {
            self.event_name_table_name.replace("_DIM", "") : self.event_name_table_name.replace("_DIM","_ID"), 
            self.currency_table_name.replace("_DIM", "") : self.currency_table_name.replace("_DIM","_ID"),
            self.interface_table_name.replace("_DIM", "") : self.interface_table_name.replace("_DIM","_ID"),
            self.tx_table_name.replace("_DIM", "") : self.tx_table_name.replace("_DIM","_ID"),
            self.login_type_table_name.replace("_DIM", "") : self.login_type_table_name.replace("_DIM","_ID"),
            }

        concat_df = concat_df.rename(columns=rename_dict)

        concat_df[self.convert_timestamp] = pd.to_datetime(concat_df[self.convert_timestamp], utc=True)
        concat_df[self.convert_string] = concat_df[self.convert_string].astype(str)
        concat_df[self.convert_int] = concat_df[self.convert_int].astype(int)
        concat_df.replace(0, np.nan, inplace=True)
        concat_df = concat_df.drop(columns=self.drop_columns_string)

        write_fact = self.table_fact_builder(concat_df, self.user_activities_table_name)
        write_csv = self.write_csv(self.user_activities_table_name)

    def mdm_process_start(self):
        self.event_table_process()
        self.login_type_table_process()
        self.currency_process()
        self.interface_process()
        self.tx_status_process()
        self.users_process()
        self.user_activities_process()




config_file = 'config.yaml'
env_variables_files = 'variables.yaml'
source_path = '~/bitso_tech_challenge/challenge_2/source_files/'
files = os.listdir(os.path.expanduser(source_path))

# start_elt = ELT(files, config_file, env_variables_files, source_path)
# start_elt.start_etl_process()

start_mdm = MDM_BUILDER(config_file, env_variables_files)
start_mdm.mdm_process_start()



    

