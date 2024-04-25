import os
import yaml
#import etl
import pandas as pd
import numpy as np
from snowflake.snowpark.session import Session

#function definition

def snowflake_connection(connection_params):
    session = Session.builder.configs(connection_params).create()
    return session

def extract_csv_to_df(input_file):
    df = pd.read_csv(input_file)
    return df

def 


with open(config_file, "r") as config:
    credentials = yaml.safe_load(config)

connection_params = {
    "account":credentials["snowflake"]["account"],
    "user": credentials["snowflake"]["user"],
    "password":credentials["snowflake"]["password"],
    "role":credentials["snowflake"]["role"],
    "warehouse":credentials["snowflake"]["warehouse"],
    "database":credentials["snowflake"]["database"]
    "schema": credentials["snowflake"]["schema"]
}

source_path = '~/bitso_tech_challenge/challenge_2/source_files/'
files = os.listdir(os.path.expanduser(source_path))

for file in files:
