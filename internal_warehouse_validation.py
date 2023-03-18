from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from datetime import datetime,timedelta

# create the engine
postgres_conn_string = 'postgresql+psycopg2://postgres:bhise@localhost:5432/weatherdb'
engine = create_engine(postgres_conn_string)

def check_missing_values(df):
    '''
    This function takes a pandas dataframe as input and returns
    a dictionary of column names and the number of missing values
    in each column.
    '''
    missing_values = df.isnull().sum().to_dict()
    return missing_values

def check_duplicates(df):
    '''
    This function takes a pandas dataframe as input and returns
    a boolean value indicating whether there are any duplicate rows
    in the dataframe.
    '''
    duplicates = df.duplicated().any()
    return duplicates

def check_data_types(df, Schema):
    '''
    This function takes a pandas dataframe and a dictionary of
    column names and expected data types as input, and returns
    a dictionary of column names and the number of rows in which
    the data type does not match the expected type.
    '''
    return str(df.dtypes.to_dict())==Schema


table_names = ['location_table','temp_data','precipitation_data','wind_data','wide_data','long_data']

Schemas = ["{'id': dtype('int64'), 'location': dtype('O'), 'latitude': dtype('float64'), 'longitude': dtype('float64')}", "{'id': dtype('int64'), 'location': dtype('O'), 'date': dtype('O'), 'hour': dtype('int64'), 'latitude': dtype('float64'), 'longitude': dtype('float64'), 'temperature': dtype('float64')}", "{'id': dtype('int64'), 'location': dtype('O'), 'latitude': dtype('float64'), 'longitude': dtype('float64'), 'preciipitation': dtype('float64'), 'hour': dtype('int64'), 'date': dtype('O')}", "{'id': dtype('int64'), 'location': dtype('O'), 'latitude': dtype('float64'), 'longitude': dtype('float64'), 'wind': dtype('float64'), 'hour': dtype('int64'), 'date': dtype('O')}", "{'id': dtype('int64'), 'location': dtype('O'), 'date': dtype('O'), 'hour': dtype('int64'), 'temperature': dtype('float64'), 'preciipitation': dtype('float64'), 'wind': dtype('float64'), 'sunshine': dtype('O')}", "{'id': dtype('int64'), 'location': dtype('O'), 'date': dtype('O'), 'hour': dtype('int64'), 'Variable': dtype('O'), 'Value': dtype('float64')}"]

idx = 0
for table in table_names:
    query = "Select * from "+table
    print("Table :",table)
    df = pd.read_sql_query(query, engine)
    print("missing_values : ",check_missing_values(df))
    print("Possibility of duplicates : ",check_duplicates(df))
    print("Schema check : ",check_data_types(df,Schemas[idx]))
    idx += 1

#Integrity - wind
# Get the common columns from both tables
wind_df = pd.read_sql_query("Select location, date, hour, wind from wind_data", engine)
wide_df = pd.read_sql_query("Select location, date, hour, wind from wide_data", engine)

# Merge the two dataframes on location, date and hour columns
merged_df = pd.merge(wind_df, wide_df, on=['location', 'date', 'hour'])

# Check for any missing or inconsistent values
print("Wind Missing Values :",merged_df.isnull().values.any())  # True if there are any missing values
print("Wind Consistency :",merged_df['wind_x'].equals(merged_df['wind_y']))  # True if wind values are consistent between wind_df and wide_df

    
#Integrity - temperature
# Get the common columns from both tables
temp_df = pd.read_sql_query("Select location, date, hour, temperature from temp_data", engine)
wide_df = pd.read_sql_query("Select location, date, hour, temperature from wide_data", engine)

# Merge the two dataframes on location, date and hour columns
merged_df = pd.merge(temp_df, wide_df, on=['location', 'date', 'hour'])

# Check for any missing or inconsistent values
print("Temperature Missing Values :",merged_df.isnull().values.any())  # True if there are any missing values
print("Temperature Consistency :",merged_df['temperature_x'].equals(merged_df['temperature_y']))  # True if wind values are consistent between temp_df and wide_df
    
#Integrity - precipitation
# Get the common columns from both tables
prec_df = pd.read_sql_query("Select location, date, hour, preciipitation from precipitation_data", engine)
wide_df = pd.read_sql_query("Select location, date, hour, preciipitation from wide_data", engine)

# Merge the two dataframes on location, date and hour columns
merged_df = pd.merge(prec_df, wide_df, on=['location', 'date', 'hour'])

# Check for any missing or inconsistent values
print("Precipitation Missing Values :",merged_df.isnull().values.any())  # True if there are any missing values
print("Precipitation Consistency :",merged_df['preciipitation_x'].equals(merged_df['preciipitation_y']))  # True if wind values are consistent between prec_df and wide_df
    
