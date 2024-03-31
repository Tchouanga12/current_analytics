import pandas as pd
from sqlalchemy import create_engine


# load csv file
def load_current_data():
    data_file = '/home/franck/current_analytics/donnee_de_consommation (9).csv'
    data = pd.read_csv(data_file)
    return data


def load_meteo_data():
    pass


# function to clean the data
def clean_current_data(data):
    # columns for timetable
    time_dim_column = ['date', 'day', 'day_name', 'month_name', 'year', 'semester']

    # columns for current table
    current_dim_column = ['date', 'day', 'day_name', 'month_name', 'consumption(kWh)']

    # extracting date from datetime
    data['date'] = pd.to_datetime(data['date'])

    # get the day from the datetime (with name 'day')
    data["day"] = data['date'].dt.day.astype(str)

    # get the day name from the datetime (with name 'day_name)
    data["day_name"] = data['date'].dt.day_name()

    # get the month name from the datetime (with name 'month_name)
    data["month_name"] = data['date'].dt.month_name()

    # get the year from the datetime
    data["year"] = data['date']

    # get the semester from datetime
    data["semester"] = data['date']

    # drop the index column (unwanted column)
    data.drop(columns=['index type'], inplace=True)

    # align the columns in the wanted order for current data
    data_current = data[current_dim_column]

    # align the columns in wanted order for time data
    data_time = data[time_dim_column]

    return data_current, data_time


def connect_mysql(database):
    host = "localhost"
    user = "root"
    password = "franck"
    database = database
    port = '3306'
    engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}')

    return engine


def save_data_to_mysql(data, database):
    db_connector = connect_mysql(database)
    data.to_sql(name='current', con=db_connector, if_exists='replace', index=False)


def save_data_to_parquet(current_data, time_data):
    # advantage of storing data in parquet due to its columnar data management form.

    current_data.to_parquet('/home/franck/current_analytics/datalake/parquet/current/current.parquet',
                            partition_cols=['month_name'])

    time_data.to_parquet('/home/franck/current_analytics/datalake/parquet/current/time.parquet',
                         partition_cols=['semester'])
