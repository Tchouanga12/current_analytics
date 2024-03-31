import pandas as pd
from sqlalchemy import create_engine


# load csv file
def load_data():
    data_file = '/home/franck/current_analytics/donnee_de_consommation (9).csv'
    data = pd.read_csv(data_file)
    return data


# function to clean the data
def clean_data(data):
    data['date'] = pd.to_datetime(data['date'])

    # get the day from the datetime (with name 'day')
    data["day"] = data['date'].dt.day.astype(str)

    # get the day name from the datetime (with name 'day_name)
    data["day_name"] = data['date'].dt.day_name()

    # get the month name from teh datetime (with name 'month_name)
    data["month_name"] = data['date'].dt.month_name()

    # drop the index column (unwanted column)
    data.drop(columns=['index type'], inplace=True)

    # align the columns in the wanted order
    data = data[['date', 'day', 'day_name', 'month_name', 'consumption(kWh)']]

    return data


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


def save_data_to_parquet(data):
    data.to_parquet('/home/franck/current_analytics/datalake/current.parquet')
