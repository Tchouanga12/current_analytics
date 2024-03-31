import pandas as pd
import pytest
from sqlalchemy import create_engine, inspect
import raw_consommation


def test_clean_data():
    data = raw_consommation.load_current_data()
    data = raw_consommation.clean_current_data(data)

    assert data.shape[1] == 5
    assert 'date' in data.columns
    assert 'day' in data.columns
    assert 'day_name' in data.columns
    assert 'month_name' in data.columns
    assert 'consumption(kWh)' in data.columns

    expected_types = {
        'date': 'datetime64[ns]',
        'day': 'object',
        'day_name': 'object',
        'month_name': 'object',
        'consumption(kWh)': 'float64'
    }

    for column, expected_type in expected_types.items():
        assert data[
                   column].dtype == expected_type, f"Column '{column}' has unexpected data type. Expected: {expected_type}, Actual: {data[column].dtype}"


def test_connect_mysql():
    connection = raw_consommation.connect_mysql('onde6')
    assert connection is not None, 'Failed to create MySQL connection object'


def test_save_data():
    data = raw_consommation.load_current_data()
    data = raw_consommation.clean_current_data(data)
    raw_consommation.save_data_to_mysql(data, 'onde6')
    mysql_db_engine = raw_consommation.connect_mysql('onde6')
    inspector = inspect(mysql_db_engine)
    tables = inspector.get_table_names()

    assert 'current' in tables
