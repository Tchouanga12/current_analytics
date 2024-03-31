import pendulum
from airflow.decorators import dag
import raw_consommation as rc


@dag(
    schedule="35 8 * * *",
    start_date=pendulum.datetime(2023, 8, 1, tz="UTC"),
    catchup=False,
    tags=["current_raw"]
)
def CURRENT_RAW():
    data = rc.load_current_data()
    current_data, time_data = rc.clean_current_data(data)
    rc.save_data_to_mysql(current_data, 'onde6')
    rc.save_data_to_mysql(time_data, 'onde6')
    rc.save_data_to_parquet(current_data,time_data)


CURRENT_RAW()
