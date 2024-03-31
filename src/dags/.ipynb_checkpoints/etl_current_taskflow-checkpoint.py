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
    data = rc.load_data()
    data = rc.clean_data(data)
    rc.save_data_to_mysql(data, 'onde6')


CURRENT_RAW()
