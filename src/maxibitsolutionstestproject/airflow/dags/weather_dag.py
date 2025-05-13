import os
from datetime import datetime

import pandas as pd
from airflow.sdk import dag, task, task_group

from logger import LOGGER
from telegram_notification import telegram_notification
from api_caller import WeatherAPICaller
from response import ResponseProcessor, Response

WAC = WeatherAPICaller(53.893009, 27.567444)
CURRENT_PATH = os.environ.get('RESULTS_PATH', '.')
DT = datetime.now()


@dag("weather_dag",
     default_args={
         "on_failure_callback": telegram_notification.send_telegram_notification
     },
     description="Collecting data from OpenWeatherAPI and creating parquet",
     schedule="@hourly",
     start_date=datetime(2025, 5, 12),
     catchup=False, )
def weather_dag():
    @task
    def processing_data_from_api_task(ti):
        response: Response = ResponseProcessor.process(WAC.make_api_call())
        temperature_df = response.create_temperature_dataframe()
        wind_df = response.create_wind_dataframe()
        ti.xcom_push(key='temperature_df', value=temperature_df.to_dict())
        ti.xcom_push(key='wind_df', value=wind_df.to_dict())

    @task_group
    def concatenate_data():
        @task
        def concatenate_wind_dataframe(**kwargs):
            wind_dict = kwargs['ti'].xcom_pull(key='wind_df',
                                               task_ids='processing_data_from_api_task')
            LOGGER.info(f'Pulling result...{wind_dict}')
            wind_df = pd.DataFrame.from_dict(wind_dict)
            path_to_file = f"{CURRENT_PATH}/wind_data/minsk_{DT.strftime('%Y_%m_%d')}_wind.parquet"
            if os.path.isfile(path_to_file):
                df = pd.read_parquet(path_to_file)
                LOGGER.info(str(df.to_dict()))
                df_row_merged = pd.concat([df, wind_df], ignore_index=True)
            else:
                df_row_merged = wind_df
            kwargs['ti'].xcom_push(key='concatenated_wind_df', value=df_row_merged.to_dict())

        @task
        def concatenate_temperature_dataframe(**kwargs):
            temperature_dict = kwargs['ti'].xcom_pull(key='temperature_df',
                                                      task_ids='processing_data_from_api_task')
            LOGGER.info(f'Pulling result...{temperature_dict}')
            temperature_df = pd.DataFrame.from_dict(temperature_dict)
            path_to_file = f"{CURRENT_PATH}/temperature_data/minsk_{DT.strftime('%Y_%m_%d')}_temp.parquet"
            if os.path.isfile(path_to_file):
                df = pd.read_parquet(path_to_file)
                df_row_merged = pd.concat([df, temperature_df], ignore_index=True)
            else:
                df_row_merged = temperature_df
            kwargs['ti'].xcom_push(key='concatenated_temperature_df', value=df_row_merged.to_dict())

        concatenate_wind_dataframe()
        concatenate_temperature_dataframe()

    @task_group
    def creating_parquets():
        @task
        def create_wind_parquet(**kwargs) -> None:
            wind_dict = kwargs['ti'].xcom_pull(
                key='concatenated_wind_df',
                task_ids='concatenate_data.concatenate_wind_dataframe')
            LOGGER.info(f'Pulling result...{wind_dict}')
            wind_df = pd.DataFrame.from_dict(wind_dict)
            path = f"{CURRENT_PATH}/wind_data/minsk_{DT.strftime('%Y_%m_%d')}_wind.parquet"
            wind_df.to_parquet(path, index=False)

        @task
        def create_temperature_parquet(**kwargs) -> None:
            temperature_dict = kwargs['ti'].xcom_pull(
                key='concatenated_temperature_df',
                task_ids='concatenate_data.concatenate_temperature_dataframe')
            LOGGER.info(f'Pulling result...{temperature_dict}')
            temperature_df = pd.DataFrame.from_dict(temperature_dict)
            path = f"{CURRENT_PATH}/temperature_data/minsk_{DT.strftime('%Y_%m_%d')}_temp.parquet"
            temperature_df.to_parquet(path, index=False)

        create_wind_parquet()
        create_temperature_parquet()

    creating_parquets() << concatenate_data() << processing_data_from_api_task()


weather_dag()
