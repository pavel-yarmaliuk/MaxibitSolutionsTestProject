import os
from datetime import datetime

import pandas as pd
from airflow.sdk import dag, task, task_group
from airflow.providers.postgres.hooks.postgres import PostgresHook

from logger import LOGGER
from telegram_notification import telegram_notification

CURRENT_PATH = os.environ.get('RESULTS_PATH', '.')
DT = datetime.now()
EXTRACT_CONNECTIONS = ['project_a_connection', 'project_b_connenciton', 'project_c_connection']
ANALYTICS_CONNECTION = 'analytics_connection'


@dag('load_data_to_analytics_database',
     default_args={
         'on_failure_callback': telegram_notification.send_telegram_notification
     },
     description='Collecting data from sources and loading to analytics database',
     schedule='*/10 * * * *',
     start_date=datetime(2025, 5, 12),
     catchup=False, )
def load_data_to_analytics_database():
    @task_group()
    def extract_data():
        """Extracting data from source materialized views and store them as csv"""

        @task
        def extract_from_source(connection_id: str):
            LOGGER.info(f'Connecting to {connection_id}')
            pg_hook = PostgresHook.get_hook(connection_id)
            LOGGER.info('Getting cursor')
            cursor = pg_hook.get_cursor()
            LOGGER.info('Getting connection object')
            connection = pg_hook.conn
            LOGGER.info('Refreshing materialized views')
            cursor.execute('REFRESH MATERIALIZED VIEW last_10_minutes_events;')
            cursor.execute('REFRESH MATERIALIZED VIEW last_10_minutes_user_sessions;')
            connection.commit()
            LOGGER.info('Fetching materialized views')
            last_10_minutes_event = pg_hook.get_pandas_df('SELECT * FROM last_10_minutes_events;')
            last_10_minutes_session = pg_hook.get_pandas_df('SELECT * FROM last_10_minutes_user_sessions;')
            LOGGER.info('Materialized views fetched, adding project column to them')
            last_10_minutes_event['project'] = connection_id[:10]
            last_10_minutes_session['project'] = connection_id[:10]
            LOGGER.info(f'Writing csv files to {CURRENT_PATH}csv')
            last_10_minutes_session.to_csv(
                f'{CURRENT_PATH}csv/{connection_id}_{DT.strftime('%Y_%m_%d')}_sessions.csv',
                index=False
            )
            last_10_minutes_event.to_csv(
                f'{CURRENT_PATH}csv/{connection_id}_{DT.strftime('%Y_%m_%d')}_events.csv',
                index=False
            )
            connection.close()

        for conn in EXTRACT_CONNECTIONS:
            extract_from_source(conn)

    @task_group()
    def load_data():
        """Load data from stored csv to user_sessions and events tables"""

        @task
        def load_to_database(source_connection, connection_id):
            LOGGER.info(f'Connecting to {connection_id}')
            pg_hook = PostgresHook.get_hook(connection_id)
            LOGGER.info('Getting engine')
            engine = pg_hook.get_sqlalchemy_engine()
            LOGGER.info('Getting cursor')
            cursor = pg_hook.get_cursor()
            LOGGER.info('Getting connection object')
            connection = pg_hook.conn

            LOGGER.info(f'Reading csv data from {CURRENT_PATH}csv')
            sessions = pd.read_csv(
                f'{CURRENT_PATH}csv/{source_connection}_{DT.strftime('%Y_%m_%d')}_sessions.csv'
            )
            events = pd.read_csv(
                f'{CURRENT_PATH}csv/{source_connection}_{DT.strftime('%Y_%m_%d')}_events.csv'
            )

            LOGGER.info('Writing data to raw tables USER_SESSIONS_RAW, EVENTS_RAW')
            sessions.to_sql('USER_SESSIONS_RAW', engine, if_exists='replace')
            events.to_sql('EVENTS_RAW', engine, if_exists='replace')

            LOGGER.info('Writing raw data to clean tables user_sessions, events')
            cursor.execute('''
                INSERT INTO public.user_sessions
                (project, user_id, id, active, page_name, last_activity_at, created_at, updated_at)
                SELECT src.project, src.user_id::int8,
                src.id::int8, src.active::boolean,
                src.page_name, src.last_activity_at::timestamp,
                src.created_at::timestamp, src.updated_at::timestamp
                FROM public."USER_SESSIONS_RAW" AS src
                ON CONFLICT (project, user_id, id)
                DO UPDATE
                SET active=EXCLUDED.active, page_name=EXCLUDED.page_name,
                last_activity_at=EXCLUDED.last_activity_at,
                created_at=EXCLUDED.created_at, updated_at=EXCLUDED.updated_at;
            ''')
            cursor.execute('''
                        INSERT INTO public.events
                        (user_id, id, project, event_name, page_id, created_at)
                        SELECT src.user_id::int8,
                        src.id::int8, src.project,
                        src.event_name, src.page_id::int8,
                        src.created_at::timestamp
                        FROM public."EVENTS_RAW" AS src
                        ON CONFLICT (user_id, id, project)
                        DO UPDATE
                        SET event_name=EXCLUDED.event_name, page_id=EXCLUDED.page_id,
                        created_at=EXCLUDED.created_at;
            ''')
            LOGGER.info('Refreshing materialized views')
            cursor.execute('REFRESH MATERIALIZED VIEW first_successful_transaction;')
            cursor.execute('REFRESH MATERIALIZED VIEW first_successful_transaction_in_usd;')
            cursor.execute('REFRESH MATERIALIZED VIEW sum_of_transactions_in_session;')
            cursor.execute('REFRESH MATERIALIZED VIEW session_analytics;')
            connection.commit()
            connection.close()

        for source_connection in EXTRACT_CONNECTIONS:
            load_to_database(source_connection, ANALYTICS_CONNECTION)

    load_data() << extract_data()


load_data_to_analytics_database()
