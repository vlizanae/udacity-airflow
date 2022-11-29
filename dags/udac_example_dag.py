import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)

from helpers import SqlQueries


class DataQualityQueries:
    songplay_has_rows = ("""
        SELECT COUNT(*)
        FROM songplays
    """)
    
    user_has_rows = ("""
        SELECT COUNT(*)
        FROM users
    """)
    
    song_has_rows = ("""
        SELECT COUNT(*)
        FROM songs
    """)
    
    artist_has_rows = ("""
        SELECT COUNT(*)
        FROM artists
    """)
    
    time_has_rows = ("""
        SELECT COUNT(*)
        FROM time
    """)
    
    songplay_check_null = ("""
        SELECT COUNT(*)
        FROM songplays
        WHERE userid IS NULL OR sessionid IS NULL
    """)
    

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2018, 11, 1, 0, 0),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
}


dag = DAG(
    'udac_example_dag',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *',
    catchup=False
)

start_operator = DummyOperator(
    task_id='Begin_execution', 
    dag=dag
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    redshift_conn_id="redshift",
    aws_conn_id="aws_credentials",
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data/{execution_date.year}/{execution_date.month}/",
    json='auto ignorecase',
    timeformat='epochmillisecs',
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    redshift_conn_id="redshift",
    aws_conn_id="aws_credentials",
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data/",
    dag=dag
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id="redshift",
    table='songplays',
    select_query=SqlQueries.songplay_table_insert,
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id="redshift",
    table='users',
    select_query=SqlQueries.user_table_insert,
    append=False,
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    redshift_conn_id="redshift",
    table='songs',
    select_query=SqlQueries.song_table_insert,
    append=False,
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    redshift_conn_id="redshift",
    table='artists',
    select_query=SqlQueries.artist_table_insert,
    append=False,
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id="redshift",
    table='time',
    select_query=SqlQueries.time_table_insert,
    append=False,
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id="redshift",
    tests_has_rows=[
        DataQualityQueries.songplay_has_rows,
        DataQualityQueries.user_has_rows,
        DataQualityQueries.song_has_rows,
        DataQualityQueries.artist_has_rows,
        DataQualityQueries.time_has_rows,
    ],
    tests_no_rows=[
        DataQualityQueries.songplay_check_null,
    ],
    dag=dag
)

end_operator = DummyOperator(
    task_id='Stop_execution',
    dag=dag
)

# Defining the execution graph
(
    start_operator
    >> [
        stage_events_to_redshift,
        stage_songs_to_redshift,
    ]
    >> load_songplays_table
    >> [
        load_user_dimension_table,
        load_song_dimension_table,
        load_artist_dimension_table,
        load_time_dimension_table
    ]
    >> run_quality_checks
    >> end_operator
)
