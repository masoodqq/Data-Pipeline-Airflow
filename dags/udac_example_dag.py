from datetime import datetime, timedelta
import pendulum
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from airflow.operators.bash_operator import BashOperator

#AWS_KEY = os.environ.get('AWS_KEY','')
# AWS_SECRET = os.environ.get('AWS_SECRET')
'''
The data files are stored in S3
staging events date range is from 11/1/2018 to 11/30/2018
I am starting from one day earlier to demonstrate that task will continue even if
there is no file on 10/31/2018.
'''
default_args = {
    'owner': 'udacity',    
    'start_date': datetime(2018,10, 31),
    #'start_date': datetime.utcnow(),
    'end_date': datetime(2018, 11, 30),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'catchup': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          #schedule_interval='@daily'
          schedule_interval='@hourly'
          #schedule_interval='@once'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

'''
#for testing
t1=BashOperator(
            task_id="display",
            dag=dag,
            #bash_command="echo 'execution date : {{ ds }} modified by macros.ds_add to add 5 days : {{ macros.ds_add(ds, 5) }}'"
            bash_command="""echo 'log_data/{{macros.ds_format(ds, "%Y-%m-%d", "%Y/%m")}}/{{ds}}-events.json'"""
    
)

'''

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data/{execution_date.year}/{execution_date.month:02d}/{ds}-events.json",
    json_format="s3://udacity-dend/log_json_path.json"
    
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket="udacity-dend",
    #s3_key="song_data/A/B/C/TRABCEI128F424C983.json"
    s3_key="song_data/",
    json_format="auto"
    
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_statement = SqlQueries.songplay_table_insert
)

'''
LoadDimensionOperator had mode parameter 
that needs to be assigned values of truncate or insert.

This gives an option to delete the current data from Dimension tables
or insert the new records in the Dimension tables.

if mode is insert then insert sql statment is executed
if mode is truncate then dimension table will first be truncated then insert statement is run.


'''

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="users",
    mode="truncate",
    sql_statement = SqlQueries.user_table_insert    
    
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songs",
    mode="truncate",
    sql_statement = SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="artists",
    mode="truncate",
    sql_statement = SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="time",
    mode="truncate",
    sql_statement = SqlQueries.time_table_insert
)


#expected_result is a value if it matched then the test will fail.
#I am checking expected_result > row_count to pass the check.

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    dq_checks=[{'check_sql':'select count(*) from users' , 'expected_result': 0},
               {'check_sql':'select count(*) from songs' , 'expected_result': 0},
               {'check_sql': 'select count(*) from artists', 'expected_result':0 },
               {'check_sql': 'select count(*) from time', 'expected_result':0 }]
)


end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator>>[stage_songs_to_redshift,stage_events_to_redshift]
[stage_songs_to_redshift,stage_events_to_redshift] >> load_songplays_table


#stage_events_to_redshift>>load_songplays_table
#stage_songs_to_redshift>>load_songplays_table

#load_songplays_table>>load_user_dimension_table
#load_songplays_table>>load_song_dimension_table
#load_songplays_table>>load_artist_dimension_table
#load_songplays_table>>load_time_dimension_table
load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table ]

#load_user_dimension_table>>run_quality_checks
#load_song_dimension_table>>run_quality_checks
#load_artist_dimension_table>>run_quality_checks
#load_time_dimension_table>>run_quality_checks

[load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table ] >> run_quality_checks
run_quality_checks>>end_operator


