from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
import sys

#from airflow import macros

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    #template_fields = ("",)
    json_copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION 'us-west-2'
        JSON '{}' """

    
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",                 
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json_format="",
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket=s3_bucket
        self.s3_key = s3_key
        self.json_format = json_format
        
        
        

    def execute(self, context):        
            aws_hook = AwsHook(self.aws_credentials_id)
            credentials = aws_hook.get_credentials()
            redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
            if (self.table == "staging_events"):
                #self.log.info("Creating staging_events table in Redshift")
                #redshift.run(StageToRedshiftOperator.create_staging_events_sql)
                #self.log.info("Finished Creating staging_events table in Redshift")
                self.log.info("Copying data from S3 to Redshift table staging_events")
                #2018-02-18T06:00:00+00:00
                #date = strstrptime(self.execution_date.format(**context), "")
                #filepath= airflow.macros.ds_format(self.execution_date, "%Y-%m-%d", "%Y/%m")+self.execution_date+"-events.json"
                #self.log.info("filepath = ", filepath)
                #self.s3_key = self.s3_key + filepath
                rendered_key = self.s3_key.format(**context)
                #self.log.info("rendered_key = ", rendered_key)
                s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
                self.log.info("s3_path = ", s3_path)
                #self.log.info("json_copy_sql = ",StageToRedshiftOperator.json_copy_sql)
                json_copy_sql_formated = StageToRedshiftOperator.json_copy_sql.format(
                        self.table,
                        s3_path,
                        credentials.access_key,
                        credentials.secret_key,
                        self.json_format
                )
                self.log.info("json_copy_sql_formated = ",json_copy_sql_formated)
                redshift.run(json_copy_sql_formated)

            if (self.table == "staging_songs"):
                #self.log.info("Creating staging_songs table in Redshift")
                #redshift.run(StageToRedshiftOperator.create_staging_songs_sql)
                #self.log.info("Finished Creating staging_songs table in Redshift")
                self.log.info("Copying data from S3 to Redshift table staging_songs")
                rendered_key = self.s3_key.format(**context)
                self.log.info("rendered_key = ", rendered_key)
                s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
                self.log.info("songs_s3_path = ", s3_path)
                self.log.info("songs_json_copy_sql = ",StageToRedshiftOperator.json_copy_sql)
                json_copy_sql_formated = StageToRedshiftOperator.json_copy_sql.format(
                        self.table,
                        s3_path,
                        credentials.access_key,
                        credentials.secret_key,
                        self.json_format
                )
                self.log.info("songs_json_copy_sql_formated = ",json_copy_sql_formated)
                redshift.run(json_copy_sql_formated)
