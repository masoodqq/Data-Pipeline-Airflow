from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    
  

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 table="",
                 redshift_conn_id="",
                 mode="",
                 sql_statement="",                 
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.mode = mode
        self.sql_statement=sql_statement

    def execute(self, context):
       
        self.log.info('LoadDimensionOperator')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.mode=="insert":
            self.log.info('Load_user_dim_table mode is insert')
            redshift.run("insert into {} {}".format(self.table, self.sql_statement))
        else:
            self.log.info('Load_dim_{}_table mode is truncate'.format(self.table)) 
            redshift.run("Truncate table public.{}".format(self.table))
            redshift.run("insert into {} {}".format(self.table, self.sql_statement))
                         
