from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    
    

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_statement="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.sql_statement=sql_statement

    def execute(self, context):
        create_songplays_sql = """CREATE TABLE IF NOT EXISTS public.songplays (
            playid varchar(32) NOT NULL,
            start_time timestamp NOT NULL,
            userid int4 NOT NULL,
            "level" varchar(256),
            songid varchar(256),
            artistid varchar(256),
            sessionid int4,
            location varchar(256),
            user_agent varchar(256),
            CONSTRAINT songplays_pkey PRIMARY KEY (playid)
        );"""
        
        insert_songplays_sql = """
        insert into songplays(playid, start_time, userid, level, songid, artistid, sessionid, location, user_agent) {}        
        """
        
        self.log.info('LoadFactOperator')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('create fact table songplays ',create_songplays_sql)
        redshift.run(create_songplays_sql)
        
        self.log.info('insert_songplays_sql = ',insert_songplays_sql)
        self.log.info('self.sql_statement = ',self.sql_statement)
        insert_statement = insert_songplays_sql.format(self.sql_statement)
        self.log.info( 'insert statement = ', insert_statement)
        redshift.run(insert_statement)
        
        
        
