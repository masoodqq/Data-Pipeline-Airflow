from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    '''
    This class checks the row count of tables provided in the tables array. 
    '''
    
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 dq_checks="",
                 redshift_conn_id="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks

    def execute(self, context):
        error_count = 0
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)        
        
        for check in self.dq_checks:
            sql = check.get('check_sql')
            exp_result = check.get('expected_result')
            records = redshift.get_records(sql)[0]
          
            if  records[0] > exp_result:
                self.log.info(f"Data quality for {sql} check passed with {records[0]} records")                
            else:
                error_count += 1
                failing_tests.append(sql)

            if error_count > 0:
                self.log.info('Tests failed')
                self.log.info(failing_tests)
                raise ValueError('Data quality check failed')
            
            
        
        '''
        for table in self.tables:
            self.log.info(f"Running DataQualityOperator for table {table}")
            records = redshift.get_records(f"select count(*) from {table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
            self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records")
        '''
            
            
        
        
        