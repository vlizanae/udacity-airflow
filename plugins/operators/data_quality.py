from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tests_has_rows=None,
                 tests_no_rows=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tests_has_rows = tests_has_rows or []
        self.tests_no_rows = tests_no_rows or []
        
    def raise_if_has_rows(self, redshift, count_query, inverse=False):
        self.log.info('Running test:')
        self.log.info(count_query)
        
        records = redshift.get_records(count_query)
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError('Data quality check failed. Query returned no results')
        if not inverse and records[0][0] > 0:
            raise ValueError('Data quality check failed. Query returned rows')
        if inverse and records[0][0] < 1:
            raise ValueError('Data quality check failed. Query returned no rows')
            
        self.log.info('Test passed succesfully')
        

    def execute(self, context):
        self.log.info(f"Executing {self.__class__.__name__}")
        
        # Connection
        self.log.info("Connecting to Redshift")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # Running tests
        for count_query in self.tests_has_rows:
            self.raise_if_has_rows(redshift, count_query, inverse=True)
        for count_query in self.tests_no_rows:
            self.raise_if_has_rows(redshift, count_query)
        