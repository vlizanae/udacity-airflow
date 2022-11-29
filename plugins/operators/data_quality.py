from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tests=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tests = tests or dict()

    def execute(self, context):
        self.log.info(f"Executing {self.__class__.__name__}")
        
        # Connection
        self.log.info("Connecting to Redshift")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # Running tests
        for name, data in self.tests.items():
            data['test'].update_parameters(data['parameters'])
            self.log.info(f"Running test {name}")
            self.log.info(data['test'].get_query())
            
            records = redshift.get_records(data['test'].get_query())
            data['test'].check_against_records(records)
            self.log.info(f"Test {name} successful")