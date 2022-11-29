from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 select_query="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.select_query = select_query

    def execute(self, context):
        self.log.info(f"Executing {self.__class__.__name__}")
        
        # Building the query
        query = f"""
        INSERT INTO {self.table}
        {self.select_query}
        """
        self.log.info(f"Query:\n{query}")
        
        # Connection
        self.log.info("Connecting to Redshift")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # Execution
        self.log.info("Copying data from Staging to Fact")
        redshift.run(query)
