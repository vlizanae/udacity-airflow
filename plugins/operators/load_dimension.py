from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 select_query="",
                 append=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.select_query = select_query
        self.append = append

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
        if not self.append:
            self.log.info("Append is set to False, removing existent data")
            redshift.run(f'DELETE FROM {self.table};')
        
        self.log.info("Copying data from Staging to Dimmension")
        redshift.run(query)