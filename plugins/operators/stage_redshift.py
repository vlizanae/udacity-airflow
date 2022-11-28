import os

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ('execution_date', )
    
    copy_sql = """
        COPY {copy}
        FROM '{from_}'
        JSON '{json}'
        DELIMITER '{delimiter}'
        TIMEFORMAT '{timeformat}'
        REGION '{region}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 s3_bucket="",
                 s3_key=None,
                 execution_date=None,
                 json='auto',
                 delimiter=",",
                 timeformat='auto',
                 region='us-west-2',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.execution_date = execution_date
        self.json = json
        self.delimiter = delimiter
        self.timeformat = timeformat
        self.region = region

    def execute(self, context):
        self.log.info(f"Executing {self.__class__.__name__}")
        
        # Building the query
        from_ = f's3://{self.s3_bucket}'
        if self.execution_date:
            from_ = os.path.join(from_, str(self.execution_date.year), str(self.execution_date.month))
        if self.s3_key:
            from_ = os.path.join(from_, self.s3_key)
        from_ = os.path.join(from_, '*.json')

        query = self.copy_sql.format(
            copy=self.table,
            from_=from_,
            json=self.json,
            delimiter=self.delimiter,
            timeformat=self.timeformat,
            region=self.region
        )
        self.log.info(f"Query:\n{query}")
        
        # Connection
        self.log.info("Connecting to Redshift")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # Execution
        self.log.info("Copying data from S3 to Redshift")
        redshift.run(query)
