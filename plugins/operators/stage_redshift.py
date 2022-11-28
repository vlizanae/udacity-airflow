import os

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ('s3_key', )
    
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
                 s3_key="",
                 json='auto',
                 delimiter=",",
                 timeformat='auto',
                 region='us-west-2',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json = json
        self.delimiter = delimiter
        self.timeformat = timeformat
        self.region = region

    def execute(self, context):
        self.log.info(f"Executing {self.__class__.__name__}")
        
        # Building the query
        from_ = f's3://{os.path.join(self.s3_bucket, self.s3_key.format(**context))}'
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
