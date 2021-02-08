from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from helpers import SqlQueries

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    template_fields = ('s3_key',)

    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        JSON '{}'
        COMPUPDATE {}
    """

    @apply_defaults
    def __init__(
                self,
                redshift_conn_id="",
                aws_credentials_id="",
                table="",
                s3_bucket="",
                s3_key="",
                json="",
                region='us-west-2',
                compupdate="OFF",
                *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json = json
        self.region = region
        self.compupdate = compupdate
        self.aws_credentials_id = aws_credentials_id

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f'Clearing data from destination Redshift table: {self.table}')
        redshift.run('DELETE FROM {}'.format(self.table))

        self.log.info(f'Copying data from S3 to Redshift')
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.json,
            self.compupdate
        )
        redshift.run(formatted_sql)