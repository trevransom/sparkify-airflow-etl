from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                expected_null_result="",
                target_column="",
                destination_table="",
                *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.destination_table = destination_table
        self.expected_null_result = expected_null_result
        self.target_column = target_column

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f'Checking data from destination Redshift table: {self.destination_table}')
        results = redshift.get_records(f'SELECT COUNT(*) FROM {self.destination_table} WHERE {self.target_column} is null')[0][0]
        self.log.info(f'Results: {results}')
        
        if results != self.expected_null_result:
            self.log.info(f'Expected result was: {type(self.expected_null_result)}. We got: {type(results)}')
            raise ValueError("Data quality check failed")

        else:
            self.log.info("Data quality check was successful! No null values")