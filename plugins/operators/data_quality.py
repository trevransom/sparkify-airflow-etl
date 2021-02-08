from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                dq_checks="",
                *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for sql_check in self.dq_checks:
            check = sql_check['check_sql']
            expected_result = sql_check['expected_result']
            results = redshift.get_records(sql_check)[0][0]
            self.log.info(f'Results: {results}')
            
            if results != expected_result:
                self.log.info(f'Expected result was: {self.expected_result}. We got: {results}')
                raise ValueError("Data quality check failed")

            else:
                self.log.info("Data quality check was successful! No null values")
