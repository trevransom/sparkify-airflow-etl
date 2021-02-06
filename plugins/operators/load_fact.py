from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                destination_table="",
                facts_sql="",
                append_mode=True,
                *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.destination_table = destination_table
        self.facts_sql = facts_sql
        self.append_mode = append_mode

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.append_mode:
            self.log.info(f'Appending data into destination Redshift table: {self.destination_table}')
            facts_sql = f'INSERT INTO {self.destination_table} {self.facts_sql}'
            redshift.run(facts_sql)
        else:
            self.log.info(f'Clearing data from destination Redshift table: {self.destination_table}')
            redshift.run(f'DELETE FROM {self.destination_table}')

            self.log.info(f'Inserting data into destination Redshift table: {self.destination_table}')
            facts_sql = f'INSERT INTO {self.destination_table} {self.facts_sql}'
            redshift.run(facts_sql)

        # redshift.run(self.facts_sql)