from airflow.hooks.postgres_hook import PostgresHook
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                destination_table="",
                dim_sql="",
                append_mode=True,
                *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.destination_table = destination_table
        self.dim_sql = dim_sql
        self.append_mode = append_mode

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(self.dim_sql)
        if self.append_mode:
            self.log.info(f'Appending data into destination Redshift table: {self.destination_table}')
            dim_sql = f'INSERT INTO {self.destination_table} {self.dim_sql}'
            redshift.run(dim_sql)
        else:
            self.log.info(f'Clearing data from destination Redshift table: {self.destination_table}')
            redshift.run(f'DELETE FROM {self.destination_table}')

            self.log.info(f'Inserting data into destination Redshift table: {self.destination_table}')
            dim_sql = f'INSERT INTO {self.destination_table} {self.dim_sql}'
            redshift.run(dim_sql)