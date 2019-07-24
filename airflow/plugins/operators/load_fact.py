from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries


class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 dest_table_name="",
                 dest_table_create_params="",
                 dest_table_insert_values="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dest_table_name = dest_table_name
        self.dest_table_insert_values = dest_table_insert_values
        self.dest_table_create_params = dest_table_create_params 

    def execute(self, context):
        # Create the Redshift hook
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # Create table if it doesn't exist
        create_query = SqlQueries.create_starter.format(self.dest_table_name) + self.dest_table_create_params
        self.log.info("Creating the table {} if it doesn't exist with the following query: {}".format(self.dest_table_name, create_query))
        redshift.run(create_query)
        
        # Insert the fact table data
        insert_query = SqlQueries.insert_starter.format(self.dest_table_name) + self.dest_table_insert_values
        self.log.info("Loading data into the table {} with the following query: {}".format(self.dest_table_name, insert_query))
        redshift.run(insert_query)