from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 staging_table="",
                 create_params="",
                 s3_path="",
                 is_json="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.staging_table = staging_table
        self.create_params = create_params
        self.s3_path = s3_path
        self.is_json = is_json

    def execute(self, context):
        # Create AWS and Redshift hooks
        self.log.info("AWS Credential Id: {}".format(self.aws_credentials_id))
        self.log.info("Redshift Id: {}".format(self.redshift_conn_id))
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Dropping Redshift staging_table if exists")
        redshift.run(SqlQueries.drop_starter.format(self.staging_table))
        
        self.log.info("Creating Redshift staging_table")
        redshift.run(SqlQueries.create_starter.format(self.staging_table) + self.create_params)
        
        self.log.info("Copying data from S3 to Redshift")
        if self.is_json:
            formatted_sql = SqlQueries.staging_json_copy.format(
                self.staging_table,
                self.s3_path,
                credentials.access_key,
                credentials.secret_key,
            )
        else:
            formatted_sql = SqlQueries.staging_csv_copy.format(
                self.staging_table,
                self.s3_path,
                credentials.access_key,
                credentials.secret_key,
            )
        redshift.run(formatted_sql)
