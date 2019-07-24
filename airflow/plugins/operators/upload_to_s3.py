import airflow.hooks.S3_hook as S3
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import os


class UploadToS3Operator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 s3_id="",
                 bucket="",
                 key="",
                 directory="",
                 *args, **kwargs):

        super(UploadToS3Operator, self).__init__(*args, **kwargs)
        self.s3_id = s3_id
        self.bucket = bucket
        self.key = key
        self.directory = directory

    def execute(self, context):
        # Create S3 hook
        self.log.info("S3 Id: {}".format(self.s3_id))
        hook = S3.S3Hook(aws_conn_id=self.s3_id)
        # Check if files already exist in S3
        dir_list = hook.list_keys(self.bucket, prefix=self.key)
        self.log.info("There are currently {} files in {}".format(len(dir_list), self.bucket+self.key))
        # Skip if so
        if len(dir_list) > 1:
            return
        else:
            # Upload if not
            for filename in os.listdir(self.directory):
                hook.load_file(self.directory + filename, self.key+filename, self.bucket, True)
