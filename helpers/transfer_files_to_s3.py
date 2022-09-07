
# transfer files from databricks to s3 bucket in another account

import boto3
s3 = boto3.resource(
    service_name='s3',
    region_name='us-west-2',
    aws_access_key_id='<access_key_id>',
    aws_secret_access_key='<secret_access_key>'
)
bucket =s3.Bucket('exasol-data')

# "/dbfs/temp_path/" should be empty
dbutils.fs.cp("s3a://<source_bucket>/<file_object_prefix>",'/dbfs/temp_path/',recurse=True,)

for f in dbutils.fs.ls("/dbfs/temp_path/"):
    bucket.upload_file(f.path.replace('dbfs:','/dbfs/'), 'destination_file_prefix')


    
# transfer files cross account using boto3
# reference and credits: https://markgituma.medium.com/copy-s3-bucket-objects-across-separate-aws-accounts-programmatically-323862d857ed

source_client = boto3.client(
    's3',
    SOURCE_AWS_ACCESS_KEY_ID,
    SOURCE_AWS_SECRET_ACCESS_KEY,
)
source_response = soure_client.get_object(
  Bucket=<SOURCE_BUCKET>,
  Key=<OBJECT_KEY>
)
destination_client = boto3.client(
    's3',
    DESTINATION_AWS_ACCESS_KEY_ID,
    DESTINATION_AWS_SECRET_ACCESS_KEY,
)
destination_client.upload_fileobj(
  source_response['Body'],
  DESTINATION_BUCKET,
  <FOLDER_LOCATION_IN_DESTINATION_BUCKET>,
)


# some more references
# https://gist.github.com/bwicklund/0000c9066845afc928e128f2ff79cba1
