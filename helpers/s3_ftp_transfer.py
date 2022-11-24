from boto3.s3.transfer import TransferConfig

s3obj = S3Url(s3_file)
s3 = boto3.resource('s3')
ssh.close()
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(ftpserver, username =ftpuser, password=ftppassword, timeout=0)
sftp_conn = ssh.open_sftp()
try:
    
    with sftp_conn.open("file","wb") as fileobj:

        transfer_callback = TransferCallback(Utilities.get_s3_fileobj_size(s3_file)/MB)
        resp = s3.get_object(Bucket=bucket, Key=key, Range='bytes={}-{}'.format(start_byte, stop_byte-1))
        res = resp['Body'].read()
        s3.Bucket(s3obj.bucket).Object(s3obj.objectKey).download_fileobj(
            fileobj,
            Config = TransferConfig(multipart_threshold=32388608,multipart_chunksize=32388608,io_chunksize=1062144),
            Callback=transfer_callback)
        
except Exception as e:
    sftp_conn.close()
    raise e
