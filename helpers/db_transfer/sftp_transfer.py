def get_ftp_connection():
    """
        get sftp connnection and return an instance
    """
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    _sftpclient = pysftp.Connection(host=ftpserver, username=ftpuser, password=ftppassword,cnopts=cnopts)
    return _sftpclient

MAX_RETRIES = 3
MB = 1024*1024

for s3_path in objects_path_list:
    sftp_conn = get_ftp_connection()
    
    file_name = s3_path.strip("/").split("/")[-1]
    remote_path=f"{UPLOAD_PATH}/{file_name}"
    s3obj = S3Url(s3_path)
    
    source_file_size = Utilities.get_s3_fileobj_size(s3_path)
    
    print(f"\n\nstarting copy from '{s3_path}' to '{remote_path}' with source_file_size as {source_file_size/MB:.2f} Mb")
    
    is_completed=False
    try:
        is_uploading_file=True
        retrying=False
        retries=0
        start_byte=""
        stop_byte=""

        while is_uploading_file:
            if not retrying:
                try:
                    with sftp_conn.open(remote_path,"wb") as fileobj:
                        transfer_callback = TransferCallback(source_file_size)
                        
                        s3.Bucket(s3obj.bucket).Object(s3obj.objectKey).download_fileobj(
                            fileobj,
                            Config = TransferConfig(multipart_threshold=32388608,multipart_chunksize=32388608,io_chunksize=1062144),
                            Callback=transfer_callback)
                    is_completed=True
                    break
                except Exception as e:
                    print(f"Encountered error while copying {s3_path}:")
                    print(e)
                finally:
                    sftp_conn.close()

            elif retrying and retries < MAX_RETRIES:
                sftp_conn = get_ftp_connection()
                retries+=1
                
                try:
                    transfer_callback = TransferCallback(source_file_size)
                    with sftp_conn.open(remote_path,"ab") as fileobj:
                        start_byte=sftp_conn.lstat(remote_path).st_size
                        
                        if start_byte==source_file_size:
                            print("\n\nSkipping, file already copied.")
                            is_completed=True
                            break
                        
                        # resuming the progress for callback
                        transfer_callback.__call__(start_byte-1)
                        print(f"\n\nreading again from byte offset : {start_byte}")
                        chunk_obj = s3_client.get_object(Bucket=s3obj.bucket, Key=s3obj.objectKey, Range='bytes={}-{}'.format(start_byte, stop_byte))['Body']
                        print(f"retrying, retry {retries}")
                        copyfileobj_with_callback(chunk_obj, fileobj,callback=transfer_callback)

                        is_completed=True
                    break
                except Exception as e:
                    print(f"Encountered error while copying {s3_path}:")
                    print(e)
                finally:
                    sftp_conn.close()
            else:
                print(f"reached max limit of retries {MAX_RETRIES}")
                is_uploading_file=False

    except Exception as e:
        print(e)
    finally:
        sftp_conn.close()
    
    if is_completed:
        print(f"file {s3_path} successfully transferred to remote path {remote_path}")
