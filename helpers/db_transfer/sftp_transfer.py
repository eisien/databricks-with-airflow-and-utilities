import sys
import threading
import shutil

MB = 1024 * 1024

class TransferCallback:
    """
    Handle callbacks from the transfer manager.

    The transfer manager periodically calls the __call__ method throughout
    the upload and download process so that it can take action, such as
    displaying progress to the user and collecting data about the transfer.
    
    Parameters:
        target_size, int: target file size in bytes
        
    """

    def __init__(self, target_size):
        self._target_size = target_size/MB
        self._total_transferred = 0
        self._lock = threading.Lock()
        self.thread_info = {}

    def __call__(self, bytes_transferred):
        """
        The callback method that is called by the transfer manager.

        Display progress during file transfer and collect per-thread transfer
        data. This method can be called by multiple threads, so shared instance
        data is protected by a thread lock.
        
        Parameters:
            bytes_transferred, int
        
        """
        thread = threading.current_thread()
        with self._lock:
            self._total_transferred += bytes_transferred
            if thread.ident not in self.thread_info.keys():
                self.thread_info[thread.ident] = bytes_transferred
            else:
                self.thread_info[thread.ident] += bytes_transferred

            target = self._target_size * MB
            sys.stdout.write(
                f"\r{self._total_transferred} of {target} bytes transferred "
                f"({(self._total_transferred / target) * 100:.2f}%).")
            sys.stdout.flush()
            
            
COPY_BUFSIZE = 1024*1024  # bytes

def copyfileobj_with_callback(fsrc, fdst, length=0,callback=None):
    
    """copy data from file-like object fsrc to file-like object fdst
    
    Parameters:
        fsrc: source file-like object
        fdst: destination file-like object
        length: buffer length in bytes, default=0
        callback: a callback object with method '__call__(bytes_transferred)' implemented
    
    """
    if not length:
        length = COPY_BUFSIZE
    # Localize variable access to minimize overhead.
    fsrc_read = fsrc.read
    fdst_write = fdst.write
    while True:
        buf = fsrc_read(length)
        if not buf:
            break
        fdst_write(buf)
        if callback is not None:
            callback.__call__(length)
            

            
            
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
