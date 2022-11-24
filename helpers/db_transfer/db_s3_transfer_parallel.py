def get_grouped_files(files, max_groups):
    # group files into a number of batches as 'parallelism' 
    file_groups =[]
    max_size=max_groups
    if len(files)<=max_groups:
        for file in files:
            file_groups.append([file])
    else:
        file_groups=[ [] for i in range(max_groups) ]
        index=0
        for file in files:
            if index == max_groups:
                index=0
            file_groups[index].append(file)
            index+=1

    return file_groups
  
 
file_groups=get_grouped_files(s3_object_list,2)
print(file_groups)

futures=[]
with ThreadPoolExecutor(max_workers=10,) as executor:
    for group in file_groups:
        arguments={
                    "s3_object_list":",".join(group),
                    "SECRETS_SCOPE":SECRETS_SCOPE,
                    "SFTP_HOST":HOST,
                    "SFTP_USER_KEY":FTP_USER_KEY,
                    "SFTP_PWD_KEY":FTP_PWD_KEY,
                    "UPLOAD_DIR_PATH":UPLOAD_PATH
                    }
        future = executor.submit(dbutils.notebook.run, **{"path":"./sftp_transfer","timeout_seconds":-1,"arguments":arguments})
        futures.append((file,future))
    
