"""
Copying a file and checking its progress while it's copying.
"""

import os
import shutil
import threading
import time

des = r'<PATH/TO/SPURCE/FILE>'
src = r'<PATH/TO/DESTINATION/FILE>'


def checker(source_path, destination_path):
    """
    Compare 2 files till they're the same and print the progress.

    :type source_path: str
    :param source_path: path to the source file
    :type destination_path: str
    :param destination_path: path to the destination file
    """

    # Making sure the destination path exists
    while not os.path.exists(destination_path):
        print "not exists"
        time.sleep(.01)

    # Keep checking the file size till it's the same as source file
    while os.path.getsize(source_path) != os.path.getsize(destination_path):
        print "percentage", int((float(os.path.getsize(destination_path))/float(os.path.getsize(source_path))) * 100)
        time.sleep(.01)

    print "percentage", 100

t = threading.Thread(name='copying', target=copying_file, args=(src, des))
# Start the copying on a separate thread
t.start()
# Checking the status of destination file on a separate thread
b = threading.Thread(name='checking', target=checker, args=(src, des))
b.start()
