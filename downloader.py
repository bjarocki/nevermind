import json
import socket
import subprocess
from nevermind.storage import Queue, DB

# Initiate StorageDB connection with default configuration
StorageDB = DB({})

# Initiate DownloaderQueue with default configuration
DownloaderQueue = Queue({})

while True:
    jobs = DownloaderQueue.get()

    for queue_name, job_id, job in jobs:
        job = json.loads(job)
        print("New job:", job)

        path = job.get('path')

        if job['synced_nodes'].get(socket.gethostname()):
            print('Already synced!', job)
            continue

        print('Need to sync this guy!', job)
        source_node = job.get('source_node')
        failed = subprocess.call([
            'rsync', '--temp-dir', '/tmp',
            '-Ra', '{}::storage{}'.format(source_node, path),
            '/'
        ])

        if not failed:
            DownloaderQueue.ack(job_id)
            StorageDB.update(path, {'synced_nodes': {socket.gethostname(): True}})
        else:
            print('Upssss...', job)
