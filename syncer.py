import socket
import subprocess
from nevermind.storage import DB

# Initiate StorageDB connection with default configuration
StorageDB = DB({})

cursor = StorageDB.changes

for data in cursor:
    sf = data.get('new_val')
    path = sf.get('path')

    if not sf['synced_nodes'].get(socket.gethostname()):
        print('Need to sync this guy!', sf)
        source_node = sf.get('source_node')
        failed = subprocess.call([
            'rsync', '--temp-dir', '/tmp',
            '-Ra', '{}::storage{}'.format(source_node, path),
            '/'
        ])
        if not failed:
            StorageDB.update(path, {'synced_nodes': {socket.gethostname(): True}})
        else:
            print('Upssss...', sf)
    else:
        print('Already synced!', sf)
