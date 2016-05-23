from nevermind.storage import DB, Queue

# Initiate StorageDB connection with default configuration
StorageDB = DB({})

# Initiate DownloaderQueue with default configuration
DownloaderQueue = Queue({})

cursor = StorageDB.changes

for data in cursor:
    sf = data.get('new_val')

    # in case I delete records from DB and this is actually empty document
    # Normally this should not happen
    if not sf:
        continue

    # store job in a queue
    jobid = DownloaderQueue.add({
        'type': 'cluster_update',
        'metadata': sf})

    # let's just print the jobid for now.
    print(jobid)
