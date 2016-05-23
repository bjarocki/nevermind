import json
from nevermind.storage import Queue
from nevermind.agents import Worker

# Initiate DownloaderQueue with default configuration
DownloaderQueue = Queue({})


while True:
    jobs = DownloaderQueue.get()

    for queue_name, job_id, job in jobs:
        job = json.loads(job)
        print("New job:", job)

        if Worker.doit(job):
            DownloaderQueue.ack(job_id)
        else:
            print('Upssss...', job)
