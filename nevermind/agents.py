import pyinotify
import subprocess
import socket
from nevermind.storage import DB, File, Queue

# Initiate StorageDB connection with default configuration
StorageDB = DB({})

# Initiate DownloaderQueue with default configuration
DownloaderQueue = Queue({})


class InotifierEventHandler(pyinotify.ProcessEvent):
    def process_IN_ACCESS(self, event):
        print "ACCESS event:", event.pathname

    def process_IN_ATTRIB(self, event):
        print "ATTRIB event:", event.pathname

    def process_IN_CLOSE_NOWRITE(self, event):
        print "CLOSE_NOWRITE event:", event.pathname

    def process_IN_CLOSE_WRITE(self, event):
        print "CLOSE_WRITE event:", event.pathname

        # for sake of POC just compare _rounded_ mtime to decide
        # if this is a new file or one rsynced
        DownloaderQueue.add({
            'type': 'local_new',
            'metadata': {
                'path': event.pathname
                }
            })

    def process_IN_CREATE(self, event):
        print "CREATE event:", event.pathname

    def process_IN_DELETE(self, event):
        print "DELETE event:", event.pathname

    def process_IN_MODIFY(self, event):
        print "MODIFY event:", event.pathname

    def process_IN_OPEN(self, event):
        print "OPEN event:", event.pathname


class InotifierAgent:
    def __init__(self, path):
        self.path = path
        self.watch_manager.add_watch(self.path, self.flags, rec=True)

    @property
    def notifier(self):
        if 'nt' not in self.__dict__:
            self.nt = pyinotify.Notifier(
                    self.watch_manager,
                    self.event_handler)
        return self.nt

    @property
    def watch_manager(self):
        if 'wm' not in self.__dict__:
            self.wm = pyinotify.WatchManager()
        return self.wm

    @property
    def event_handler(self):
        if 'eh' not in self.__dict__:
            self.eh = InotifierEventHandler()
        return self.eh

    @property
    def flags(self):
        return \
            pyinotify.IN_CLOSE_WRITE | \
            pyinotify.IN_CREATE | \
            pyinotify.IN_MODIFY

    def run(self):
        self.notifier.loop()


class Worker:
    def __init__(self):
        pass

    @staticmethod
    def local_new(metadata):
        path = metadata.get('path')

        # get info about local file
        lf = File(path)

        # grab what do we have in the storageDB about this guy
        sf = File(path)
        sf.load(StorageDB.get(path))

        # compare md5 with what we have in the DB
        if not sf.path or lf.md5 != sf.md5:
            print('Storing file in DB: local md5: {} / DB md5: {}'.format(lf.md5, sf.md5))
            StorageDB.set(lf.storage_object)

        return True

    @staticmethod
    def cluster_update(metadata):
        path = metadata.get('path')

        # ignore if len(synced_nodes) > 1 - we should only verify the first
        # information about a fresh storage update. len(synced_nodes) == 1 is
        # a sign that somebody has just uploaded a fresh file
        if len(metadata.get('synced_nodes')) != 1:
            return True

        # get local file info
        lf = File(path)

        # compare md5 with what we have in the DB
        if lf.md5 != metadata.get('md5'):
            print('Getting the file with rsync: local md5: {} / DB md5: {}'.format(lf.md5, metadata.get('md5')))
            source_node = metadata.get('source_node')

            failed = subprocess.call([
                'rsync', '--temp-dir', '/tmp',
                '-Ra', '{}::storage{}'.format(source_node, path),
                '/'
            ])

            if not failed:
                StorageDB.update(path, {'synced_nodes': {socket.gethostname(): True}})
                return True
            else:
                return False

        return True

    @staticmethod
    def doit(job):
        jobtype = job.get('type')

        if jobtype == 'local_new':
            return Worker.local_new(job.get('metadata'))

        elif jobtype == 'cluster_update':
            return Worker.cluster_update(job.get('metadata'))
