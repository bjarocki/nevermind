import pyinotify
from nevermind.storage import DB, File

# Initiate StorageDB connection with default configuration
StorageDB = DB({})


class InotifierEventHandler(pyinotify.ProcessEvent):
    def process_IN_ACCESS(self, event):
        print "ACCESS event:", event.pathname

    def process_IN_ATTRIB(self, event):
        print "ATTRIB event:", event.pathname

    def process_IN_CLOSE_NOWRITE(self, event):
        print "CLOSE_NOWRITE event:", event.pathname

    def process_IN_CLOSE_WRITE(self, event):
        print "CLOSE_WRITE event:", event.pathname
        lf = File(event.pathname)
        sf = File(event.pathname)
        sf.load(StorageDB.get(event.pathname))
        if not sf.path or lf.mtime != sf.mtime:
            print('Storing file in DB')
            StorageDB.set(lf.storage_object)
        print(lf.storage_object)

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
