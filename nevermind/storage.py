import os
import uuid
import time
import socket
import rethinkdb as r
from rethinkdb.errors import RqlRuntimeError


class DB:
    def __init__(self, conf):
        self.conf = conf
        self.conn = r.connect(self.host, self.port)
        try:
            r.db_create(self.db).run(self.conn)
            print 'Database setup completed.'
        except RqlRuntimeError:
            try:
                r.db(self.db).table_create(
                        self.table,
                        primary_key='path').run(self.conn)
            except:
                pass

    @property
    def host(self):
        return self.conf.get('host') or 'localhost'

    @property
    def port(self):
        return self.conf.get('port') or 28015

    @property
    def db(self):
        return self.conf.get('db') or 'storage'

    @property
    def table(self):
        return self.conf.get('table') or 'storage'

    @property
    def changes(self):
        return r.db(self.db).table(self.table).changes().run(self.conn)

    def get(self, id):
        return r.db(self.db).table(self.table).get(id).run(self.conn)

    def update(self, id, data):
        return r.db(self.db).table(self.table).get(id).update(data).run(self.conn)

    def set(self, data):
        return r.db(self.db).table(self.table).insert(
                data,
                conflict="replace"
            ).run(self.conn)


class File:
    def __init__(self, path):
        self.path = path
        self.stat = os.stat(path)
        self.data = {}

    @classmethod
    def load(cls, data):
        if data:
            cls.data = data
            cls.path = data.get('path')
        return cls

    @property
    def size(self):
        return self.data.get('size') or self.stat.st_size

    @property
    def path(self):
        return self.data.get('path') or self.path

    @property
    def mtime(self):
        return self.data.get('mtime') or self.stat.st_mtime

    @property
    def md5(self):
        # let's just fake this for now
        return self.data.get('md5') or str(uuid.uuid4())

    @property
    def updated(self):
        return self.data.get('updated') or time.time()

    @property
    def synced_nodes(self):
        return self.data.get('synced_nodes') or {socket.gethostname(): True}

    @property
    def source_node(self):
        return self.data.get('source_node') or socket.gethostname()

    @property
    def storage_object(self):
        return {
               'path': self.path,
               'source_node': self.source_node,
               'synced_nodes': self.synced_nodes,
               'updated': self.updated,
               'mtime': self.mtime,
               'size': self.size,
               'md5': self.md5
        }
