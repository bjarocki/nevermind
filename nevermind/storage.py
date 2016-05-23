import os
import time
import socket
import json
import subprocess
import re
import rethinkdb as r
from rethinkdb.errors import RqlRuntimeError
from pydisque.client import Client


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


class Queue:
    def __init__(self, conf):
        self.conf = conf
        self.client = Client([':'.join([self.host, str(self.port)])])
        self.client.connect()

    @property
    def host(self):
        return self.conf.get('host') or 'localhost'

    @property
    def port(self):
        return self.conf.get('port') or 7711

    @property
    def queue(self):
        return self.conf.get('queue') or 'downloader'

    def get(self):
        return self.client.get_job([self.queue])

    def add(self, job):
        return self.client.add_job(self.queue, json.dumps(job), timeout=1000)

    def ack(self, job_id):
        return self.client.ack_job(job_id)


class File:
    def __init__(self, path):
        self.path = path
        self.data = {}

    @property
    def exists(self):
        return os.path.isfile(self.path)

    @property
    def stat(self):
        if 'filestat' in self.__dict__:
            return self.filestat

        self.filestat = os.stat(self.path)

        return self.filestat

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
        if self.data.get('md5'):
            return self.data.get('md5')

        # let's do dont calculate md5 multiple time here
        if 'md5sum' in self.__dict__:
            return self.md5sum

        p = subprocess.Popen('md5sum {}'.format(self.path), stdout=subprocess.PIPE, shell=True)
        (output, err) = p.communicate()

        if p.wait() > 0:
            return None

        # let's grab the md5sum string
        match = re.match('^(?P<md5sum>\S+).*', output)

        if match:
            self.md5sum = match.group('md5sum')
        else:
            return None

        return self.md5sum

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

    def load(self, data):
        if data:
            self.data = data
            self.path = data.get('path')
        else:
            self.path = None
