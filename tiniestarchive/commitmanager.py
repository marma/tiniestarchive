from os import makedirs
from sys import stdout, stderr
from shutil import rmtree, move
from os.path import exists, join
from tempfile import gettempdir
from uuid import uuid4

class CommitManager(object):
    def __init__(self, resource, instance_fun, tmpdir : str = None):
        self.instance_fun = instance_fun
        self.tmpdir = tmpdir
        self.resource = resource

    def __enter__(self):
        if not self.tmpdir:
            self.tmpdir = join(gettempdir(), str(uuid4()))

        print("__enter__" + self.tmpdir, file=stderr)

        self.instance = self.instance_fun(self.tmpdir)

        return self.instance

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            if not exc_type:
                self.resource.update(self.instance)
        finally:
            if exists(self.tmpdir):
                print(f'instance - rmtree({self.tmpdir})', file=stderr)
                rmtree(self.tmpdir)
