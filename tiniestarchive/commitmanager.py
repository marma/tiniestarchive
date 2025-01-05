from os import makedirs
from sys import stdout, stderr
from shutil import rmtree, move
from os.path import exists, join
from tempfile import gettempdir
from uuid import uuid4

class CommitManager(object):
    def __init__(self, target_dir : str, wrappee_fun, tmpdir : str = None, resource = None, archive = None):
        self.wrappee_fun = wrappee_fun
        self.tmpdir = tmpdir
        self.target_dir = target_dir
        self.resource = resource
        self.archive = archive

    def __enter__(self):
        if not self.tmpdir:
            self.tmpdir = join(gettempdir(), str(uuid4()))

        if exists(self.tmpdir):
            raise Exception("Temporary directory already exists")
        
        if exists(self.target_dir):
            raise Exception("Target directory already exists")

        self.wrappee = self.wrappee_fun(self.tmpdir)

        return self.wrappee

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type:
            #print(f'rmtree({self.tmpdir})', file=stderr)
            rmtree(self.tmpdir)
        else:
            if self.tmpdir != self.target_dir:
                if exists(self.target_dir):
                    raise Exception("Target directory already exists")

                #print(f'move({self.tmpdir}, {self.target_dir})', file=stderr)
                move(self.tmpdir, self.target_dir)
