from os import makedirs
from sys import stdout, stderr
from shutil import rmtree, move
from os.path import exists, join
from tempfile import gettempdir
from uuid import uuid4

class IngestManager(object):
    def __init__(self, archive, resource):
        self.archive = archive
        self.resource = resource

    def __enter__(self):
        return self.resource

    def __exit__(self, exc_type, exc_value, traceback):
        resource_path = self.resource.path

        try:
            if not exc_type:
                self.archive.ingest(self.resource)
                self.resource._clear()
        finally:
            if resource_path.exists():
                print(f'resource - rmtree({resource_path})', file=stderr)
                rmtree(self.resource.path)
