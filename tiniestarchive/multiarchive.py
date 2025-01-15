from tiniestarchive import Archive,Resource

class MultiArchive(Archive):
    def __init__(self, archives : list):
        self.archives = archives

    def get(self, resource_id : str):
        return MultiResource([ a.get(resource_id) for a in self.archives if a.exists(resource_id) ])


class MultiResource(Resource):
    def __init__(self, resources : list):
        self.resources = resources

        if not self.resources:
            raise Exception('No resources found')

    def open(self, resource_id : str, path : str, mode='r'):
        for r in self.resources:
            if path in r:
                return r.open(path, mode)
    
    def read(self, resource_id : str, path : str, mode : str = 'r'):
        for r in reversed(self.resources):
            if path in r:
                return r.read(path, mode)
            
    def exists(self, resource_id : str):
        for r in self.resources:
            if r.exists(resource_id):
                return True

        return False
    
    
