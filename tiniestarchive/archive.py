import tiniestarchive

class Archive:
    def __new__(cls, root=None, **kwargs):
        if root:
            if isinstance(root, str) and root.startswith('http'):
                return super(Archive, tiniestarchive.HttpArchive).__new__(tiniestarchive.HttpArchive)

        return super(Archive, tiniestarchive.FileArchive).__new__(tiniestarchive.FileArchive)

class Resource:
    def __new__(cls, root=None, **kwargs):
        if root:
            if isinstance(root, str) and root.startswith('http'):
                return super(Resource, tiniestarchive.HttpResource).__new__(tiniestarchive.HttpResource)

        return super(Archive, tiniestarchive.FileResource).__new__(tiniestarchive.FileResource)
    
class Instance:
    def __new__(cls, root=None, **kwargs):
        if root:
            if isinstance(root, str) and root.startswith('http'):
                return super(Resource, tiniestarchive.HttpInstance).__new__(tiniestarchive.HttpInstance)

        return super(Archive, tiniestarchive.FileInstance).__new__(tiniestarchive.FileInstance)
    
