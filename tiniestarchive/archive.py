import tiniestarchive

class Archive:
    def __new__(cls, root=None, **kwargs):
        if root:
            if isinstance(root, str) and root.startswith('http'):
                return super(Archive, tiniestarchive.HttpArchive).__new__(tiniestarchive.HttpArchive)

        return super(Archive, tiniestarchive.FileArchive).__new__(tiniestarchive.FileArchive)
