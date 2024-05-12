

def split_path(u):
    SPLITS = [ 0, 4, 6, 8, 10 ]

    return '/'.join([ u[SPLITS[i]:SPLITS[i+1]] for i in range(0, len(SPLITS)-1) ] + [ u ])

# TODO: more checks
def safe_path(path):
    return path.replace('..', '').replace('//', '/')