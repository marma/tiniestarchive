import streaming_form_data
from streaming_form_data import StreamingFormDataParser
from streaming_form_data.targets import FileTarget, ValueTarget, SHA256Target
from itertools import count
from os import makedirs
from os.path import exists, join, dirname
import logging
from urllib.parse import unquote

def split_path(u):
    SPLITS = [ 0, 4, 6, 8, 10 ]

    return '/'.join([ u[SPLITS[i]:SPLITS[i+1]] for i in range(0, len(SPLITS)-1) ] + [ u ])

# TODO: more checks
def safe_path(path):
    return path.replace('..', '').replace('//', '/')


async def save_to_tmp(instance_id, tmpdir, request):
    parser = StreamingFormDataParser(headers=request.headers)
    data = ValueTarget()
    parser.register('data', data)
    
    headers = dict(request.headers)
    filenames = []
    checksums = {}
    i =0
    #logging.info(f"Headers: {headers}")

    # register targets for files and checksums
    for i in count():
        filename = headers.get(f'filename{i}', None)
        if filename is None:
            break

        filename = safe_path(unquote(filename))
        filenames.append(filename)
        filepath = join(tmpdir, filename)
        logging.info(f"Uploading {filename} to {filepath}")

        if not exists(dirname(filepath)):
            makedirs(dirname(filepath))

        checksums[filename] = SHA256Target()
        parser.register(f'file{i}', FileTarget(filepath))
        parser.register(f'file{i}', checksums[filename])

    # stream request into files
    async for chunk in request.stream():
        #print('CHONK!')
        parser.data_received(chunk)

    return filenames, { key:('sha256:' + value.value) for key,value in checksums.items() }