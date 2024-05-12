from fastapi import FastAPI, Request, HTTPException, File, UploadFile, status
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse,RedirectResponse,JSONResponse,FileResponse,StreamingResponse
from starlette.requests import ClientDisconnect
from urllib.parse import unquote
import streaming_form_data
from streaming_form_data import StreamingFormDataParser
from streaming_form_data.targets import FileTarget, ValueTarget
from fastapi.templating import Jinja2Templates
from uuid_utils import uuid7
from utils import split_path,safe_path

from typing import List
from os import getenv,walk,listdir,makedirs
from os.path import exists,join
import logging
from json import dumps,loads
import shutil

DATA_DIR=getenv('DATA_DIR', '/data')
LOG_LEVEL=getenv('LOG_LEVEL', 'WARNING')
PREFIX=getenv('PREFIX', None)
logging.basicConfig(level=logging.getLevelName(LOG_LEVEL))

app = FastAPI(root_path=PREFIX)
static = StaticFiles(directory="static")
templates = Jinja2Templates(directory="templates")

def _resolve(instance_id, filename=None):
    if filename and not filename.startswith('_'):
        filename = f'data/{filename}'

    return join(DATA_DIR, split_path(instance_id.replace('-', ''))) + (('/' + filename) if filename else '')

def _meta(instance_id):
    with open(_resolve(instance_id, '_meta.json'), 'r') as f:
        return loads(f.read())
                         
def _create_new_instance(**meta):
    instance_id = str(uuid7())
    path = _resolve(instance_id)

    if not exists(path):
        #makedirs(path)
        makedirs(join(path, 'data'))
    
    with open(join(path, '_meta.json'), 'w') as f:
        f.write(dumps(
            {
                **{ "urn": f'urn:uuid:{instance_id}', "version": str(uuid7()), "status": "open" },
                **meta
            },
            indent=4))
    
    # TODO: lock _instances.txt
    with open(join(DATA_DIR, '_instances.txt'), 'a') as f:
        print(instance_id, file=f)

    return instance_id


def _list_instances():
    path = join(DATA_DIR, '_instances.txt')

    if not exists(path):
        return []
    else:
        with open(join(DATA_DIR, '_instances.txt'), 'r') as f:
            return  [ x for x in f.readlines() if x.strip() ]


def _list_files(instance_id):
    files = [ x for x in listdir(_resolve(instance_id)) if x != 'data' ]
    files += [ x for x in listdir(join(_resolve(instance_id), 'data')) ]
             
    return files


def _touch(instance_id):
    with open(_resolve(instance_id, '_meta.json'), 'r') as f:
        m = loads(f.read())
        m['version'] = str(uuid7())

    with open(_resolve(instance_id, '_meta.json'), 'w') as f:
        f.write(dumps(m, indent=4))


@app.post('/_newinstance', response_class=JSONResponse)
async def new_instance(request: Request):
    instance_id = _create_new_instance(**request.query_params)

    return RedirectResponse(f'/{instance_id}/', status_code=status.HTTP_303_SEE_OTHER)


@app.post('/{instance_id}/_add', response_class=JSONResponse)
async def add_files(instance_id: str, request: Request):
    if not exists(_resolve(instance_id)):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail='Instance not found')

    if _meta(instance_id)['status'] == 'finalized':
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail='Instance is finalized')

    # TODO: check version

    try:
        parser = StreamingFormDataParser(headers=request.headers)
        data = ValueTarget()
        parser.register('data', data)
        
        headers = dict(request.headers)
        filenames = []
        i =0
        logging.info(f"Headers: {headers}")

        while True:
            filename = headers.get(f'filename{i}', None)
            if filename is None:
                break

            filename = safe_path(unquote(filename))
            filenames.append(filename)
            filepath = _resolve(instance_id, filename)
            logging.info(f"Uploading {filename} to {filepath}")

            file_ = FileTarget(filepath)
            parser.register(f'file{i}', file_)
            i += 1
        
        # TODO: calculate checksum while streaming
        async for chunk in request.stream():
            #print('CHONK!')
            parser.data_received(chunk)
    except ClientDisconnect:
        logging.warning("Client Disconnected")
    except Exception as e:
        logging.exception(e)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            detail='There was an error uploading the file') 
    
    _touch(instance_id)

    return { "message": f"Successfuly uploaded {filenames}"}


@app.get("/_instances", response_class=JSONResponse)
async def instances():
    return _list_instances()


@app.get("/")
async def get_root(request: Request):
    return templates.TemplateResponse("index.html", { "request": request, "instances": _list_instances() })


@app.get("/{instance_id}/{filename}", response_class=FileResponse)
async def get_file(instance_id: str, filename: str):
    path = _resolve(instance_id, filename)

    if not exists(path):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail='File not found')

    return FileResponse(path)


@app.get("/{instance_id}/")
async def get_collection(instance_id: str, request: Request):
    if not exists(_resolve(instance_id)):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail='Instance not found')

    return templates.TemplateResponse(
                "instance.html",
                {
                    "request": request,
                    "instance_id": instance_id,
                    "meta": _meta(instance_id),
                    "files": sorted(_list_files(instance_id))
                })


@app.post("/{instance_id}/_finalize", response_class=JSONResponse)
async def close_instance(instance_id: str, request: Request):
    path = _resolve(instance_id, "_meta.json")

    if not exists(path):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail='Instance not found')

    with open(path, 'r') as f:
        meta = loads(f.read())

        if meta['status'] != 'open':
            return HTTPException(status_code=status.HTTP_409_CONFLICT, detail='Instance not open')

    with open(path, 'w') as f:
        meta['status'] = 'finalized'
        f.write(dumps(meta, indent=4))

    return { "message": "Instance finalized" }


@app.get('/ok')
async def ok():
    return "ok"
