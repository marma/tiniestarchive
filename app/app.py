from fastapi import FastAPI, Request, HTTPException, File, UploadFile, status
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse,RedirectResponse,JSONResponse,FileResponse,StreamingResponse
from starlette.requests import ClientDisconnect
from fastapi.templating import Jinja2Templates
from uuid_utils import uuid7
from utils import split_path,safe_path,save_to_tmp

from typing import List
from os import getenv,walk,listdir,makedirs
from os.path import exists,join,dirname
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
        makedirs(join(path, 'data'))
    
    with open(join(path, '_meta.json'), 'w') as f:
        f.write(dumps(
            {
                **{ "urn": f'urn:uuid:{instance_id}', "version": str(uuid7()), "status": "open" },
                **meta
            },
            indent=4))
        
    with open(join(path, '_files.json'), 'w') as f:
        f.write(dumps([], indent=4))
    
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

    # TODO: lock instance
    # TODO: check version

    tmpdir = join(_resolve(instance_id), f'.tmp-{str(uuid7())}')
    touched=False
    try:
        filenames, checksums = await save_to_tmp(instance_id, tmpdir, request)

        # TODO: validate file checksums against supplied checksums

        # check if all files exists in tmpdir
        for filename in filenames:
            if not exists(join(tmpdir, filename)):
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail='File {filename} not found in request')
            
        # move files to final location
        for filename in filenames:
            target_path = _resolve(instance_id, filename)

            if not exists(dirname(target_path)):
                makedirs(dirname(target_path))

            shutil.move(join(tmpdir, filename), target_path)
            touched=True

        # TODO: (optionally) validate files in final location
    except ClientDisconnect:
        logging.warning("Client Disconnected")
    except HTTPException as e:
        logging.warning(e)
        raise e
    except Exception as e:
        logging.exception(e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            detail='There was an error uploading the file(s)')
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

        if touched:
            _touch(instance_id)

    logging.info(f"filenames: {filenames}, checksums: {checksums}")

    return { "files": checksums }


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

    if request.headers.get('Accept') == 'application/json':
        return { **_meta(instance_id), "files": _list_files(instance_id) }

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
