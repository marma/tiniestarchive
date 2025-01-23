from fastapi import FastAPI, Request, HTTPException, File, UploadFile, status
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse,RedirectResponse,JSONResponse,FileResponse,StreamingResponse
from starlette.requests import ClientDisconnect
from fastapi.templating import Jinja2Templates
from uuid_utils import uuid7
from tiniestarchive import FileArchive
from tiniestarchive.utils import split_path,safe_path,save_to_tmp
from itertools import islice

from typing import List
from os import getenv,walk,listdir,makedirs
from os.path import exists,join,dirname
import logging
from json import dumps,loads
import shutil

ARCHIVE_DIR=getenv('DATA_DIR', '/data')
LOG_LEVEL=getenv('LOG_LEVEL', 'WARNING')
PREFIX=getenv('PREFIX', None)
logging.basicConfig(level=LOG_LEVEL)

app = FastAPI(root_path=PREFIX)
static = StaticFiles(directory="static")
templates = Jinja2Templates(directory="templates")
archive = FileArchive(ARCHIVE_DIR)

def _list_resources(max=None):
    return islice(archive, max)

def _get_resource(resource_id):
    return archive.get(resource_id)

def _get_json(resource_id):
    return archive.json(resource_id)

def _events(start=None):
    return [ x for x in archive.events(start=start) ]

@app.post('/_ingest')
async def ingest():
    archive.ingest(request.stream())

    i = archive.new(mode='w')

    print(i)

    return RedirectResponse(f'/{i.instance_id}/', status_code=status.HTTP_302_FOUND)

@app.get("/_resources", response_class=JSONResponse)
async def resources():
    return _list_resources()

@app.get("/")
async def get_root(request: Request):
    return templates.TemplateResponse("index.html", { "request": request, "instances": _list_resources(max=100) })

@app.get("/{instance_id}/{filename}", response_class=FileResponse)
async def get_file(instance_id: str, filename: str):
    if instance_id not in archive:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail='File not found')

    return FileResponse(archive.resolve(instance_id, filename))

@app.post("/{instance_id}/_add", response_class=JSONResponse)
def add_files(instance_id, files: List[UploadFile]):
    # TODO: use streaming and validate checksums
    with archive.get(instance_id, mode='a') as i:
        for file in files:
            i.add(file.filename, fobj=file.file)

    return RedirectResponse(f'/{i.instance_id}/', status_code=status.HTTP_302_FOUND)

@app.get("/{instance_id}/")
async def get_collection(instance_id: str, request: Request):
    if instance_id not in archive:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail='Instance not found')

    if request.headers.get('Accept') == 'application/json':
        return _meta(instance_id)

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
    if instance_id not in archive:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail='Instance not found')

    with archive.get(instance_id, mode='a') as i:
        if i.config['status'] != 'open':
            return HTTPException(status_code=status.HTTP_409_CONFLICT, detail='Instance not open')

        i.finalize()

    return { "message": "Instance finalized" }

@app.post("/_events", response_class=JSONResponse)
async def get_events(request: Request):
    ...

@app.get('/ok')
async def ok():
    return "ok"

@app.post('/{instance_id}/_add', response_class=JSONResponse)
async def add_files(instance_id: str, request: Request):
    if not instance_id in archive:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail='Instance not found')

    if _meta(instance_id)['status'] == 'finalized':
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail='Instance is finalized')

    # TODO: lock instance
    # TODO: check version

    tmpdir = join(_resolve(instance_id), f'.tmp-{str(uuid7())}')
    touched=False
    try:
        filenames, checksums = await save_to_tmp(instance_id, tmpdir, request)

        # check if all files exists in tmpdir and validate
        for filename in filenames:
            if not exists(join(tmpdir, filename)):
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail='File {filename} not found in request')
            
            # TODO: validate file checksum against supplied checksum

        # move files to final location
        for filename in filenames:
            target_path = _resolve(instance_id, filename)

            if not exists(dirname(target_path)):
                makedirs(dirname(target_path))

            # move within the same filesystem assumed to be atomic
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

