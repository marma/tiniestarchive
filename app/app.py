from tempfile import gettempdir
from fastapi import FastAPI, Request, HTTPException, File, UploadFile, status
from fastapi.responses import RedirectResponse,JSONResponse,FileResponse,StreamingResponse,PlainTextResponse
from uuid_utils import uuid7
from uuid import UUID, uuid4
from tiniestarchive import FileArchive,FileInstance,FileResource

from typing import List
from os import getenv,walk,listdir,makedirs
from os.path import exists,join,dirname
import logging
from json import dumps,loads

ARCHIVE_DIR=getenv('DATA_DIR', '/data')
LOG_LEVEL=getenv('LOG_LEVEL', 'WARNING')
PREFIX=getenv('PREFIX', None)
logging.basicConfig(level=LOG_LEVEL)

app = FastAPI(root_path=PREFIX)
archive = FileArchive(ARCHIVE_DIR)

@app.get("/")
async def root():
    return archive.config

@app.get("/{resource_id}/", response_class=JSONResponse)
async def get_resource(resource_id : UUID):
    return archive.get(str(resource_id)).json()

@app.post("/{resource_id}/_add")
async def add(resource_id : UUID, files: List[UploadFile]):
    with archive.get(resource_id, mode='w') as r:
        with r.transaction() as t:
            for file in files:
                t.add(file.filename, data=file.file)

    return "OK"

@app.post("/{resource_id}/_update")
async def ingest(resource_id : UUID, file: UploadFile):
    with FileInstance.deserialize(file.file) as instance:
            with archive.get(str(resource_id), mode='w') as r:
                r.update(instance)

    return "OK"

@app.get("/{resource_id}/_serialize", response_class=StreamingResponse)
async def stream(resource_id : UUID):
    headers = { 'Content-Disposition': f'attachment; filename="{resource_id}.tar"' }

    return StreamingResponse(
            archive.get(str(resource_id)).serialize(as_iter=True),
            headers=headers,
            media_type='application/tar')

@app.get("/{resource_id}/{filename}", response_class=FileResponse)
async def get_file(resource_id : UUID, filename: str):
    return FileResponse(archive._resolve(resource_id, filename))

@app.post("/_ingest")
async def ingest(file: UploadFile):
    with FileResource.deserialize(file.file) as resource:
        archive.ingest(resource)

    return "OK"

@app.get("/_resources", response_class=PlainTextResponse)
async def resources(request: Request):
    # TODO implement paging
    def i(start=0, max=None):
        for r in archive:
            yield r + '\n'

    return StreamingResponse(i, media_type='text/plain')

@app.get("/_events", response_class=JSONResponse)
async def events(request: Request):
    start = request.query_params.get('start', None)
    max = request.query_params.get('max', None)

    return StreamingResponse(
            archive.events(
                start=start,
                max=max),
            media_type='text/jsonl')

@app.get('/ok')
async def ok():
    return "ok"

