from fastapi import APIRouter, UploadFile, File
from fastapi.responses import FileResponse, JSONResponse
import asyncio
import uuid
from pathlib import Path

from app.tasks.registry import create_task_entry, get_task_status
from app.tasks.processor import run_background_task

router = APIRouter()

@router.post("/upload_async")
async def upload_excel_async(file: UploadFile = File(...)):
    token = create_task_entry()
    asyncio.create_task(run_background_task(file, token))
    return {"token": token, "status_url": f"/status/{token}", "download_url": f"/download/{token}.xlsx"}

@router.get("/status/{token}")
async def get_status(token: str):
    return get_task_status(token)

@router.get("/download/{filename}")
async def download(filename: str):
    file_path = Path("files") / filename
    if file_path.exists():
        return FileResponse(
            path=file_path,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename=filename
        )
    return JSONResponse(status_code=404, content={"error": "Arquivo não encontrado"})
