from fastapi import APIRouter, UploadFile, File
from fastapi.responses import FileResponse
from app.services import process_excel

router = APIRouter()

@router.post("/upload")
async def upload_excel(file: UploadFile = File(...)):
    output_path = await process_excel(file)
    return {"download_url": f"/download/{output_path.name}"}

@router.get("/download/{filename}")
def download(filename: str):
    file_path = f"./files/{filename}"
    return FileResponse(file_path, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", filename=filename)