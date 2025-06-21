from fastapi import APIRouter, UploadFile, File, BackgroundTasks
from fastapi.responses import FileResponse, JSONResponse
from app.services import process_excel, start_background_process
from app.tasks.registry import create_task_entry, get_task_status
from functools import lru_cache

@lru_cache(maxsize=1000)
def get_cached_cnpj_data(cnpj: str):
    # Implementar cache em Redis ou memória
    pass

router = APIRouter()

# === MODO 1: PROCESSAMENTO IMEDIATO ===
@router.post("/upload")
async def upload_excel(file: UploadFile = File(...)):
    if file.size > MAX_FILE_SIZE_MB * 1024 * 1024:
        raise HTTPException(400, "Arquivo muito grande")
    # Verificar se é Excel
    if not file.filename.endswith(('.xlsx', '.xls')):
        raise HTTPException(400, "Formato de arquivo inválido")
    output_path = await process_excel(file)
    return {"download_url": f"/download/{output_path.name}"}
    
    

@router.get("/download/{filename}")
def download(filename: str):
    file_path = f"./files/{filename}"
    return FileResponse(file_path, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", filename=filename)

# === MODO 2: ASSÍNCRONO COM TOKEN ===
@router.post("/start")
async def start_process(file: UploadFile = File(...), background_tasks: BackgroundTasks = None):
    token = create_task_entry()
    background_tasks.add_task(start_background_process, file, token)
    return {"token": token}

@router.get("/status/{token}")
def check_status(token: str):
    status_data = get_task_status(token)
    if status_data["status"] == "completed":
        return {
            "status": status_data["status"],
            "download_url": f"/download/{status_data['file']}"
        }
    return status_data
