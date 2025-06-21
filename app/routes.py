from fastapi import APIRouter, UploadFile, File, BackgroundTasks, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from pathlib import Path
import os

from app.services import process_excel_sync, start_background_process
from app.tasks.registry import create_task_entry, get_task_status
from app.config import MAX_FILE_SIZE_MB, FILES_DIR

router = APIRouter()

# Garantir que o diretório de arquivos existe
Path(FILES_DIR).mkdir(parents=True, exist_ok=True)

# === MODO 1: PROCESSAMENTO IMEDIATO (SÍNCRONO) ===
@router.post("/upload")
async def upload_excel(file: UploadFile = File(...)):
    """
    Upload e processamento imediato de arquivo Excel
    Retorna URL para download quando concluído
    """
    # Validações
    if not file.filename:
        raise HTTPException(status_code=400, detail="Nome do arquivo não fornecido")
    
    if file.size and file.size > MAX_FILE_SIZE_MB * 1024 * 1024:
        raise HTTPException(status_code=400, detail=f"Arquivo muito grande. Máximo: {MAX_FILE_SIZE_MB}MB")
    
    if not file.filename.lower().endswith(('.xlsx', '.xls')):
        raise HTTPException(status_code=400, detail="Formato de arquivo inválido. Use .xlsx ou .xls")
    
    try:
        # Processar arquivo
        output_path = await process_excel_sync(file)
        
        return {
            "status": "success",
            "message": "Arquivo processado com sucesso",
            "download_url": f"/download/{output_path.name}"
        }
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro no processamento: {str(e)}")

@router.get("/download/{filename}")
def download_file(filename: str):
    """
    Download do arquivo processado
    """
    file_path = Path(FILES_DIR) / filename
    
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="Arquivo não encontrado")
    
    return FileResponse(
        path=file_path,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=filename
    )

# === MODO 2: PROCESSAMENTO ASSÍNCRONO COM TOKEN ===
@router.post("/start")
async def start_async_process(file: UploadFile = File(...), background_tasks: BackgroundTasks = None):
    """
    Inicia processamento assíncrono
    Retorna token para acompanhar progresso
    """
    # Validações (mesmas do endpoint síncrono)
    if not file.filename:
        raise HTTPException(status_code=400, detail="Nome do arquivo não fornecido")
    
    if file.size and file.size > MAX_FILE_SIZE_MB * 1024 * 1024:
        raise HTTPException(status_code=400, detail=f"Arquivo muito grande. Máximo: {MAX_FILE_SIZE_MB}MB")
    
    if not file.filename.lower().endswith(('.xlsx', '.xls')):
        raise HTTPException(status_code=400, detail="Formato de arquivo inválido. Use .xlsx ou .xls")
    
    try:
        # Criar entrada de tarefa
        token = create_task_entry()
        
        # Adicionar tarefa em background
        background_tasks.add_task(start_background_process, file, token)
        
        return {
            "status": "started",
            "token": token,
            "message": "Processamento iniciado. Use o token para verificar o progresso."
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao iniciar processamento: {str(e)}")

@router.get("/status/{token}")
def check_processing_status(token: str):
    """
    Verifica status do processamento assíncrono
    """
    status_data = get_task_status(token)
    
    if status_data["status"] == "not_found":
        raise HTTPException(status_code=404, detail="Token não encontrado")
    
    if status_data["status"] == "completed":
        return {
            "status": status_data["status"],
            "progress": status_data["progress"],
            "download_url": f"/download/{status_data['file']}"
        }
    
    return {
        "status": status_data["status"],
        "progress": status_data.get("progress", 0),
        "error": status_data.get("error")
    }

# === ENDPOINTS AUXILIARES ===
@router.get("/health")
def health_check():
    """
    Endpoint para verificação de saúde da API
    """
    return {"status": "healthy", "message": "API funcionando corretamente"}

@router.get("/")
def root():
    """
    Endpoint raiz com informações da API
    """
    return {
        "message": "CNPJ Enrichment API",
        "version": "1.0",
        "endpoints": {
            "upload": "POST /upload - Processamento síncrono",
            "start": "POST /start - Iniciar processamento assíncrono", 
            "status": "GET /status/{token} - Verificar status",
            "download": "GET /download/{filename} - Download do arquivo"
        }
    }
