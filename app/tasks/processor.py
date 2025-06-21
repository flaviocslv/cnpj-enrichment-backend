"""
Processador de tarefas assíncronas para enriquecimento de CNPJ
"""
import asyncio
import logging
from pathlib import Path
from fastapi import UploadFile

from app.tasks.registry import update_task
from app.services import handle_upload

logger = logging.getLogger(__name__)

async def run_background_task(file: UploadFile, token: str):
    """
    Executa o processamento em background com atualizações de status
    """
    try:
        logger.info(f"Iniciando processamento background para token: {token}")
        
        # Atualizar status inicial
        update_task(token, status="processing", progress=0)
        
        # Processar arquivo
        output_path = await handle_upload(file, token)
        
        # Verificar se o arquivo foi criado
        if not output_path.exists():
            raise FileNotFoundError(f"Arquivo de saída não foi criado: {output_path}")
        
        # Atualizar status final
        update_task(token, 
                   status="completed", 
                   progress=100, 
                   file=output_path.name)
        
        logger.info(f"Processamento concluído para token: {token}")
        
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Erro no processamento para token {token}: {error_msg}")
        
        update_task(token, 
                   status="failed", 
                   progress=0,
                   error=error_msg)
        
        raise e

async def cleanup_old_files(max_age_hours: int = 24):
    """
    Remove arquivos antigos da pasta files
    """
    try:
        files_dir = Path("files")
        if not files_dir.exists():
            return
        
        import time
        current_time = time.time()
        max_age_seconds = max_age_hours * 3600
        
        for file_path in files_dir.glob("*.xlsx"):
            file_age = current_time - file_path.stat().st_mtime
            if file_age > max_age_seconds:
                file_path.unlink()
                logger.info(f"Arquivo removido: {file_path}")
                
    except Exception as e:
        logger.error(f"Erro na limpeza de arquivos: {e}")

def start_cleanup_scheduler():
    """
    Inicia agendador para limpeza automática de arquivos
    """
    async def cleanup_loop():
        while True:
            await cleanup_old_files()
            await asyncio.sleep(3600)  # Verificar a cada hora
    
    asyncio.create_task(cleanup_loop())
