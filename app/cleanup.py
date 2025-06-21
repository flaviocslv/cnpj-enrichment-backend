import asyncio
import logging
import time
from pathlib import Path
from app.config import FILES_DIR, MAX_FILE_AGE_HOURS

logger = logging.getLogger(__name__)

async def cleanup_old_files():
    """
    Remove arquivos antigos da pasta files
    """
    try:
        files_dir = Path(FILES_DIR)
        if not files_dir.exists():
            return
        
        current_time = time.time()
        max_age_seconds = MAX_FILE_AGE_HOURS * 3600
        removed_count = 0
        
        for file_path in files_dir.glob("*.xlsx"):
            try:
                file_age = current_time - file_path.stat().st_mtime
                if file_age > max_age_seconds:
                    file_path.unlink()
                    removed_count += 1
                    logger.info(f"Arquivo removido: {file_path.name}")
            except Exception as e:
                logger.error(f"Erro ao remover arquivo {file_path}: {e}")
        
        if removed_count > 0:
            logger.info(f"Limpeza concluída: {removed_count} arquivos removidos")
                
    except Exception as e:
        logger.error(f"Erro na limpeza de arquivos: {e}")

async def start_cleanup_scheduler():
    """
    Inicia agendador para limpeza automática de arquivos a cada hora
    """
    logger.info("Iniciando agendador de limpeza de arquivos")
    
    while True:
        try:
            await cleanup_old_files()
            await asyncio.sleep(3600)  # Aguardar 1 hora
        except Exception as e:
            logger.error(f"Erro no agendador de limpeza: {e}")
            await asyncio.sleep(600)  # Em caso de erro, tentar novamente em 10 minutos
