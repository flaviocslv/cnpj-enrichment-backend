from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pathlib import Path
import logging
import asyncio

from app.routes import router
from app.config import FILES_DIR, DEBUG, LOG_LEVEL
from app.cleanup import start_cleanup_scheduler

# Configurar logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper()),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

# Criar aplicação FastAPI
app = FastAPI(
    title="CNPJ Enrichment API",
    description="API para enriquecer planilhas Excel com dados de CNPJ usando a API pública cnpja.com",
    version="1.0.0",
    debug=DEBUG
)

# Configurar CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Em produção, especificar domínios
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Garantir que diretório de arquivos existe
Path(FILES_DIR).mkdir(parents=True, exist_ok=True)

# Incluir rotas
app.include_router(router)

@app.on_event("startup")
async def startup_event():
    """Evento executado na inicialização da aplicação"""
    logger.info("🚀 CNPJ Enrichment API iniciada")
    logger.info(f"📁 Diretório de arquivos: {FILES_DIR}")
    logger.info(f"🔧 Modo debug: {DEBUG}")
    
    # Iniciar agendador de limpeza em background
    try:
        asyncio.create_task(start_cleanup_scheduler())
        logger.info("🧹 Agendador de limpeza iniciado")
    except Exception as e:
        logger.error(f"Erro ao iniciar agendador de limpeza: {e}")

@app.on_event("shutdown")
async def shutdown_event():
    """Evento executado no encerramento da aplicação"""
    logger.info("⏹️ CNPJ Enrichment API encerrada")

# Middleware para log de requisições
@app.middleware("http")
async def log_requests(request, call_next):
    start_time = time.time()
    
    try:
        response = await call_next(request)
        process_time = time.time() - start_time
        
        logger.info(
            f"{request.method} {request.url.path} - "
            f"Status: {response.status_code} - "
            f"Time: {process_time:.3f}s"
        )
        
        return response
    except Exception as e:
        process_time = time.time() - start_time
        logger.error(
            f"{request.method} {request.url.path} - "
            f"Error: {str(e)} - "
            f"Time: {process_time:.3f}s"
        )
        raise

if __name__ == "__main__":
    import uvicorn
    import time
    
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=DEBUG
    )
