from fastapi import FastAPI
from app.routes import router

app = FastAPI(title="CNPJ Enrichment API")

app.include_router(router)