import os

# API Configuration
API_URL = os.getenv("CNPJA_API_URL", "https://open.cnpja.com/office")
DELAY = float(os.getenv("API_DELAY", "2.0"))  # segundos entre requisições

# Rate Limiting
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
BACKOFF_FACTOR = float(os.getenv("BACKOFF_FACTOR", "2.0"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))

# File Management
FILES_DIR = os.getenv("FILES_DIR", "files")
MAX_FILE_AGE_HOURS = int(os.getenv("MAX_FILE_AGE_HOURS", "24"))
MAX_FILE_SIZE_MB = int(os.getenv("MAX_FILE_SIZE_MB", "10"))

# Processing Limits
MAX_CNPJS_SYNC = int(os.getenv("MAX_CNPJS_SYNC", "50"))  # Limite para processamento síncrono
MAX_CNPJS_TOTAL = int(os.getenv("MAX_CNPJS_TOTAL", "1000"))  # Limite total por arquivo

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# Excel Configuration
EXCEL_ENGINE = "openpyxl"
MAX_SOCIOS = 5
MAX_ESTABELECIMENTOS = 5

# Environment
ENVIRONMENT = os.getenv("ENVIRONMENT", "development")
DEBUG = ENVIRONMENT == "development"
