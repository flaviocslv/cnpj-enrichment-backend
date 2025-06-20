import uuid
from typing import Dict, Optional

# Registro de tarefas em memória
task_registry: Dict[str, Dict] = {}

def create_task_entry() -> str:
    token = str(uuid.uuid4())
    task_registry[token] = {
        "status": "pending",
        "progress": 0,
        "error": None,
        "file": None
    }
    return token

def update_task(token: str, **kwargs):
    if token in task_registry:
        task_registry[token].update(kwargs)

def get_task_status(token: str) -> Dict:
    return task_registry.get(token, {"status": "not_found"})

def set_status(token: str, status: str):
    update_task(token, status=status)

def set_result_path(token: str, filename: Optional[str]):
    update_task(token, file=filename)
