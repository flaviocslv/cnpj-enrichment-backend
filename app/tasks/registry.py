import uuid
from typing import Dict

# Armazena status das tarefas
task_registry: Dict[str, Dict] = {}

def create_task_entry() -> str:
    token = str(uuid.uuid4())
    task_registry[token] = {
        "status": "processing",
        "progress": 0,
        "error": None,
        "file": None
    }
    return token

def update_task(token: str, **kwargs):
    if token in task_registry:
        task_registry[token].update(kwargs)

def get_task_status(token: str):
    return task_registry.get(token, {"status": "not_found"})
