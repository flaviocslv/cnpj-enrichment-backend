import pandas as pd
import requests
import time
import uuid
from pathlib import Path
from app.utils import sanitize_cnpj
from app.config import API_URL, DELAY

async def process_excel(uploaded_file):
    df = pd.read_excel(uploaded_file.file)
    df["CNPJ_Sanitizado"] = df["CNPJ"].apply(sanitize_cnpj)

    df["AtividadePrincipal"] = ""
    df["Porte"] = ""
    df["Telefone"] = ""

    for i, row in df.iterrows():
        cnpj = row["CNPJ_Sanitizado"]
        response = requests.get(f"{API_URL}/{cnpj}")
        if response.status_code == 200:
            data = response.json()
            df.at[i, "AtividadePrincipal"] = data.get("main_activity", {}).get("text", "")
            df.at[i, "Porte"] = data.get("company_size", "")
            df.at[i, "Telefone"] = data.get("phone", "")
        time.sleep(DELAY)

    output_filename = f"{uuid.uuid4()}.xlsx"
    output_path = Path("files") / output_filename
    df.to_excel(output_path, index=False)
    return output_path