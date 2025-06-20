import pandas as pd
import requests
import time
import uuid
import json
from pathlib import Path
from app.utils import sanitize_cnpj
from app.config import API_URL, DELAY
from app.tasks.registry import set_status, set_result_path

# Função reutilizável para processar o Excel e retornar caminho do arquivo gerado
def enrich_excel_from_file(file_obj) -> Path:
    df = pd.read_excel(file_obj)
    df["CNPJ_Sanitizado"] = df["CNPJ"].apply(sanitize_cnpj)

    # Colunas fixas
    base_cols = [
        "RazaoSocial", "Status", "DataStatus", "NaturezaJuridica", "Porte",
        "CapitalSocial", "Telefone", "Email", "AtividadePrincipal",
        "CNAEs", "Endereco", "Municipio", "UF", "CEP", "Numero", "Complemento",
        "SimplesOptante", "SimplesSince", "MEIOptante", "MEISince",
        "Latitude", "Longitude", "InscricoesEstaduais"
    ]
    for col in base_cols:
        df[col] = ""

    # Dinâmico
    max_socios = 5
    max_estabs = 5

    for i in range(1, max_socios+1):
        df[f"Socio_{i}_Nome"] = ""
        df[f"Socio_{i}_Tipo"] = ""
        df[f"Socio_{i}_TaxId"] = ""
        df[f"Socio_{i}_Role"] = ""

    for i in range(1, max_estabs+1):
        df[f"Estab_{i}_CNPJ"] = ""
        df[f"Estab_{i}_Tipo"] = ""
        df[f"Estab_{i}_Endereco"] = ""
        df[f"Estab_{i}_UF"] = ""

    for idx, row in df.iterrows():
        cnpj = row["CNPJ_Sanitizado"]
        try:
            resp = requests.get(f"{API_URL}/{cnpj}?simples=true&registrations=BR&suframa=true&geocoding=true")
            if resp.status_code != 200:
                continue
            data = resp.json()
            comp = data.get("company", {})
            addr = data.get("address", {})

            df.at[idx, "RazaoSocial"] = comp.get("name", "")
            df.at[idx, "Status"] = data.get("status", {}).get("text", "")
            df.at[idx, "DataStatus"] = data.get("statusDate", "")
            df.at[idx, "NaturezaJuridica"] = comp.get("nature", {}).get("text", "")
            df.at[idx, "Porte"] = comp.get("size", {}).get("text", "")
            df.at[idx, "CapitalSocial"] = comp.get("equity", "")
            phones = data.get("phones", [])
            df.at[idx, "Telefone"] = phones[0].get("number", "") if phones else ""
            emails = data.get("emails", [])
            df.at[idx, "Email"] = emails[0].get("address", "") if emails else ""
            df.at[idx, "AtividadePrincipal"] = data.get("mainActivity", {}).get("text", "")
            df.at[idx, "CNAEs"] = "; ".join([a.get("text","") for a in data.get("sideActivities", [])])
            df.at[idx, "Endereco"] = addr.get("street", "")
            df.at[idx, "Municipio"] = addr.get("city", "")
            df.at[idx, "UF"] = addr.get("state", "")
            df.at[idx, "CEP"] = addr.get("zip", "")
            df.at[idx, "Numero"] = addr.get("number", "")
            df.at[idx, "Complemento"] = addr.get("details", "")
            df.at[idx, "Latitude"] = addr.get("latitude", "")
            df.at[idx, "Longitude"] = addr.get("longitude", "")

            simples = comp.get("simples", {})
            df.at[idx, "SimplesOptante"] = "Sim" if simples.get("optant") else "Não"
            df.at[idx, "SimplesSince"] = simples.get("since", "")
            simei = comp.get("simei", {})
            df.at[idx, "MEIOptante"] = "Sim" if simei.get("optant") else "Não"
            df.at[idx, "MEISince"] = simei.get("since", "")

            ies_dict = {
                reg.get("state"): reg.get("number")
                for reg in data.get("registrations", [])
                if reg.get("state") and reg.get("number")
            }
            df.at[idx, "InscricoesEstaduais"] = json.dumps(ies_dict, ensure_ascii=False)

            for i, m in enumerate(comp.get("members", [])[:max_socios]):
                df.at[idx, f"Socio_{i+1}_Nome"] = m.get("person", {}).get("name", "")
                df.at[idx, f"Socio_{i+1}_Tipo"] = m.get("person", {}).get("type", "")
                df.at[idx, f"Socio_{i+1}_TaxId"] = m.get("person", {}).get("taxId", "")
                df.at[idx, f"Socio_{i+1}_Role"] = m.get("role", {}).get("text", "")

            for i, est in enumerate(data.get("establishments", [])[:max_estabs]):
                df.at[idx, f"Estab_{i+1}_CNPJ"] = est.get("cnpj", "")
                df.at[idx, f"Estab_{i+1}_Tipo"] = est.get("tipo", "")
                df.at[idx, f"Estab_{i+1}_Endereco"] = est.get("logradouro", "")
                df.at[idx, f"Estab_{i+1}_UF"] = est.get("estado", "")

            time.sleep(DELAY)
        except Exception as e:
            continue  # ignora falhas individuais

    Path("files").mkdir(parents=True, exist_ok=True)
    out = Path("files") / f"{uuid.uuid4()}.xlsx"
    df.to_excel(out, index=False)
    return out

# Compatível com o endpoint atual (síncrono)
async def process_excel(uploaded_file):
    return enrich_excel_from_file(uploaded_file.file)

# Compatível com o novo fluxo assíncrono
async def start_background_process(uploaded_file, token: str):
    try:
        set_status(token, "processing")
        output_path = enrich_excel_from_file(uploaded_file.file)
        set_result_path(token, output_path.name)
        set_status(token, "completed")
    except Exception as e:
        set_status(token, "failed")
        raise e
