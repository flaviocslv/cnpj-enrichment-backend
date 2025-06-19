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

    # Definição mínima de colunas fixas
    base_cols = ["RazaoSocial", "Status", "DataStatus", "NaturezaJuridica", "Porte",
        "CapitalSocial", "Telefone", "Email", "AtividadePrincipal",
        "CNAEs", "Endereco", "Municipio", "UF", "CEP", "Numero", "Complemento",
        "SimplesOptante", "SimplesSince", "MEIOptante", "MEISince",
        "Latitude", "Longitude"
    ]
    for col in base_cols:
        df[col] = ""

    # Tratamento de colunas dinâmicas (IE, sócios, estabelecimentos)
    max_socios = 5  # ajustável conforme necessidade
    max_estabs = 5
    for uf in ["AC","AL","AP","AM","BA","CE","DF","ES","GO","MA","MT","MS","MG","PA","PB","PR","PE","PI","RJ","RN","RS","RO","RR","SC","SP","SE","TO"]:
        df[f"IE_{uf}"] = ""
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

        # IE por UF
        for reg in data.get("registrations", []):
            uf = reg.get("state")
            df.at[idx, f"IE_{uf}"] = reg.get("number", "")

        # SUFRAMA - pode adicionar colunas semelhantes se necessário

        # Sócios
        for i, m in enumerate(comp.get("members", [])[:max_socios]):
            df.at[idx, f"Socio_{i+1}_Nome"] = m.get("person", {}).get("name", "")
            df.at[idx, f"Socio_{i+1}_Tipo"] = m.get("person", {}).get("type", "")
            df.at[idx, f"Socio_{i+1}_TaxId"] = m.get("person", {}).get("taxId", "")
            df.at[idx, f"Socio_{i+1}_Role"] = m.get("role", {}).get("text", "")

        # Estabelecimentos (se retornado)
        for i, est in enumerate(data.get("establishments", [])[:max_estabs]):
            df.at[idx, f"Estab_{i+1}_CNPJ"] = est.get("cnpj", "")
            df.at[idx, f"Estab_{i+1}_Tipo"] = est.get("tipo", "")
            df.at[idx, f"Estab_{i+1}_Endereco"] = est.get("logradouro", "")
            df.at[idx, f"Estab_{i+1}_UF"] = est.get("estado", "")

        time.sleep(DELAY)

    Path("files").mkdir(parents=True, exist_ok=True)
    out = Path("files") / f"{uuid.uuid4()}.xlsx"
    df.to_excel(out, index=False)
    return out
