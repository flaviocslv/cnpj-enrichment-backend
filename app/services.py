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

    # Adiciona novas colunas no DataFrame
    cols = [
        "Razão Social", "Situação", "Data Situação", "Natureza Jurídica", "Porte",
        "Telefone", "Email", "Atividade Principal", "CNAEs Secundários",
        "Endereço", "Município", "UF", "CEP", "Número", "Complemento",
        "Simples Optante", "Simples Desde", "MEI Optante", "MEI Desde",
        "IE_SP", "IE_MS", "IE_AM", "Geolocalização", "Sócios"
    ]
    for col in cols:
        df[col] = ""

    for i, row in df.iterrows():
        cnpj = row["CNPJ_Sanitizado"]
        try:
            response = requests.get(f"{API_URL}/{cnpj}")
            if response.status_code != 200:
                print(f"Erro ao consultar CNPJ {cnpj}: {response.status_code}")
                continue

            data = response.json()

            company = data.get("company", {})
            address = data.get("address", {})
            simples = company.get("simples", {})
            simei = company.get("simei", {})

            df.at[i, "Razão Social"] = company.get("name", "")
            df.at[i, "Situação"] = data.get("status", {}).get("text", "")
            df.at[i, "Data Situação"] = data.get("statusDate", "")
            df.at[i, "Natureza Jurídica"] = company.get("nature", {}).get("text", "")
            df.at[i, "Porte"] = company.get("size", {}).get("text", "")

            # Telefones
            phones = data.get("phones", [])
            if phones and isinstance(phones, list):
                df.at[i, "Telefone"] = phones[0].get("number", "")

            # Emails
            emails = data.get("emails", [])
            if emails and isinstance(emails, list):
                df.at[i, "Email"] = emails[0].get("address", "")

            # Atividades
            df.at[i, "Atividade Principal"] = data.get("mainActivity", {}).get("text", "")
            df.at[i, "CNAEs Secundários"] = "; ".join([act.get("text", "") for act in data.get("sideActivities", [])])

            # Endereço
            df.at[i, "Endereço"] = address.get("street", "")
            df.at[i, "Número"] = address.get("number", "")
            df.at[i, "Complemento"] = address.get("details", "")
            df.at[i, "Município"] = address.get("city", "")
            df.at[i, "UF"] = address.get("state", "")
            df.at[i, "CEP"] = address.get("zip", "")
            df.at[i, "Geolocalização"] = f"{address.get('latitude', '')}, {address.get('longitude', '')}"

            # Simples Nacional / MEI
            df.at[i, "Simples Optante"] = "Sim" if simples.get("optant") else "Não"
            df.at[i, "Simples Desde"] = simples.get("since", "")
            df.at[i, "MEI Optante"] = "Sim" if simei.get("optant") else "Não"
            df.at[i, "MEI Desde"] = simei.get("since", "")

            # Inscrições estaduais por UF
            for reg in data.get("registrations", []):
                if reg.get("state") == "SP":
                    df.at[i, "IE_SP"] = reg.get("number", "")
                elif reg.get("state") == "MS":
                    df.at[i, "IE_MS"] = reg.get("number", "")
                elif reg.get("state") == "AM":
                    df.at[i, "IE_AM"] = reg.get("number", "")

            # Sócios
            membros = company.get("members", [])
            nomes_socios = [m.get("person", {}).get("name", "") for m in membros if "person" in m]
            df.at[i, "Sócios"] = "; ".join(nomes_socios)

        except Exception as e:
            print(f"Erro ao processar CNPJ {cnpj}: {e}")

        time.sleep(DELAY)

    # Garante que a pasta 'files' existe
    output_dir = Path("files")
    output_dir.mkdir(parents=True, exist_ok=True)

    output_filename = f"{uuid.uuid4()}.xlsx"
    output_path = output_dir / output_filename
    df.to_excel(output_path, index=False)

    return output_path
