import pandas as pd
import requests
import time
import uuid
import json
import io
import logging
from pathlib import Path
from typing import Optional, Dict, Any
from fastapi import UploadFile

from app.utils import sanitize_cnpj
from app.config import API_URL, DELAY, FILES_DIR, REQUEST_TIMEOUT, MAX_RETRIES, BACKOFF_FACTOR
from app.tasks.registry import update_task

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CNPJEnricher:
    def __init__(self, api_url: str = API_URL, delay: float = DELAY):
        self.api_url = api_url
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'CNPJ-Enrichment-Tool/1.0',
            'Accept': 'application/json'
        })

    def fetch_cnpj_data(self, cnpj: str) -> Optional[Dict[Any, Any]]:
        try:
            url = f"{self.api_url}/{cnpj}?simples=true&registrations=BR&suframa=true&geocoding=true"
            for attempt in range(MAX_RETRIES):
                try:
                    response = self.session.get(url, timeout=REQUEST_TIMEOUT)
                    if response.status_code == 200:
                        return response.json()
                    elif response.status_code == 429:
                        wait_time = self.delay * (BACKOFF_FACTOR ** attempt)
                        logger.warning(f"Rate limit atingido para CNPJ {cnpj}. Aguardando {wait_time}s...")
                        time.sleep(wait_time)
                        continue
                    elif response.status_code == 404:
                        logger.info(f"CNPJ {cnpj} não encontrado")
                        return None
                    else:
                        logger.warning(f"Status {response.status_code} para CNPJ {cnpj}")
                        if attempt < MAX_RETRIES - 1:
                            time.sleep(self.delay * (attempt + 1))
                            continue
                        return None
                except requests.exceptions.RequestException as e:
                    if attempt == MAX_RETRIES - 1:
                        logger.error(f"Erro na requisição para CNPJ {cnpj}: {e}")
                        return None
                    wait_time = self.delay * (attempt + 1)
                    logger.warning(f"Tentativa {attempt + 1} falhou para CNPJ {cnpj}. Aguardando {wait_time}s...")
                    time.sleep(wait_time)
        except Exception as e:
            logger.error(f"Erro inesperado ao buscar CNPJ {cnpj}: {e}")
            return None
        return None

    def extract_data_from_response(self, data: Dict[Any, Any]) -> Dict[str, Any]:
        if not data:
            return {}
        comp = data.get("company", {})
        addr = data.get("address", {})
        extracted = {
            "RazaoSocial": comp.get("name", ""),
            "Status": data.get("status", {}).get("text", ""),
            "DataStatus": data.get("statusDate", ""),
            "NaturezaJuridica": comp.get("nature", {}).get("text", ""),
            "Porte": comp.get("size", {}).get("text", ""),
            "CapitalSocial": comp.get("equity", ""),
            "AtividadePrincipal": data.get("mainActivity", {}).get("text", ""),
            "CNAEs": "; ".join([a.get("text", "") for a in data.get("sideActivities", [])]),
            "Telefone": "",
            "Email": "",
            "Endereco": addr.get("street", ""),
            "Municipio": addr.get("city", ""),
            "UF": addr.get("state", ""),
            "CEP": addr.get("zip", ""),
            "Numero": addr.get("number", ""),
            "Complemento": addr.get("details", ""),
            "Latitude": addr.get("latitude", ""),
            "Longitude": addr.get("longitude", ""),
            "SimplesOptante": "",
            "SimplesSince": "",
            "MEIOptante": "",
            "MEISince": "",
            "InscricoesEstaduais": ""
        }
        phones = data.get("phones", [])
        if phones:
            extracted["Telefone"] = phones[0].get("number", "")
        emails = data.get("emails", [])
        if emails:
            extracted["Email"] = emails[0].get("address", "")
        simples = comp.get("simples", {})
        extracted["SimplesOptante"] = "Sim" if simples.get("optant") else "Não"
        extracted["SimplesSince"] = simples.get("since", "")
        simei = comp.get("simei", {})
        extracted["MEIOptante"] = "Sim" if simei.get("optant") else "Não"
        extracted["MEISince"] = simei.get("since", "")
        registrations = data.get("registrations", [])
        if registrations:
            ies_dict = {
                reg.get("state"): reg.get("number")
                for reg in registrations
                if reg.get("state") and reg.get("number")
            }
            extracted["InscricoesEstaduais"] = json.dumps(ies_dict, ensure_ascii=False)
        members = comp.get("members", [])
        for i, member in enumerate(members[:5]):
            person = member.get("person", {})
            extracted[f"Socio_{i+1}_Nome"] = person.get("name", "")
            extracted[f"Socio_{i+1}_Tipo"] = person.get("type", "")
            extracted[f"Socio_{i+1}_TaxId"] = person.get("taxId", "")
            extracted[f"Socio_{i+1}_Role"] = member.get("role", {}).get("text", "")
        return extracted

    def setup_dataframe_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df["CNPJ_Sanitizado"] = df["CNPJ"].apply(sanitize_cnpj)
        base_cols = [
            "RazaoSocial", "Status", "DataStatus", "NaturezaJuridica", "Porte",
            "CapitalSocial", "Telefone", "Email", "AtividadePrincipal",
            "CNAEs", "Endereco", "Municipio", "UF", "CEP", "Numero", "Complemento",
            "SimplesOptante", "SimplesSince", "MEIOptante", "MEISince",
            "Latitude", "Longitude", "InscricoesEstaduais",
            "TipoEstab", "CNPJ_Matriz_Provavel"
        ]
        for col in base_cols:
            if col not in df.columns:
                df[col] = ""
        for i in range(1, 6):
            for suffix in ["Nome", "Tipo", "TaxId", "Role"]:
                col_name = f"Socio_{i}_{suffix}"
                if col_name not in df.columns:
                    df[col_name] = ""
        return df

    def enrich_dataframe(self, df: pd.DataFrame, token: str = None) -> pd.DataFrame:
        df = self.setup_dataframe_columns(df)
        total_rows = len(df)
        processed_count = 0
        success_count = 0
        logger.info(f"Iniciando enriquecimento de {total_rows} CNPJs")
        for idx, row in df.iterrows():
            try:
                cnpj = row["CNPJ_Sanitizado"]
                if not cnpj or len(cnpj) != 14 or not cnpj.isdigit():
                    logger.warning(f"CNPJ inválido na linha {idx + 1}: {cnpj}")
                    processed_count += 1
                    continue
                if token:
                    progress = int((processed_count / total_rows) * 100)
                    update_task(token, progress=progress)
                data = self.fetch_cnpj_data(cnpj)
                if data:
                    extracted_data = self.extract_data_from_response(data)
                    for key, value in extracted_data.items():
                        if key in df.columns:
                            df.at[idx, key] = value
                    success_count += 1
                    logger.info(f"CNPJ {cnpj} enriquecido com sucesso")
                else:
                    logger.warning(f"Dados não encontrados para CNPJ {cnpj}")
                processed_count += 1
                if processed_count < total_rows:
                    time.sleep(self.delay)
            except Exception as e:
                logger.error(f"Erro ao processar linha {idx + 1}: {e}")
                processed_count += 1
                continue

        # Identificação de matriz e filial
        cnpjs_sanitizados = df["CNPJ_Sanitizado"]
        raiz_map = {}
        for cnpj in cnpjs_sanitizados:
            raiz = cnpj[:8]
            if raiz not in raiz_map:
                raiz_map[raiz] = []
            raiz_map[raiz].append(cnpj)

        for idx, cnpj in enumerate(cnpjs_sanitizados):
            raiz = cnpj[:8]
            sufixo = cnpj[8:12]
            if sufixo == "0001":
                df.at[idx, "TipoEstab"] = "Matriz"
                df.at[idx, "CNPJ_Matriz_Provavel"] = ""
            else:
                df.at[idx, "TipoEstab"] = "Filial"
                cnpj_matriz = next((c for c in raiz_map.get(raiz, []) if c[8:12] == "0001"), f"{raiz}0001")
                df.at[idx, "CNPJ_Matriz_Provavel"] = cnpj_matriz

        logger.info(f"Enriquecimento concluído: {success_count}/{total_rows} CNPJs processados com sucesso")
        return df

def read_excel_file(file_content: bytes) -> pd.DataFrame:
    try:
        df = pd.read_excel(io.BytesIO(file_content), engine='openpyxl', dtype={'CNPJ': str})
    except Exception:
        try:
            df = pd.read_excel(io.BytesIO(file_content), engine='xlrd', dtype={'CNPJ': str})
        except Exception as e:
            raise ValueError(f"Erro ao ler arquivo Excel: {str(e)}")
    if "CNPJ" not in df.columns:
        available_cols = ", ".join(df.columns.tolist())
        raise ValueError(f"Coluna 'CNPJ' não encontrada. Colunas disponíveis: {available_cols}")
    df = df.dropna(subset=['CNPJ'])
    if len(df) == 0:
        raise ValueError("Arquivo não possui CNPJs válidos para processar")
    logger.info(f"Arquivo Excel lido com sucesso: {len(df)} linhas encontradas")
    return df

def save_enriched_excel(df: pd.DataFrame) -> Path:
    Path(FILES_DIR).mkdir(parents=True, exist_ok=True)
    output_filename = f"enriquecido_{uuid.uuid4().hex[:8]}.xlsx"
    output_path = Path(FILES_DIR) / output_filename
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Dados_Enriquecidos')
    logger.info(f"Arquivo salvo: {output_path}")
    return output_path

async def process_excel_sync(uploaded_file: UploadFile) -> Path:
    try:
        logger.info(f"Iniciando processamento síncrono do arquivo: {uploaded_file.filename}")
        contents = await uploaded_file.read()
        df = read_excel_file(contents)
        enricher = CNPJEnricher()
        df_enriched = enricher.enrich_dataframe(df)
        output_path = save_enriched_excel(df_enriched)
        logger.info(f"Processamento síncrono concluído: {output_path}")
        return output_path
    except Exception as e:
        logger.error(f"Erro no processamento síncrono: {e}")
        raise

async def start_background_process(file_content: bytes, file_name: str, token: str):
    try:
        logger.info(f"Iniciando processamento assíncrono para token: {token}")
        update_task(token, status="processing", progress=0)
        df = read_excel_file(file_content)
        enricher = CNPJEnricher()
        df_enriched = enricher.enrich_dataframe(df, token)
        output_path = save_enriched_excel(df_enriched)
        update_task(token, status="completed", progress=100, file=output_path.name)
        logger.info(f"Processamento assíncrono concluído para token: {token}")
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Erro no processamento assíncrono para token {token}: {error_msg}")
        update_task(token, status="failed", progress=0, error=error_msg)
        raise
