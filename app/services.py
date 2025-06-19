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

    # Adiciona colunas separadas com base na API CNPJa
    additional_columns = [
        # Receita Federal
        "razao_social", "nome_fantasia", "data_inicio_atividade", "email",
        "logradouro", "numero", "complemento", "bairro", "municipio", "uf", "cep",
        "telefone_1", "telefone_2", "fax",
        "cnae_principal_codigo", "cnae_principal_descricao",
        "natureza_juridica_codigo", "natureza_juridica_descricao",
        "situacao", "data_situacao", "motivo_situacao",
        "porte", 

        # Simples Nacional
        "simples_opcao", "simples_data_opcao", "simples_data_exclusao",
        "mei_opcao", "mei_data_opcao", "mei_data_exclusao",

        # Inscrição Estadual (consolidada)
        "ie_ufs", "ie_numeros", "ie_situacoes",

        # SUFRAMA
        "suframa_numero", "suframa_situacao", "suframa_incentivo",

        # Geocodificação
        "latitude", "longitude",

        # Sócios (lista concatenada)
        "socios_nomes"
    ]

    for col in additional_columns:
        df[col] = ""

    for i, row in df.iterrows():
        cnpj = row["CNPJ_Sanitizado"]
        url = f"{API_URL}/{cnpj}?simples=true&registrations=BR&geocoding=true"
        try:
            response = requests.get(url)
            if response.status_code != 200:
                continue
            data = response.json()

            # Receita Federal
            df.at[i, "razao_social"] = data.get("razaoSocial", "")
            df.at[i, "nome_fantasia"] = data.get("nomeFantasia", "")
            df.at[i, "data_inicio_atividade"] = data.get("dataInicioAtividade", "")
            df.at[i, "email"] = data.get("email", "")

            endereco = data.get("endereco", {})
            df.at[i, "logradouro"] = endereco.get("logradouro", "")
            df.at[i, "numero"] = endereco.get("numero", "")
            df.at[i, "complemento"] = endereco.get("complemento", "")
            df.at[i, "bairro"] = endereco.get("bairro", "")
            df.at[i, "municipio"] = endereco.get("municipio", "")
            df.at[i, "uf"] = endereco.get("uf", "")
            df.at[i, "cep"] = endereco.get("cep", "")

            telefones = data.get("telefones", [])
            if len(telefones) > 0:
                df.at[i, "telefone_1"] = telefones[0].get("numero", "")
            if len(telefones) > 1:
                df.at[i, "telefone_2"] = telefones[1].get("numero", "")
            df.at[i, "fax"] = data.get("fax", "")

            cnae_principal = data.get("cnaePrincipal", {})
            df.at[i, "cnae_principal_codigo"] = cnae_principal.get("codigo", "")
            df.at[i, "cnae_principal_descricao"] = cnae_principal.get("descricao", "")

            natureza = data.get("naturezaJuridica", {})
            df.at[i, "natureza_juridica_codigo"] = natureza.get("codigo", "")
            df.at[i, "natureza_juridica_descricao"] = natureza.get("descricao", "")

            df.at[i, "situacao"] = data.get("situacao", "")
            df.at[i, "data_situacao"] = data.get("dataSituacao", "")
            df.at[i, "motivo_situacao"] = data.get("motivoSituacao", "")
            df.at[i, "porte"] = data.get("porte", "")

            # Simples Nacional
            simples = data.get("simples", {})
            df.at[i, "simples_opcao"] = simples.get("simples", "")
            df.at[i, "simples_data_opcao"] = simples.get("dataOpcaoSimples", "")
            df.at[i, "simples_data_exclusao"] = simples.get("dataExclusaoSimples", "")
            df.at[i, "mei_opcao"] = simples.get("mei", "")
            df.at[i, "mei_data_opcao"] = simples.get("dataOpcaoMei", "")
            df.at[i, "mei_data_exclusao"] = simples.get("dataExclusaoMei", "")

            # Inscrição Estadual
            ies = data.get("inscricoesEstaduais", [])
            df.at[i, "ie_ufs"] = ", ".join([ie.get("uf", "") for ie in ies])
            df.at[i, "ie_numeros"] = ", ".join([ie.get("numero", "") for ie in ies])
            df.at[i, "ie_situacoes"] = ", ".join([ie.get("situacao", "") for ie in ies])

            # SUFRAMA
            suframa = data.get("suframa", {})
            df.at[i, "suframa_numero"] = suframa.get("numero", "")
            df.at[i, "suframa_situacao"] = suframa.get("situacao", "")
            df.at[i, "suframa_incentivo"] = suframa.get("incentivoFiscal", "")

            # Geocodificação
            geo = data.get("geocodificacao", {})
            df.at[i, "latitude"] = geo.get("latitude", "")
            df.at[i, "longitude"] = geo.get("longitude", "")

            # Sócios
            socios = data.get("socios", [])
            df.at[i, "socios_nomes"] = "; ".join([s.get("nome", "") for s in socios])

        except Exception as e:
            print(f"Erro ao processar CNPJ {cnpj}: {e}")

        time.sleep(DELAY)

    # Salva o Excel com os dados enriquecidos
    Path("files").mkdir(parents=True, exist_ok=True)
    output = Path("files") / f"{uuid.uuid4()}.xlsx"
    df.to_excel(output, index=False)
    return output
