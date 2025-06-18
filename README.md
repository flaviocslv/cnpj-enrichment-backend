# CNPJ Enrichment Backend

API em Python + FastAPI para enriquecer planilhas Excel com dados de CNPJ utilizando a API pública cnpja.com.

## 🚀 Como executar localmente

```bash
git clone https://github.com/SEU_USUARIO/cnpj-enrichment-backend.git
cd cnpj-enrichment-backend
pip install -r requirements.txt
uvicorn app.main:app --reload
```

A API ficará disponível em: `http://localhost:8000`.

## 🧩 Endpoints

- **POST /upload**: recebe arquivo Excel `.xlsx` com coluna `CNPJ`, retorna JSON com `download_url`.
- **GET /download/{filename}**: serve o Excel enriquecido.

## 📦 Implantação com Render

Inclui `render.yaml` com configuração:

- Serviço web gratuito em Python
- Comandos de build e start definidos

## ⚙️ Configurações

- `app/config.py`: defina `API_URL` e `DELAY` (segundos de espera entre consultas).
- `app/utils.py`: sanitização de CNPJ.
- `app/services.py`: lógica de leitura, enriquecimento e gravação do Excel.