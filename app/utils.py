def sanitize_cnpj(cnpj: str) -> str:
    return ''.join(filter(str.isdigit, str(cnpj)))