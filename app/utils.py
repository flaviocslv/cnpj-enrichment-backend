def sanitize_cnpj(cnpj: str) -> str:
    """Remove caracteres não numéricos e garante 14 dígitos com zeros à esquerda."""
    import re
    digits = re.sub(r'\D', '', str(cnpj))
    return digits.zfill(14)[:14]
