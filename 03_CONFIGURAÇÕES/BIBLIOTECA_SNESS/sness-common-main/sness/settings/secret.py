"""
Sness Common - Gerenciamento de configurações.

Compatibilidade com Databricks.
Não utiliza Google Cloud Secret Manager.
"""

import os


def get_var_secret_manager(name):
    """
    Obtém uma configuração a partir de variável de ambiente.

    Mantém a interface original do Sness.

    Args:
        name (str): Nome da variável.

    Returns:
        str | None: Valor encontrado ou None.
    """

    if not name:
        return None

    return os.environ.get(name)