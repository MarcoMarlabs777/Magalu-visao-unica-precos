"""
Compatibilidade Sness com Databricks Serverless.

Ambiente sem GCP, Google Secret Manager ou MagaluCloud S3.
Os dados são acessados por meio de Databricks Volumes.
"""


class S3Session:
    """
    Classe preservada para compatibilidade com imports existentes.
    """

    def __init__(self):
        pass

    def _fetch_secret(self):
        raise RuntimeError(
            "Google Secret Manager desabilitado. "
            "Este ambiente utiliza Databricks Volumes."
        )

    def boto3_session(self, my_config=None):
        raise RuntimeError(
            "MagaluCloud S3 desabilitado. "
            "Utilize leitura e escrita Parquet em Databricks Volumes."
        )