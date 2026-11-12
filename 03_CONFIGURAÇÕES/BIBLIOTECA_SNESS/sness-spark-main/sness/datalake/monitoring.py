
"""
Sness Spark - Monitoramento de datasets.

Adaptação para Databricks Serverless.
Monitoramento externo GCS/Prometheus desabilitado.
"""

import logging


class Monitoring:
    """
    Mantém a interface de monitoramento do Sness.

    O monitoramento externo permanece desabilitado
    durante a execução local no Databricks.
    """

    def __init__(
        self,
        spark_session,
        enviroment,
        zone,
        namespace,
        metric_name,
        dataset
    ):
        self.spark_session = spark_session
        self.enviroment = enviroment.value
        self.zone = zone.value
        self.namespace = namespace
        self.dataset = dataset
        self.metric_name = metric_name

        self.dataset_key = self._get_dataset_key()
        self.dataset_query = None
        self.result = None

        self.enable_monitoring = self._check_monitoring()

        logging.info(
            "Monitoramento externo desabilitado: %s",
            self.dataset_key
        )

    def _get_dataset_key(self):
        return (
            f"{self.enviroment}-"
            f"{self.zone}-"
            f"{self.namespace}-"
            f"{self.dataset}"
        )

    def _check_monitoring(self):
        return False

    def _load_monitoring_json(self):
        return {}

    def monitor_dataset(self, df):
        logging.info(
            "Monitoramento ignorado para dataset: %s",
            self.dataset
        )

    def send_to_prometheus(self):
        logging.info(
            "Envio ao Prometheus desabilitado."
        )
