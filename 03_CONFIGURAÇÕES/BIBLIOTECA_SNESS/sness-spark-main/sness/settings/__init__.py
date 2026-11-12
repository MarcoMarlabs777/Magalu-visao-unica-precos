import os

REGION = "global"
PROJECT = "maga-bigdata"
FOO = "xBC7vDcmvqD49qif"
HOOK = os.getenv("SLACK_TOKEN")
DATAPROC_ZONES_US = ['us-east1-b', 'us-east1-c', 'us-east1-d']
DATAPROC_ZONES_SA = [
    'southamerica-east1-a',
    'southamerica-east1-b',
    'southamerica-east1-c',
]
DEFAULT_CLUSTER_NAME = "prd-cluster-jobs"
CATALOG_HOST = os.getenv("SQLHOST") or "10.7.0.3"
HIVE_METASTORE_URI = "thrift://10.209.0.58:9083"
HIVE_METASTORE_WAREHOUSE_DIR = "gs://prd-hive-metadata/spark/warehouse"
DATAPROC_LABELS = {
    'project': 'dataproc-spark-job',
    'se_org': 'magazineluiza',
    'se_product': 'composer-dataproc',
    'se_shared_resource': 'true',
}
DATAPROC_SQOOP_LABELS = {
    'project': 'dataproc-sqoop',
    'tribe': 'dados',
    'se_org': 'magazineluiza',
    'se_tribe': 'dados',
    'se_vertical': 'ecomm-dados',
    'se_product': 'composer-sqoop-dataproc',
    'se_shared_resource': 'true',
}
PUSH_GATEWAY = 'http://maga-production-prometheus-pushgateway-external.gcp.luizalabs.com:9091'  # noqa
NAMESPACES_TO_CONVERT = []
