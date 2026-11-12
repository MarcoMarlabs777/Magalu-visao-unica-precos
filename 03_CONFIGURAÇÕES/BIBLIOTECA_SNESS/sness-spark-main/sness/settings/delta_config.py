from pyspark import SparkConf
from functools import wraps
import inspect

CONFIGS_DICT = {
    # Configure spark catalog and file system to use Delta and GCS
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.delta.logStore.gs.impl": "io.delta.storage.GCSLogStore",
    "fs.AbstractFileSystem.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
    # Enable schema evolution
    "spark.databricks.delta.schema.autoMerge.enabled": "True",
    # Disable time check when using VACUMM with less than 30 days
    "spark.databricks.delta.retentionDurationCheck.enabled": "False",
    # Execute VACUUM in parallel
    "spark.databricks.delta.vacuum.parallelDelete.enabled": "True",
    # Improve optimize for small files (use repartition instead of coalesce)
    "spark.databricks.delta.optimize.repartition.enabled": "True",
}

# CONFIGURAÇÃO AMBIENTE MAGALU E OU GCP
#def change_spark_config(spark_session):
#    """
#    Changes spark configs to enable Delta Lake
#    """
#    for config_k, config_v in CONFIGS_DICT.items():
#        spark_session.conf.set(
#            config_k,
#            config_v,
#        )
#    return spark_session
# CONFIGURAÇÃO AMBIENTE DATABRICKS
def change_spark_config(spark_session):
    """
    Configura Delta Lake em ambientes Spark tradicionais.
    No Databricks Serverless, utiliza a sessão
    gerenciada pela plataforma sem alterar
    configurações restritas.
    """

    import os

    if os.environ.get("SNESS_DATABRICKS_SERVERLESS") == "true":
        return spark_session

    # CONFIGURAÇÃO ORIGINAL DO SNESS
    for config_k, config_v in CONFIGS_DICT.items():
        spark_session.conf.set(
            config_k,
            config_v,
        )

    return spark_session


def define_base_spark_conf():
    conf = SparkConf()
    for config_k, config_v in CONFIGS_DICT.items():
        conf.set(config_k, config_v)
    return conf


def ensure_delta_table(func):
    @wraps(func)
    def wrapper(*args, **kwargs):

        sig = inspect.signature(func)
        bound_args = sig.bind(*args, **kwargs)
        bound_args.apply_defaults()

        if 'spark_session' in bound_args.arguments:

            original_spark = bound_args.arguments['spark_session']
            updated_spark = change_spark_config(original_spark)

            bound_args.arguments['spark_session'] = updated_spark

            from delta.tables import DeltaTable

            if 'DeltaTable' not in func.__globals__:
                func.__globals__['DeltaTable'] = DeltaTable

            return func(*bound_args.args, **bound_args.kwargs)

        return func(*args, **kwargs)

    return wrapper


def inject_delta_dependency(cls):

    original_init = cls.__init__

    @wraps(original_init)
    def new_init(self, *args, **kwargs):

        sig = inspect.signature(original_init)
        bound_args = sig.bind(self, *args, **kwargs)
        bound_args.apply_defaults()

        if 'spark_session' in bound_args.arguments:
            original_spark = bound_args.arguments['spark_session']

            updated_spark = change_spark_config(original_spark)
            bound_args.arguments['spark_session'] = updated_spark

            from delta.tables import DeltaTable

            self.DeltaTable = DeltaTable
            self.spark = updated_spark

        original_init(*bound_args.args, **bound_args.kwargs)

    cls.__init__ = new_init
    return cls
