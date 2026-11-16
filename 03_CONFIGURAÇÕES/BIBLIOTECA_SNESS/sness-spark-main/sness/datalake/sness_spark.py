import logging
from datetime import datetime
import os
from typing import TYPE_CHECKING, Iterable, Optional
from functools import wraps

from pyspark.sql import SparkSession, DataFrame
from sness.settings import delta_config
from pyspark import SparkConf
from sness.settings import cloud_config
from sness.datalake import delta_operations, metadata_operations
from sness.datalake.delta_operations import (
    get_last_version_of_dataset,
    execute_optimize,
    execute_clustering,
    check_if_is_clustered,
)
from sness.datalake.metadata import DatasetMetadata, Environment, Zone
from sness.datalake.monitoring import Monitoring
from sness.datalake.changelogs import Changelogs
from sness.settings.delta_namespaces import convert_namespace_to_delta

# The IDE sees this, but it NEVER runs at execution time
if TYPE_CHECKING:
    from delta.tables import DeltaTable


class SnessSpark:
    """
    Spark wrapper class made to abstract the way Sness data lake
    works and provide some easier ways around spark.
    :param conf: :class:`pyspark.conf.SparkConf` default None,
    used to customize the SparkSession as needed.
    :param conf: :class:`pyspark.conf.SparkConf`, default None:
    SparkConf intance to customize SparkSession
    """

    def __init__(self, conf: SparkConf = None):
        self.spark: SparkSession = self.__build_session(conf=conf)

    @property
    def merge(self):
        """
        Wrapper of delta merge method, returns a
        :class:`_SnessDataFrameMerge` instance
        """
        return _SnessDataFrameMerge(self.spark)

    @property
    def update(self):
        """
        Wrapper of delta format method, returns a
        :class:`_SnessDataFrameUpdate` instance
        """
        return _SnessDataFrameUpdate(self.spark)

    @property
    def delete(self):
        """
        Wrapper of delta delete method, returns a
        :class:`_SnessDataFrameDelete` instance
        """
        return _SnessDataFrameDelete(self.spark)

    @property
    def history(self):
        """
        Wrapper of delta history method, returns a
        :class:`_SnessDataFrameHistory` instance
        """
        return _SnessDataFrameHistory(self.spark)

    @property
    def read(self):
        """
        Wrapper of spark read method, returns a
        :class:`_SnessDataFrameReader` instance
        """
        return _SnessDataFrameReader(self.spark).read()

    @property
    def readStream(self):
        """
        Wrapper of spark readStream method, returns a
        :class:`_SnessDataFrameReader` instance
        """
        return _SnessDataFrameReader(self.spark).readStream()

    @property
    def write(self):
        """
        Wrapper of spark read method, returns a
        :class:`_SnessDataFrameWriter` instance
        """
        return _SnessDataFrameWriter(self.spark)

    @property
    def writeStream(self):
        """
        Wrapper of spark read method, returns a
        :class:`_SnessDataFrameStreamWriter` instance
        """
        return _SnessDataFrameStreamWriter(self.spark)

    @property
    def deltaOperation(self):
        return _SnessDataFrameDeltaOperations(self.spark)

    # Build_Session refeito para adapatar ao Databricks pois é uma função Nativa
    #def __build_session(self, conf: SparkConf = None):
    #    session_builder = SparkSession.builder
    #    base_conf = delta_config.define_base_spark_conf()
    #    for x in base_conf.getAll():
    #        session_builder = session_builder.config(x[0], x[1])
    #
    #    if conf:
    #        for x in conf.getAll():
    #            session_builder = session_builder.config(x[0], x[1])
    #
    #    return session_builder.enableHiveSupport().getOrCreate()

    def __build_session(self, conf: SparkConf = None):
        """
        Utiliza a sessão Spark gerenciada pelo Databricks Serverless.
        Não aplica configurações de infraestrutura externa.
        """
        return SparkSession.builder.getOrCreate()


@delta_config.inject_delta_dependency
class _SnessSparkBase:
    def __init__(self, spark_session):
        self.spark_session = spark_session
        if TYPE_CHECKING:
            self.DeltaTable: DeltaTable

    def _get_path(
        self,
        environment,
        zone,
        namespace,
        dataset,
        cloud_environment=cloud_config.CLOUD_ENVIRONMENT,
        mode=None,
        version=None,
        version_offset=0,
        write_needed=False,
    ):
        """Resolve caminhos: leitura somente na origem; escrita somente em resultados.

        Mantem o comportamento original SNESS fora do modo Databricks.
        """
        if os.environ.get("SNESS_DATABRICKS_SERVERLESS") == "true":
            base = "/Volumes/test/default/dados_origin_visao_unica"

            if not isinstance(dataset, str) or not dataset:
                raise ValueError("Nome de dataset invalido")
            if "/" in dataset or "\\" in dataset or dataset in (".", ".."):
                raise ValueError(f"Nome de dataset nao permitido: {dataset}")

            if write_needed:
                return f"{base}/resultados/{dataset}"
            return f"{base}/parquet_volumetria_final/{dataset}"

        ds = DatasetMetadata(namespace, dataset)
        return ds.build_lake_path(
            environment,
            zone,
            mode=mode,
            version=version,
            version_offset=version_offset,
            write_needed=write_needed,
            cloud_environment=cloud_environment,
        )

    # ==========================================
    # IMPLEMENTACAO ORIGINAL MAGALU / GCP
    # ==========================================
    @staticmethod
    def resolve_path(func):
        @wraps(func)
        def wrapper(
            self,
            environment,
            zone,
            namespace: str,
            dataset: str,
            cloud_environment=cloud_config.CLOUD_ENVIRONMENT,
            mode=None,
            version=None,
            version_offset=0,
            write_needed=False,
        ):
            path = self._get_path(
                environment,
                zone,
                namespace,
                dataset,
                cloud_environment,
                mode,
                version,
                version_offset,
                write_needed,
            )
            return func(self, path)

        return wrapper

    def _build_version(self):
        return datetime.utcnow().strftime('%Y%m%d%H%M%S')

    def _parquet_only_zone(self, zone):
        return zone in (Zone.RAW, Zone.TRUSTED)


class _SnessDataFrameMerge(_SnessSparkBase):
    def parquet(
        self,
        df,
        environment,
        zone,
        namespace,
        dataset,
        merge_keys,
        cloud_environment=cloud_config.CLOUD_ENVIRONMENT,
        **options,
    ):
        """
        Used to merge data from a data frame with a DeltaLake dataset
        :param environment: string, one of :class:`Environment` values
        :param zone: string, one of :class:`Zone` values
        :param namespace: string, name of namespace
        :param dataset: string, name of dataset
        :param cloud_environment: string, target cloud name
        :param merge_keys: string, name of key columns used to join data from
        data lake and data frame.<br>

        To merge data, you will have to access both fields from data lake and
        from data frame to create your join and conditons. To do so, use the
        following aliases:
            To access fields from the data lake, you can use the alias 'lake'.
            To access fields from the data frame, you can use the alias 'df'.

        'merge_keys' examples:
            merge(...,merge_keys="lake.order_id = df.num_id")
            merge(...,merge_keys="lake._id = df._id and lake.sku = df.sku")
        """

        if options.get('delta', True):
            path = self._get_path(
                environment,
                zone,
                namespace,
                dataset,
                cloud_environment,
            )

            if self.DeltaTable.isDeltaTable(self.spark_session, path):

                delta_operations.check_and_mark_with_too_many_files(
                    spark_session=self.spark_session,
                    environment=environment,
                    zone=zone,
                    namespace=namespace,
                    dataset=dataset,
                    max_allowed_files=options.get(
                        'delta_max_allowed_files', 10000
                    ),
                    cloud_environment=cloud_environment,
                )

                is_compacted = delta_operations.compact_dataset(
                    spark_session=self.spark_session,
                    environment=environment,
                    zone=zone,
                    namespace=namespace,
                    dataset=dataset,
                    cloud_environment=cloud_environment,
                )

                if is_compacted:
                    retention_hours = 2
                else:
                    retention_hours = options.get('delta_retention_hours', 168)

                delta_operations.vacuum(
                    spark_session=self.spark_session,
                    environment=environment,
                    zone=zone,
                    namespace=namespace,
                    dataset=dataset,
                    retention_hours=retention_hours,
                    cloud_environment=cloud_environment,
                )

                merge_table = (
                    self.DeltaTable.forPath(self.spark_session, f"{path}")
                    .alias("lake")
                    .merge(source=df.alias("df"), condition=merge_keys)
                )

                try:
                    options['format'] = 'delta'
                    metadata_operations.save_metadata(
                        self.spark_session,
                        df,
                        environment,
                        zone,
                        namespace,
                        dataset,
                        path,
                        options,
                    )
                except Exception as msg:
                    logging.error(
                        f"Error in save metadata: {dataset}: {str(msg)}"
                    )

                return merge_table
            else:
                raise ValueError(f'Merge destination {path} is not DeltaLake')


class _SnessDataFrameUpdate(_SnessSparkBase):
    """
    _SnessDataFrameUpdate
    =====================================================================

    Class to handle Delta Lake operations.

    - **parquet**: Update rows in a Delta Lake table
    - **parquetWithDF**: Update rows in a Delta Lake table using a data frame
    """

    def parquet(
        self,
        environment,
        zone,
        namespace,
        dataset,
        condition,
        changes,
        cloud_environment=cloud_config.CLOUD_ENVIRONMENT,
    ):
        """
        Update data from the table on the rows that match the given
        ``condition``, which performs the rules defined by ``changes``.

        This method is a wrapper of Delta Lake update method.
        https://docs.delta.io/1.1.0/delta-update.html#update-a-table

        Example::

            # condition using SQL formatted string
            ss = SnessSpark()

            ss.merge.update(
                environment=Environment.STAGING,
                zone=Zone.TRUSTED,
                namespace='witcher',
                dataset='events',
                cloud_environment=cloud_config.CLOUD_ENVIRONMENT,
                condition = "eventType = 'clck'",
                changes = { "eventType": "'click'" } )

            # condition using Spark SQL functions
            ss.merge.update(
                environment=Environment.STAGING,
                zone=Zone.TRUSTED,
                namespace='witcher',
                dataset='events',
                cloud_environment=cloud_config.CLOUD_ENVIRONMENT,
                condition = col("eventType") == "clck",
                changes = { "eventType": lit("click") } )

        :param environment: string, one of :class:`Environment` values
        :param zone: string, one of :class:`Zone` values
        :param namespace: string, name of namespace
        :param dataset: string, name of dataset
        :param condition: string, condition used to filter data to update
        :param changes: dict with str as keys and str or
                        pyspark.sql.Column as values
        :param cloud_environment: string, target cloud name
        """

        ds = DatasetMetadata(namespace, dataset)
        path = ds.build_lake_path(
            environment=environment,
            zone=zone,
            cloud_environment=cloud_environment,
        )
        logging.debug(f'path: {path}')

        if self.DeltaTable.isDeltaTable(self.spark_session, path):
            # Set Delta Lake configs

            deltaTable = self.DeltaTable.forPath(self.spark_session, path)
            deltaTable.update(condition=condition, set=changes)
            logging.info(f'Delta update done for {path}')
        else:
            raise ValueError(f'Update destination {path} is not DeltaLake')

    def parquetWithDF(
        self,
        df,
        environment,
        zone,
        namespace,
        dataset,
        condition,
        cloud_environment=cloud_config.CLOUD_ENVIRONMENT,
    ):
        """
        Update data from a DataFrame ``df`` on the rows
        in the ``lake`` that match the given ``condition``.

        Example::
            ss = SnessSpark()

            # Spark DataFrame with updated values
            df = ss.read.parquet(...)

            # ``lake`` is a Delta Lake table
            ss.update.parquetWithDF(
                df=df,
                environment=Environment.STAGING,
                zone=Zone.TRUSTED,
                namespace='witcher',
                dataset='events',
                cloud_environment=cloud_config.CLOUD_ENVIRONMENT,
                condition = "lake.primary_key = df.primary_key" )

        :param df: Spark DataFrame, data to update
        :param environment: string, one of :class:`Environment` values
        :param zone: string, one of :class:`Zone` values
        :param namespace: string, name of namespace
        :param dataset: string, name of dataset
        :param condition: string, condition used to filter data to update
        :type condition: str or pyspark.sql.Column
        :param changes: dict with str as keys and str or
                        pyspark.sql.Column as values
        :param cloud_environment: string, target cloud name
        """

        ds = DatasetMetadata(namespace, dataset)
        path = ds.build_lake_path(
            environment=environment,
            zone=zone,
            cloud_environment=cloud_environment,
        )
        logging.debug(f'path: {path}')

        if self.DeltaTable.isDeltaTable(self.spark_session, path):
            # Set Delta Lake configs

            deltaTable = self.DeltaTable.forPath(self.spark_session, path)
            deltaTable.alias("lake").merge(
                df.alias("df"), condition
            ).whenMatchedUpdateAll().execute()
            logging.info(f'Delta update done for {path} with a DataFrame')
        else:
            raise ValueError(f'Update destination {path} is not DeltaLake')


class _SnessDataFrameDelete(_SnessSparkBase):
    """
    _SnessDataFrameDelete
    =====================================================================

    Class to handle Delta Lake's delete operations.

    - **parquet**: Delete rows in a Delta Lake table
    """

    def parquet(
        self,
        environment,
        zone,
        namespace,
        dataset,
        condition,
        cloud_environment=cloud_config.CLOUD_ENVIRONMENT,
    ):
        """
        Delete data from the table on the rows that match the given
        ``condition``.

        This method is a wrapper of Delta Lake delete method.
        https://docs.delta.io/1.1.0/delta-update.html#delete-from-a-table

        Example::
            ss = SnessSpark()

            # predicate using SQL formatted string
            ss.delete.parquet(
                environment=Environment.STAGING,
                zone=Zone.TRUSTED,
                namespace='witcher',
                dataset='events',
                cloud_environment=cloud_config.CLOUD_ENVIRONMENT,
                condition="date < '2021-01-01'")

            # predicate using Spark SQL functions
            ss.delete.parquet(
                environment=Environment.STAGING,
                zone=Zone.TRUSTED,
                namespace='witcher',
                dataset='events',
                cloud_environment=cloud_config.CLOUD_ENVIRONMENT,
                col("date") < "2021-01-01")

        :param environment: string, one of :class:`Environment` values
        :param zone: string, one of :class:`Zone` values
        :param namespace: string, name of namespace
        :param dataset: string, name of dataset
        :param condition: condition used to filter data to delete
        :type condition: str or pyspark.sql.Column
        :param cloud_environment: string, target cloud name
        """

        ds = DatasetMetadata(namespace, dataset)
        path = ds.build_lake_path(
            environment=environment,
            zone=zone,
            cloud_environment=cloud_environment,
        )
        logging.debug(f'path: {path}')

        if self.DeltaTable.isDeltaTable(self.spark_session, path):
            # Set Delta Lake configs

            deltaTable = self.DeltaTable.forPath(self.spark_session, path)
            deltaTable.delete(condition=condition)
            logging.info(f'Delta delete done for {path}')
        else:
            raise ValueError(f'Delete destination {path} is not DeltaLake')


class _SnessDataFrameHistory(_SnessSparkBase):
    """
    _SnessDataFrameHistory
    =====================================================================

    Class to handle Delta Lake's history operations.

    - **parquet**: Get history of a Delta Lake table in Spark DataFrame format.
                   This method is read-only.
    """

    def parquet(
        self,
        environment,
        zone,
        namespace,
        dataset,
        cloud_environment=cloud_config.CLOUD_ENVIRONMENT,
    ):
        """
        Get history of a Delta Lake table in Spark DataFrame format.

        This method is a wrapper of Delta Lake history method.
        https://docs.delta.io/latest/delta-utility.html#retrieve-delta-table-history

        Example::
            ```
            ss = SnessSpark()

            # get history of a Delta Lake table
            df_history = ss.history.parquet(
                environment=Environment.STAGING,
                zone=Zone.TRUSTED,
                namespace='witcher',
                dataset='events',
                cloud_environment='mgc')

            df_history.show(truncate=False)
            ```

        :param environment: string, one of :class:`Environment` values
        :param zone: string, one of :class:`Zone` values
        :param namespace: string, name of namespace
        :param dataset: string, name of dataset
        :return: Spark DataFrame with history of a Delta Lake table
        """

        ds = DatasetMetadata(namespace, dataset)
        path = ds.build_lake_path(
            environment=environment,
            zone=zone,
            cloud_environment=cloud_environment,
        )
        logging.debug(f"Starting ss.history.parquet for {path}")

        if self.DeltaTable.isDeltaTable(self.spark_session, path):
            deltaTable = self.DeltaTable.forPath(self.spark_session, path)
            return deltaTable.history()
        else:
            raise ValueError(f'Update destination {path} is not DeltaLake')


class _SnessDataFrameReader(_SnessSparkBase):
    def read(self):
        """
        Wrapper of spark read method, returns itself
        """
        self._reader = self.spark_session.read
        return self

    def readStream(self):
        """
        Wrapper of spark readStream method, returns itself
        """
        self._reader = self.spark_session.readStream
        return self

    def csv(
        self,
        environment,
        zone,
        namespace,
        dataset,
        cloud_environment=cloud_config.CLOUD_ENVIRONMENT,
        version=None,
        version_offset=0,
        **options,
    ):
        """
        Used to read csv files as a Spark :class:`pyspark.sql.DataFrame`
        :param environment: string, one of :class:`Environment` values
        :param zone: string, one of :class:`Zone` values
        :param namespace: string, name of namespace
        :param dataset: string, name of dataset
        :param version: string, id of the wanted version
        :param version_offset: string, version offset meaning that 0
        is the most recent
        :param options: Spark-native options that can be passed on
        """
        return self.load(
            environment,
            zone,
            namespace,
            dataset,
            cloud_environment,
            version=version,
            version_offset=version_offset,
            format='csv',
            **options,
        )

    def json(
        self,
        environment,
        zone,
        namespace,
        dataset,
        cloud_environment=cloud_config.CLOUD_ENVIRONMENT,
        version=None,
        version_offset=0,  # TODO - use -1 instead of 1
        **options,
    ):
        """
        Used to read json files as a Spark :class:`pyspark.sql.DataFrame`
        :param environment: string, one of :class:`Environment` values
        :param zone: string, one of :class:`Zone` values
        :param namespace: string, name of namespace
        :param dataset: string, name of dataset
        :param version: string, id of the wanted version
        :param version_offset: string, version offset meaning that 0
        is the most recent
        :param options: Spark-native options that can be passed on
        """
        return self.load(
            environment,
            zone,
            namespace,
            dataset,
            cloud_environment,
            version=version,
            version_offset=version_offset,
            format='json',
            **options,
        )

    def parquet(
        self,
        environment,
        zone,
        namespace,
        dataset,
        cloud_environment=cloud_config.CLOUD_ENVIRONMENT,
        version=None,
        version_offset=0,
        **options,
    ) -> DataFrame:
        """
        Used to read parquet files as a Spark :class:`pyspark.sql.DataFrame`
        :param environment: string, one of :class:`Environment` values
        :param zone: string, one of :class:`Zone` values
        :param namespace: string, name of namespace
        :param dataset: string, name of dataset
        :param version: string, id of the wanted version
        :param version_offset: string, version offset meaning that 0
        is the most recent
        :param options: Spark-native options that can be passed on
        """

        return self.load(
            environment,
            zone,
            namespace,
            dataset,
            cloud_environment,
            version=version,
            version_offset=version_offset,
            format='parquet',
            **options,
        )

    def avro(
        self,
        environment,
        zone,
        namespace,
        dataset,
        cloud_environment=cloud_config.CLOUD_ENVIRONMENT,
        version=None,
        version_offset=0,
        **options,
    ):
        """
        Used to read avro files as a Spark :class:`pyspark.sql.DataFrame`
        :param environment: string, one of :class:`Environment` values
        :param zone: string, one of :class:`Zone` values
        :param namespace: string, name of namespace
        :param dataset: string, name of dataset
        :param version: string, id of the wanted version
        :param version_offset: string, version offset meaning that 0
        is the most recent
        :param options: Spark-native options that can be passed on
        """
        return self.load(
            environment,
            zone,
            namespace,
            dataset,
            cloud_environment,
            version=version,
            version_offset=version_offset,
            format='avro',
            **options,
        )

    def kafka(self, topic_name, bootstrap_servers, **options):
        """
        Used to read files from a kafka topic
        :param df: :class:`pyspark.sql.DataFrame` instance
        :param environment: string, one of :class:`Environment` values
        :param topic_name: Name of topic that data will be written to
        :param bootstrap_servers: Kafka servers in the format
        host1:port1,host2:port2. Default value follows the environment
        parameters. In case of Environment.PRODUCTION it will be
        kafka-data-engineering.gcp.luizalabs.com:9092, otherwise
        kafka-stg.gcp.luizalabs.com:9092
        :param options: Spark-native options that can be passed on

        For more details: https://spark.apache.org/docs/2.2.0/structured-streaming-kafka-integration.html  # noqa
        """
        reader = (
            self._reader.format("kafka")
            .option("kafka.bootstrap.servers", bootstrap_servers)
            .option("subscribe", topic_name)
        )

        return reader.load(**options)

    def load(
        self,
        environment,
        zone,
        namespace,
        dataset,
        cloud_environment=cloud_config.CLOUD_ENVIRONMENT,
        version=None,
        version_offset=0,
        **options,
    ):
        """
        Used to read files as a Spark :class:`pyspark.sql.DataFrame`,
        format must be specified
        :param environment: string, one of :class:`Environment` values
        :param zone: string, one of :class:`Zone` values
        :param namespace: string, name of namespace
        :param dataset: string, name of dataset
        :param version: string, id of the wanted version
        :param version_offset: string, version offset meaning that 0
        is the most recent
        :param options: Spark-native options that can be passed on
        :param cloud_environment: string, target cloud name
        """


        # ==========================================
        # LEITURA ADAPTADA PARA DATABRICKS
        # ==========================================
        if os.environ.get("SNESS_DATABRICKS_SERVERLESS") == "true":
            # Leitura EXCLUSIVA da origem, mesmo quando ha arquivos
            # com o mesmo nome no diretorio de resultados.
            caminho = self._get_path(
                environment=environment,
                zone=zone,
                namespace=namespace,
                dataset=dataset,
                cloud_environment=cloud_environment,
                version=version,
                version_offset=version_offset,
                write_needed=False,
            )

            if version is not None or int(version_offset) != 0:
                raise ValueError("Leitura versionada nao suportada no Serverless")

            if not os.path.isdir(caminho):
                raise FileNotFoundError(
                    f"Dataset '{dataset}' nao encontrado. "
                    f"Caminho de entrada: {caminho}"
                )

            opcoes_spark = dict(options)
            formato = opcoes_spark.pop("format", "parquet")
            if formato != "parquet":
                raise ValueError("Esta adaptacao suporta apenas Parquet")

            print(f"[SNESS READ REAL] dataset={dataset} | path={caminho}", flush=True)
            return self._reader.format(formato).options(**opcoes_spark).load(caminho)

        # ==========================================
        # LEITURA ORIGINAL SNESS
        # ==========================================


        path = self._get_path(
            environment,
            zone,
            namespace,
            dataset,
            version=version,
            version_offset=version_offset,
            write_needed=False,
            cloud_environment=cloud_environment,
        )
        if delta_operations.is_delta_lake(
            self.spark_session,
            environment,
            zone,
            namespace,
            dataset,
            options,
            cloud_environment,
        ):

            options['format'] = 'delta'
            try:
                if int(version_offset) > 0:
                    last_version = get_last_version_of_dataset(
                        self.spark_session,
                        environment,
                        zone,
                        namespace,
                        dataset,
                        cloud_environment,
                    )
                    if int(last_version) - int(version_offset) < 0:
                        options["versionAsOf"] = int(last_version)
                    else:
                        options["versionAsOf"] = int(last_version) - int(
                            version_offset
                        )
            except Exception as err:
                raise ValueError(
                    f"version_offset should be a numeric value: {err}"
                )
        return self._reader.load(path=path, **options)


class _SnessDataFrameStreamWriter(_SnessSparkBase):
    def __init__(self, spark_session):
        super().__init__(spark_session)
        self.environment_spec = (
            os.environ['ENVIRONMENT_SPEC']
            if 'ENVIRONMENT_SPEC' in os.environ
            else ""
        )

    def csv(
        self,
        df,
        environment,
        zone,
        namespace,
        dataset,
        cloud_environment=cloud_config.CLOUD_ENVIRONMENT,
        **options,
    ):
        """
        Used to write files as csv
        :param df: :class:`pyspark.sql.DataFrame` instance
        :param environment: string, one of :class:`Environment` values
        :param zone: string, one of :class:`Zone` values
        :param mode: string, one of :class:`Mode` values
        :param namespace: string, name of namespace
        :param dataset: string, name of dataset
        :param version: string, id of the wanted version
        :param version_offset: string, version offset meaning that 0
        is the most recent
        :param options: Spark-native options that can be passed on
        :param cloud_environment: string, target cloud name
        """
        return self.save(
            df,
            environment,
            zone,
            namespace,
            dataset,
            cloud_environment,
            format='csv',
            **options,
        )

    def json(
        self,
        df,
        environment,
        zone,
        namespace,
        dataset,
        cloud_environment=cloud_config.CLOUD_ENVIRONMENT,
        **options,
    ):
        """
        Used to write files as json
        :param df: :class:`pyspark.sql.DataFrame` instance
        :param environment: string, one of :class:`Environment` values
        :param zone: string, one of :class:`Zone` values
        :param mode: string, one of :class:`Mode` values
        :param namespace: string, name of namespace
        :param dataset: string, name of dataset
        :param version: string, id of the wanted version
        :param version_offset: string, version offset meaning that 0
        is the most recent
        :param options: Spark-native options that can be passed on
        """
        return self.save(
            df,
            environment,
            zone,
            namespace,
            dataset,
            cloud_environment,
            format='json',
            **options,
        )

    def parquet(
        self,
        df,
        environment,
        zone,
        namespace,
        dataset,
        cloud_environment=cloud_config.CLOUD_ENVIRONMENT,
        **options,
    ):
        """
        Used to write files as parquet
        :param df: :class:`pyspark.sql.DataFrame` instance
        :param environment: string, one of :class:`Environment` values
        :param zone: string, one of :class:`Zone` values
        :param mode: string, one of :class:`Mode` values
        :param namespace: string, name of namespace
        :param dataset: string, name of dataset
        :param version: string, id of the wanted version
        :param version_offset: string, version offset meaning that 0
        is the most recent
        :param options: Spark-native options that can be passed on
        """
        return self.save(
            df,
            environment,
            zone,
            namespace,
            dataset,
            cloud_environment,
            format='parquet',
            **options,
        )

    def kafka(self, df, environment, bootstrap_servers, topic_name, **options):
        """
        Used to write files into a kafka topic
        :param df: :class:`pyspark.sql.DataFrame` instance
        :param environment: string, one of :class:`Environment` values
        :param topic_name: Name of topic that data will be written to
        :param bootstrap_servers: Kafka servers in the format
        host1:port1,host2:port2. Default value follows the environment
        parameters. In case of Environment.PRODUCTION it will be
        kafka-data-engineering.gcp.luizalabs.com:9092, otherwise
        kafka-stg.gcp.luizalabs.com:9092
        :param options: Spark-native options that can be passed on

        The Dataframe being written to Kafka should have the following columns
        in schema: key (optional) [Type: string or binary] and
        value (required) [Type: string or binary]

        For more details: https://spark.apache.org/docs/2.2.0/structured-streaming-kafka-integration.html  # noqa
        """
        if not options.get('checkpointLocation'):
            base_checkpoint_location = self._get_bucket_checkpoint_location(
                Environment(environment).value, self.environment_spec
            )
            options[
                'checkpointLocation'
            ] = f'{base_checkpoint_location}/kafka-{topic_name}'
        write = (
            df.writeStream.format("kafka")
            .option("kafka.bootstrap.servers", bootstrap_servers)
            .option("topic", topic_name)
        )

        return write.trigger(once=True).start(**options).awaitTermination()

    def save(
        self,
        df,
        environment,
        zone,
        namespace,
        dataset,
        cloud_environment=cloud_config.CLOUD_ENVIRONMENT,
        **options,
    ):
        """
        Used to write files as specified format
        :param df: :class:`pyspark.sql.DataFrame` instance
        :param environment: string, one of :class:`Environment` values
        :param zone: string, one of :class:`Zone` values
        :param mode: string, one of :class:`Mode` values
        :param namespace: string, name of namespace
        :param dataset: string, name of dataset
        :param version: string, id of the wanted version
        :param version_offset: string, version offset meaning that 0
        is the most recent
        :param options: Spark-native options that can be passed on
        :param cloud_environment: string, target cloud name
        """

        if (
            self._parquet_only_zone(Zone(zone))
            and options.get('format') != 'parquet'
        ):
            raise ValueError(f'Can only write parquet in {Zone(zone).name}')
        self._writer = df.writeStream
        if not options.get('checkpointLocation'):
            options['checkpointLocation'] = self._get_checkpoint_location(
                Environment(environment).value,
                Zone(zone).value,
                namespace,
                dataset,
                cloud_environment,
            )

        path = self._get_path(
            environment,
            zone,
            namespace,
            dataset,
            mode='append',
            write_needed=True,
            cloud_environment=cloud_environment,
        )

        return_value = None
        if delta_operations.is_delta_lake(
            self.spark_session,
            environment,
            zone,
            namespace,
            dataset,
            options,
            cloud_environment=cloud_environment,
        ):

            options['format'] = 'delta'

            # Executing the writeStream
            return_value = (
                df.writeStream.trigger(once=True)
                .start(path=path, **options)
                .awaitTermination()
            )
        elif delta_operations.is_a_new_dataset(
            self.spark_session,
            environment,
            zone,
            namespace,
            dataset,
            options,
            cloud_environment,
        ):

            options['format'] = 'delta'
            return_value = (
                df.writeStream.trigger(once=True)
                .start(path=path, **options)
                .awaitTermination()
            )
        else:
            # Executing the write
            return_value = (
                df.writeStream.trigger(once=True)
                .start(path=path, **options)
                .awaitTermination()
            )
        return return_value

    def _get_bucket_checkpoint_location(
        self,
        env,
        cloud_environment=cloud_config.CLOUD_ENVIRONMENT,
        environment_spec='',
    ):
        if cloud_environment == 'gcp':
            cloud_prefix = 'gs'
        elif cloud_environment == 'mgc':
            cloud_prefix = 's3a'

        if environment_spec != '':
            bucket = f'{env}-cluster-config-{environment_spec}'
        else:
            bucket = f'{env}-cluster-config'
        return f'{cloud_prefix}://{bucket}/checkpoints'

    def _get_checkpoint_location(
        self, env, zone, namespace, dataset, cloud_environment
    ):
        base_path = self._get_bucket_checkpoint_location(
            env, cloud_environment, self.environment_spec
        )
        logging.error("INSIDE _get_checkpoint_location")
        return f'{base_path}/{zone}-{namespace}-{dataset}'


class _SnessDataFrameWriter(_SnessSparkBase):
    def __init__(self, spark_session):
        super().__init__(spark_session)
        self.multi_cloud_write = (
            os.environ['ENVIRONMENT_MULTI_CLOUD_WRITE'].lower()
            in ('true', '1')
            if 'ENVIRONMENT_MULTI_CLOUD_WRITE' in os.environ
            else True
        )

    def csv(
        self,
        df,
        environment,
        zone,
        namespace,
        dataset,
        cloud_environment=cloud_config.CLOUD_ENVIRONMENT,
        **options,
    ):
        """
        Used to writestream files as csv
        :param df: :class:`pyspark.sql.DataFrame` instance
        :param environment: string, one of :class:`Environment` values
        :param zone: string, one of :class:`Zone` values
        :param mode: string, one of :class:`Mode` values
        :param namespace: string, name of namespace
        :param dataset: string, name of dataset
        :param version: string, id of the wanted version
        :param version_offset: string, version offset meaning that 0
        is the most recent
        :param options: Spark-native options that can be passed on
        :param cloud_environment: string, target cloud name
        :param multi_cloud_write: bool, enable write to multiple clouds
        """
        self.save(
            df,
            environment,
            zone,
            namespace,
            dataset,
            cloud_environment,
            self.multi_cloud_write,
            format='csv',
            **options,
        )

    def json(
        self,
        df,
        environment,
        zone,
        namespace,
        dataset,
        cloud_environment=cloud_config.CLOUD_ENVIRONMENT,
        **options,
    ):
        """
        Used to writestream files as json
        :param df: :class:`pyspark.sql.DataFrame` instance
        :param environment: string, one of :class:`Environment` values
        :param zone: string, one of :class:`Zone` values
        :param mode: string, one of :class:`Mode` values
        :param namespace: string, name of namespace
        :param dataset: string, name of dataset
        :param version: string, id of the wanted version
        :param version_offset: string, version offset meaning that 0
        is the most recent
        :param options: Spark-native options that can be passed on
        :param cloud_environment: string, target cloud name
        :param multi_cloud_write: bool, enable write to multiple clouds
        """
        self.save(
            df,
            environment,
            zone,
            namespace,
            dataset,
            cloud_environment,
            self.multi_cloud_write,
            format='json',
            **options,
        )

    def parquet(
        self,
        df,
        environment,
        zone,
        namespace,
        dataset,
        cloud_environment=cloud_config.CLOUD_ENVIRONMENT,
        **options,
    ):
        """
        Used to writestream files as parquet
        :param df: :class:`pyspark.sql.DataFrame` instance
        :param environment: string, one of :class:`Environment` values
        :param zone: string, one of :class:`Zone` values
        :param mode: string, one of :class:`Mode` values
        :param namespace: string, name of namespace
        :param dataset: string, name of dataset
        :param version: string, id of the wanted version
        :param version_offset: string, version offset meaning that 0
        is the most recent
        :param options: Spark-native options that can be passed on
        :param cloud_environment: string, target cloud name
        :param multi_cloud_write: bool, enable write to multiple clouds
        """
        self.save(
            df,
            environment,
            zone,
            namespace,
            dataset,
            cloud_environment,
            self.multi_cloud_write,
            format='parquet',
            **options,
        )

    def kafka(self, df, topic_name, bootstrap_servers, **options):
        """
        Used to write files into a kafka topic
        :param df: :class:`pyspark.sql.DataFrame` instance
        :param environment: string, one of :class:`Environment` values
        :param topic_name: Name of topic that data will be written to
        :param bootstrap_servers: Kafka servers in the format
        host1:port1,host2:port2. Default value follows the environment
        parameters. In case of Environment.PRODUCTION it will be
        kafka-data-engineering.gcp.luizalabs.com:9092, otherwise
        kafka-stg.gcp.luizalabs.com:9092
        :param options: Spark-native options that can be passed on

        The Dataframe being written to Kafka should have the following columns
        in schema: key (optional) [Type: string or binary] and
        value (required) [Type: string or binary]

        For more details: https://spark.apache.org/docs/2.2.0/structured-streaming-kafka-integration.html  # noqa
        """
        write = (
            df.write.format("kafka")
            .option("kafka.bootstrap.servers", bootstrap_servers)
            .option("topic", topic_name)
        )

        return write.save(**options)

    def save(
        self,
        df,
        environment,
        zone,
        namespace,
        dataset,
        cloud_environment=cloud_config.CLOUD_ENVIRONMENT,
        multi_cloud_write=True,
        **options,
    ):
        """
        Used to write files as specified format
        :param df: :class:`pyspark.sql.DataFrame` instance
        :param environment: string, one of :class:`Environment` values
        :param zone: string, one of :class:`Zone` values
        :param mode: string, one of :class:`Mode` values
        :param namespace: string, name of namespace
        :param dataset: string, name of dataset
        :param version: string, id of the wanted version
        :param version_offset: string, version offset meaning that 0
        is the most recent
        :param options: Spark-native options that can be passed on
        :param cloud_environment: string, target cloud name
        :param multi_cloud_write: bool, enable write to multiple clouds
        """
        # ==========================================
        # CONFIGURAÇÃO DATABRICKS SERVERLESS
        # ==========================================
        if os.environ.get("SNESS_DATABRICKS_SERVERLESS") == "true":

            base_saida = (
                "/Volumes/test/default/"
                "dados_origin_visao_unica/resultados"
            )

            if not isinstance(dataset, str) or not dataset:
                raise ValueError("Nome de dataset inválido")

            if (
                "/" in dataset
                or "\\" in dataset
                or dataset in (".", "..")
            ):
                raise ValueError(
                    f"Dataset inválido: {dataset}"
                )

            caminho = f"{base_saida}/{dataset}"
            print(f"[SNESS WRITE REAL] dataset={dataset} | path={caminho}", flush=True)

            # Opções específicas da biblioteca Sness
            # que não pertencem ao DataFrameWriter Spark.
            opcoes_sness = {
                "delta",
                "changelog_columns",
                "changelog_keys",
                "delta_retention_hours",
            }

            opcoes_spark = {
                chave: valor
                for chave, valor in options.items()
                if chave not in opcoes_sness
            }

            formato = opcoes_spark.pop("format", "parquet")
            modo = opcoes_spark.pop("mode", "errorifexists")

            # Compatibilidade com Databricks Serverless / Spark Connect
            if modo.lower() in ("errorifexists", "error"):
                modo = "error"

            # Proteção adicional contra escrita na origem
            if not caminho.startswith(base_saida + "/"):
                raise ValueError(
                    "Gravação fora do diretório de resultados"
                )

            (
                df.write
                .format(formato)
                .mode(modo)
                .options(**opcoes_spark)
                .save(caminho)
            )

            return
        
        # ==========================================
        # CONFIGURAÇÃO ORIGINAL MAGALU / GCP
        # ==========================================

        if (
            self._parquet_only_zone(Zone(zone))
            and options.get('format') != 'parquet'
        ):
            raise ValueError(f'Can only write parquet in {Zone(zone).name}')
        version = None
        if options.get('mode') == 'overwrite':
            version = self._build_version()

        def save_to_cloud(
            self,
            df,
            environment,
            zone,
            namespace,
            dataset,
            cloud_environment=cloud_config.CLOUD_ENVIRONMENT,
            **options,
        ):
            path = self._get_path(
                environment,
                zone,
                namespace,
                dataset,
                version=version,
                write_needed=True,
                cloud_environment=cloud_environment,
            )
            if cloud_environment == 'gcp':
                try:
                    mm = Monitoring(
                        self.spark_session,
                        environment,
                        zone,
                        namespace,
                        'dataset_quality_health',
                        dataset,
                    )
                    if mm.enable_monitoring:
                        mm.monitor_dataset(df)
                except Exception as msg:
                    logging.error(
                        f'sness_spark: Error in monitoring dataset:\
                            {dataset}: {str(msg)}'
                    )
            try:
                if options.get('mode') == 'overwrite':
                    if options.get("changelog_columns", None):
                        if options.get("changelog_keys", None):
                            cl = Changelogs(
                                spark_session=self.spark_session,
                                environment=environment,
                                zone=zone,
                                namespace=namespace,
                                dataset=dataset,
                                changelog_keys=options.get(
                                    "changelog_keys", None
                                ),  # noqa
                                changelog_columns=options.get(
                                    "changelog_columns", None
                                ),  # noqa
                                cloud_environment=cloud_environment,
                            )
                            cl.register_changelog(df, cloud_environment)
                        else:
                            logging.error(
                                'To use "changelog_columns" you must '  # noqa
                                'inform also a value for the '
                                '"changelog_keys" parameter'
                            )
            except Exception as msg:
                logging.error(
                    f'sness_spark: Error in data changelog:\
                                    {dataset}: {str(msg)}'
                )
            if delta_operations.is_delta_lake(
                self.spark_session,
                environment,
                zone,
                namespace,
                dataset,
                options,
                cloud_environment,
            ):

                options['format'] = 'delta'

                if options.get('mode') == 'overwrite':
                    delta_operations.vacuum(
                        spark_session=self.spark_session,
                        environment=environment,
                        zone=zone,
                        namespace=namespace,
                        dataset=dataset,
                        retention_hours=options.get(
                            'delta_retention_hours', 12
                        ),
                        cloud_environment=cloud_environment,
                    )
                ds = DatasetMetadata(namespace, dataset)
                path = ds.build_lake_path(
                    environment=environment,
                    zone=zone,
                    cloud_environment=cloud_environment,
                )

            if delta_operations.is_a_new_dataset(
                self.spark_session,
                environment,
                zone,
                namespace,
                dataset,
                options,
                cloud_environment,
            ):

                options['format'] = 'delta'
                ds = DatasetMetadata(namespace, dataset)
                path = ds.build_lake_path(
                    environment=environment,
                    zone=zone,
                    cloud_environment=cloud_environment,
                )

            df.write.save(path=path, **options)

            if options.get(
                "format"
            ) == "parquet" and convert_namespace_to_delta(namespace):
                delta_operations.convert_to_delta(
                    self.spark_session,
                    environment,
                    zone,
                    namespace,
                    dataset,
                    cloud_environment,
                )

            zone = zone if type(zone) == str else zone.value  # noqa

            try:
                metadata_operations.save_metadata(
                    self.spark_session,
                    df,
                    environment,
                    zone,
                    namespace,
                    dataset,
                    path,
                    options,
                )
            except Exception as msg:
                logging.error(f"Error in save metadata: {dataset}: {str(msg)}")

        if cloud_environment == 'mgc':
            if multi_cloud_write:
                save_to_cloud(
                    self,
                    df,
                    environment,
                    zone,
                    namespace,
                    dataset,
                    'mgc',
                    **options,
                )
                save_to_cloud(
                    self,
                    df,
                    environment,
                    zone,
                    namespace,
                    dataset,
                    'gcp',
                    **options,
                )
            else:
                save_to_cloud(
                    self,
                    df,
                    environment,
                    zone,
                    namespace,
                    dataset,
                    'mgc',
                    **options,
                )
        else:
            save_to_cloud(
                self,
                df,
                environment,
                zone,
                namespace,
                dataset,
                'gcp',
                **options,
            )


class _SnessDataFrameDeltaOperations(_SnessSparkBase):
    @staticmethod
    def require_min_delta_version(min_version: str = "3.3.0"):
        def decorator(func):
            @wraps(func)
            def wrapper(self, *args, **kwargs):
                import delta

                current_version_str = delta.__version__

                def _parse_version(v: str) -> tuple:
                    return tuple(
                        int(part) for part in v.split('.') if part.isdigit()
                    )

                if _parse_version(current_version_str) < _parse_version(
                    min_version
                ):
                    raise RuntimeError(
                        f"Unsupported Delta version. "
                        f"Method '{func.__name__}' requires Delta >= {min_version}, "
                        f"but found version {current_version_str}."
                    )

                return func(self, *args, **kwargs)

            return wrapper

        return decorator

    @require_min_delta_version("3.3.0")
    @_SnessSparkBase.resolve_path
    def optmize(
        self,
        path: str,
        clustered_columns: Optional[list[str]] = None,
    ):
        """Example:
        >>> ss.deltaOperation.optmize(
        ...     environment="prd",
        ...     zone="trusted",
        ...     namespace="ecommerce",
        ...     dataset="orders",
        ...     clustered_columns=["order_date", "store_id"]
        ... )"""

        execute_optimize(self.spark_session, path, clustered_columns)

    @require_min_delta_version("3.3.0")
    @_SnessSparkBase.resolve_path
    def cluster_by_columns(
        self,
        path: str,
        columns: str | Iterable[str],
    ):
        """Example:
        >>> ss.deltaOperation.cluster_by_columns(
        ...     environment="prd",
        ...     zone="trusted",
        ...     namespace="ecommerce",
        ...     dataset="orders",
        ...     columns=["order_date", "store_id"]
        ... )"""

        execute_clustering(self.spark_session, path, columns)

    @require_min_delta_version("3.3.0")
    @_SnessSparkBase.resolve_path
    def is_clustered(
        self,
        path: str,
        columns: str | Iterable[str],
    ):
        """Example:
        >>> ss.deltaOperation.is_clustered(
        ...     environment="prd",
        ...     zone="trusted",
        ...     namespace="ecommerce",
        ...     dataset="orders",
        ...     columns=["order_date", "store_id"]
        ... )"""

        check_if_is_clustered(self.spark_session, path, columns)
