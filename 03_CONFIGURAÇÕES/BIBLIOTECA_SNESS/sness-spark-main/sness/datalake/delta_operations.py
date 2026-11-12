import os
import json
import time
import logging
import subprocess
from typing import TYPE_CHECKING, Iterable, Optional
import pyspark

from sness.settings import delta_config
from sness.settings import cloud_config
from sness.datalake.metadata import DatasetMetadata, Mode
from pyspark.sql.functions import split, expr, col, input_file_name
from sness.storage import (
    check_if_file_exists,
    remove_file,
    count_files_on_manifest,
    mark_dataset_to_repartition,
    get_list_of_files_in_manifest,
)

# The IDE sees this, but it NEVER runs at execution time
if TYPE_CHECKING:
    from delta.tables import DeltaTable


@delta_config.ensure_delta_table
def get_partitions_from_dataframe(
    spark_session, path, cloud_environment=cloud_config.CLOUD_ENVIRONMENT
):
    """ "
    Get the partitions and types of a spark DataFrame.
    Return value is a list of dicts with the following structure:
    [
        {
            "name": "partition_name",
            "type": "partition_type",
            "nullable": True or False,
            "metadata": {...}
        }
    ]

    If you just wanna to know the partitions name in a Delta Lake
    dataset, you can use the following code because it's less costly:

    ```
    from sness.datalake.delta_files import DeltaFile
    from sness.datalake.metadata import DatasetMetadata

    ds = DatasetMetadata(namespace=namespace, dataset=dataset)
    path = ds.build_lake_path(environment, zone)
    delta_file = DeltaFile(path)
    partitions = delta_file.get_partitions()
    print(partitions)
    ```

    :param spark_session: SparkSession
    :param path: path of the dataset
    :param cloud_environment: string, target cloud name
    :return: list of dict with partitions
    """

    df = None
    if DeltaTable.isDeltaTable(spark_session, path):
        df = spark_session.read.format("delta").load(path)
    else:
        df = spark_session.read.parquet(path)

    dataframe_schema = json.loads(df.schema.json())['fields']

    if len(dataframe_schema) == 0:
        raise Exception(f"DataFrame schema of `{path}` is empty.")

    partitions = []
    if len(df.take(1)) > 0:
        partitions = (
            df.withColumn('_filepath', input_file_name())
            .select(col('_filepath'))  # noqa
            .withColumn(
                '_filepath_splitted', split(col('_filepath'), r'(\/)')
            )  # noqa
            .withColumn(
                '_partitions',
                expr("filter(_filepath_splitted, x -> x like '%=%')"),
            )  # noqa
            .withColumn(
                '_partitionBy',
                expr(
                    "transform(_partitions, x -> substring_index(x, '=', 1))"
                ),
            )  # noqa
            .select(['_filepath', '_partitionBy'])
            .select('_partitionBy')
            .distinct()
            .take(1)[0]['_partitionBy']
        )

    return [field for field in dataframe_schema if field['name'] in partitions]


@delta_config.ensure_delta_table
def convert_to_delta(
    spark_session,
    environment,
    zone,
    namespace,
    dataset,
    cloud_environment=cloud_config.CLOUD_ENVIRONMENT,
):
    """
    Method used to convert a parquet dataset into a DeltaLake format
    :param spark_session: SparkSession
    :param environment: str from ENUM Environment.<env>
    :param zone: str from ENUM Zone.<zone>
    :param namespace: name of the namespace
    :param dataset: name of the dataset
    """

    try:
        ds = DatasetMetadata(namespace, dataset)
        path = ds.build_lake_path(
            environment=environment,
            zone=zone,
            cloud_environment=cloud_environment,
        )

        if cloud_environment == 'gcp':
            if not DeltaTable.isDeltaTable(spark_session, path):
                try:
                    print("Converting dataset to DeltaLake")
                    bucket_lake = path  # noqa

                    os.system(
                        f"gsutil -m rm -r "
                        f"gs://{environment}-lake-backup/{namespace}_{dataset}_{zone}/*"  # noqa
                    )
                    return_value = os.system(
                        f"gsutil -m cp -r {path}* "
                        f"gs://{environment}-lake-backup/{namespace}_{dataset}_{zone}/"  # noqa
                    )
                    if return_value != 0:
                        raise Exception(
                            "Error copying old data to backup. Check write permissions"  # noqa
                            f" in the bucket gs://{environment}-lake-backup."
                        )
                    try:
                        ds.build_lake_path(
                            environment=environment,
                            zone=zone,
                            cloud_environment=cloud_environment,
                            mode=Mode.OVERWRITE,
                        )
                        print(
                            "Dataset in OVERWRITE mode. "
                            "Moving data to the root of the dataset."
                        )
                        os.system(f"gsutil -m rm -r {bucket_lake}*")
                        os.system(
                            f"gsutil -m cp -r "
                            f"gs://{environment}-lake-backup/{namespace}_{dataset}_{zone}/* "  # noqa
                            f"{bucket_lake}"
                        )
                    except Exception:
                        print("Dataset not in OVERWRITE mode.")

                    partitions = get_partitions_from_dataframe(
                        spark_session=spark_session,
                        path=bucket_lake,
                        cloud_environment=cloud_environment,
                    )
                    if len(partitions) > 0:
                        print(f"Partitions found: {partitions}")
                        partition_by = ','.join(
                            [
                                f"{partition['name']} {partition['type']}"
                                for partition in partitions
                            ]
                        )
                        DeltaTable.convertToDelta(
                            spark_session,
                            f"parquet.`{bucket_lake}`",
                            partition_by,  # noqa
                        )
                    else:
                        print("No partitions found")
                        DeltaTable.convertToDelta(
                            spark_session, f"parquet.`{bucket_lake}`"
                        )
                    print("Dataset converted")
                    return True
                except Exception as msg:
                    print(f"Fail when converting to DeltaLake: {msg}")
                    return False
            return False
        elif cloud_environment == 'mgc':
            if not DeltaTable.isDeltaTable(spark_session, path):
                try:
                    print("Converting dataset to Deltalake")
                    bucket_lake = (
                        f"container:{path.replace('s3a://', '')}/"  # noqa
                    )

                    os.system(
                        f"rclone purge "
                        f"container:{environment}-lake-backup/{namespace}_{dataset}_{zone}/"  # noqa
                    )

                    return_value = os.system(
                        f"rclone copy container:{path} "
                        f"container:{environment}-lake-backup/{namespace}_{dataset}_{zone}/"  # noqa
                    )

                    if return_value != 0:
                        raise Exception(
                            "Error copying old data to backup. Check write permissions"  # noqa
                            f" in the bucket gs://{environment}-lake-backup."
                        )
                    try:
                        ds.build_lake_path(
                            environment=environment,
                            zone=zone,
                            cloud_environment=cloud_environment,
                            mode=Mode.OVERWRITE,
                        )
                        print(
                            "Dataset in OVERWRITE mode. "
                            "Moving data to the root of the dataset."
                        )

                        os.system(f"rclone purge {bucket_lake}")
                        os.system(
                            f"rclone copy container:{environment}-lake-backup/{namespace}_{dataset}_{zone}/ "  # noqa
                            f"{bucket_lake}"
                        )
                    except Exception:
                        print("Dataset not in OVERWRITE mode.")

                    partitions = get_partitions_from_dataframe(
                        spark_session=spark_session,
                        path=bucket_lake,
                        cloud_environment=cloud_environment,
                    )
                    if len(partitions) > 0:
                        print(f"Partitions found: {partitions}")
                        partition_by = ','.join(
                            [
                                f"{partition['name']} {partition['type']}"
                                for partition in partitions
                            ]
                        )
                        DeltaTable.convertToDelta(
                            spark_session,
                            f"parquet.`{bucket_lake}`",
                            partition_by,  # noqa
                        )
                    else:
                        print("No partitions found")
                        DeltaTable.convertToDelta(
                            spark_session, f"parquet.`{bucket_lake}`"
                        )
                    print("Dataset converted")
                    return True
                except Exception as msg:
                    print(f"Fail when converting to DeltaLake: {msg}")
                    return False
            return False
    except Exception as err:
        print(f"Fail when converting to DeltaLake: {err}")
        return False


@delta_config.ensure_delta_table
def is_delta_lake(
    spark_session,
    environment,
    zone,
    namespace,
    dataset,
    options,
    cloud_environment=cloud_config.CLOUD_ENVIRONMENT,
):
    # CONGIURAÇÃO CLOUD OU AMBIENTE MAGALU
    #"""
    #Method used to identify if the dataset is in Delta Lake format
    #:param spark_session: SparkSession
    #:param environment: str from ENUM Environment.<env>
    #:param zone: str from ENUM Zone.<zone>
    #:param namespace: name of the namespace
    #:param dataset: name of the dataset
    #:param options: dict of properties of the operation
    #:return: True if is DeltaLake, False otherwise
    #"""
    #if options.get("format") == "parquet" and options.get("delta", True):
    #
    #    ds = DatasetMetadata(namespace, dataset)
    #    path = ds.build_lake_path(
    #        environment=environment,
    #        zone=zone,
    #        cloud_environment=cloud_environment,
    #    )
    #
    #    # Sets cloud_environment based on the value of prefix
    #    cloud_environment = 'gcp' if path.split('://')[0] == 'gs' else 'mgc'
    #    if DeltaTable.isDeltaTable(spark_session, path):
    #        return True
    #return False

    # CONFIGURAÇÃO DATABRICKS
    """
    Identifica datasets Delta Lake.
    Databricks Serverless:
        utiliza os caminhos do Unity Catalog Volumes.
    Ambiente original:
        preserva o comportamento do Sness.
    """
    import os

    if (
        options.get("format") == "parquet"
        and options.get("delta", True)
    ):

        if os.environ.get("SNESS_DATABRICKS_SERVERLESS") == "true":

            if not isinstance(dataset, str) or not dataset:
                raise ValueError("Nome de dataset inválido")

            if "/" in dataset or "\\" in dataset or dataset in (".", ".."):
                raise ValueError(f"Dataset inválido: {dataset}")

            base = (
                "/Volumes/workspace/default/"
                "dados_teste_visao_unica"
            )
            caminho_entrada = f"{base}/parquet/{dataset}"
            caminho_saida = f"{base}/resultados/{dataset}"

            # Mesma regra de leitura do _get_path()
            if os.path.isdir(caminho_saida):
                path = caminho_saida
            else:
                path = caminho_entrada

            return DeltaTable.isDeltaTable(spark_session, path)

        # COMPORTAMENTO ORIGINAL DO SNESS
        ds = DatasetMetadata(namespace, dataset)

        path = ds.build_lake_path(
            environment=environment,
            zone=zone,
            cloud_environment=cloud_environment,
        )

        cloud_environment = (
            "gcp" if path.split("://")[0] == "gs" else "mgc"
        )

        if DeltaTable.isDeltaTable(spark_session, path):
            return True

    return False


@delta_config.ensure_delta_table
def vacuum(
    spark_session,
    environment,
    zone,
    namespace,
    dataset,
    retention_hours=12,
    cloud_environment=cloud_config.CLOUD_ENVIRONMENT,
):
    """
    Method used to delete files not required by versions more than 168 hours
    old
    :param spark_session: SparkSession
    :param environment: str from ENUM Environment.<env>
    :param zone: str from ENUM Zone.<zone>
    :param namespace: name of the namespace
    :param dataset: name of the dataset
    :param retention_hours: maximum retention hours for older versions of the
    files (default 168 hours)
    """

    start_time = time.time()

    ds = DatasetMetadata(namespace, dataset)
    path = ds.build_lake_path(
        environment=environment, zone=zone, cloud_environment=cloud_environment
    )
    DeltaTable.forPath(spark_session, f"{path}").vacuum(retention_hours)
    print(
        ">> vacuum function takes %s seconds to execute"
        % round(time.time() - start_time)
    )


def compact_files(
    spark_session,
    environment,
    zone,
    namespace,
    dataset,
    repartition_num=None,
    cloud_environment=cloud_config.CLOUD_ENVIRONMENT,
):
    """
    Method used to compact files in Delta Lake.
    https://docs.delta.io/latest/best-practices.html#compact-files

    :param spark_session: SparkSession
    :param environment: str from ENUM Environment.<env>
    :param zone: str from ENUM Zone.<zone>
    :param namespace: name of the namespace
    :param dataset: name of the dataset
    """
    start_time = time.time()

    ds = DatasetMetadata(namespace=namespace, dataset=dataset)
    path = ds.build_lake_path(
        environment=environment, zone=zone, cloud_environment=cloud_environment
    )
    path = path if not path.endswith("/") else path[:-1]

    # Change to only string manipulation
    partitions = []
    if (
        check_if_file_exists(path + '/_symlink_format_manifest/'),
        cloud_environment,
    ):
        # GET list of files with symlink_format_manifest
        file_list = get_list_of_files_in_manifest(path, cloud_environment)
        partitions_found = set()
        for filepath in file_list:
            for each in filepath.replace(path, '').split('/')[:-1]:
                if '=' in each:
                    partitions_found.add(next(iter(each.split('='))))
        partitions = list(partitions_found)
    else:
        # Get partitions with spark
        partitions = get_partitions_from_dataframe(
            spark_session=spark_session, path=path
        )
        partitions = [x['name'] for x in partitions]

    df = spark_session.read.format("delta").load(path)

    if repartition_num is not None:
        # Static repartition
        print(f'>> Number of partitions: {repartition_num}')
        df = df.repartition(repartition_num)
    elif len(partitions) > 0:
        # Repartition by partitions path
        print(f'>> Repartition by: {partitions}')
        df = df.repartition(*partitions)
    else:
        # Auto repartition by number of records by each 1000000
        number_of_records = df.count()
        print(f'>> Number of records: {number_of_records}')
        num_of_partitions = int(number_of_records / 1000000) + 1
        print(f'>> Number of partitions: {num_of_partitions}')
        df = df.repartition(num_of_partitions)
    (
        df.write.option("dataChange", "false")
        .format("delta")
        .mode("overwrite")
        .save(path)
    )

    print(
        ">> compact_files function takes %s seconds to execute"
        % round(time.time() - start_time)
    )


def compact_dataset(
    spark_session,
    environment,
    zone,
    namespace,
    dataset,
    cloud_environment=cloud_config.CLOUD_ENVIRONMENT,
):
    """
    Method used to check if the dataset have _TOO_MANY_FILES flag set.
    and call compact_files function if it is the case.

    :param spark_session: SparkSession
    :param environment: str from ENUM Environment.<env>
    :param zone: str from ENUM Zone.<zone>
    :param namespace: name of the namespace
    :param dataset: name of the dataset

    :return: True if the dataset need was compacted, False otherwise
    """

    path = DatasetMetadata(namespace, dataset).build_lake_path(
        environment=environment, zone=zone, cloud_environment=cloud_environment
    )

    # remove slash from path
    path = path if not path.endswith("/") else path[:-1]
    # Check if _TOO_MANY_FILES exists
    marked_filepath = f'{path}/_TOO_MANY_FILES'
    too_many_files = check_if_file_exists(marked_filepath, cloud_environment)

    if too_many_files:
        # This delta lake function
        # automatic detect hive partitions with
        # dataChange = false
        logging.info(
            'Detected _TOO_MANY_FILES file. ' 'Compacting delta lake files.'
        )
        compact_files(
            spark_session=spark_session,
            environment=environment,
            zone=zone,
            namespace=namespace,
            dataset=dataset,
            cloud_environment=cloud_environment,
        )

        # Remove _TOO_MANY_FILES
        remove_file(marked_filepath)

        return True
    return False


@delta_config.ensure_delta_table
def check_and_mark_with_too_many_files(
    spark_session,
    environment,
    zone,
    namespace,
    dataset,
    max_allowed_files=2,
    cloud_environment=cloud_config.CLOUD_ENVIRONMENT,
):
    """
    Function used to check if the dataset have more than 10000
    or `max_allowed_files` files in the last version of the
    dataset in Delta Lake format. If it is the case, the
    function will create a _TOO_MANY_FILES file in the
    dataset folder.

    :param spark_session: SparkSession
    :param environment: str from ENUM Environment.<env>
    :param zone: str from ENUM Zone.<zone>
    :param namespace: name of the namespace
    :param dataset: name of the dataset
    :param cloud_environment: string, target cloud name
    :param max_allowed_files: maximum number of files allowed int the last
        version of the dataset in Delta Lake format

    :return: True if the dataset need was compacted, False otherwise
    """

    path = DatasetMetadata(namespace, dataset).build_lake_path(
        environment=environment, zone=zone, cloud_environment=cloud_environment
    )
    # remove slash from path
    path = path if not path.endswith("/") else path[:-1]

    if not DeltaTable.isDeltaTable(spark_session, path):
        raise ValueError(f"Dataset {dataset} is not in Delta Lake format.")

    DeltaTable.forPath(spark_session, path).generate("symlink_format_manifest")
    count_files = count_files_on_manifest(
        path=path, cloud_environment=cloud_environment
    )

    if count_files > max_allowed_files:
        return mark_dataset_to_repartition(path, cloud_environment)
    else:
        return False


@delta_config.ensure_delta_table
def get_last_version_of_dataset(
    spark_session,
    environment,
    zone,
    namespace,
    dataset,
    cloud_environment=cloud_config.CLOUD_ENVIRONMENT,
):
    """
    Method used to identify the last valid version of a dataset

    :param spark_session: SparkSession
    :param environment: str from ENUM Environment.<env>
    :param zone: str from ENUM Zone.<zone>
    :param namespace: name of the namespace
    :param dataset: name of the dataset

    :return: Number of the version
    """

    ds = DatasetMetadata(namespace, dataset)
    path = ds.build_lake_path(
        environment=environment, zone=zone, cloud_environment=cloud_environment
    )
    try:
        last_version = (
            DeltaTable.forPath(spark_session, f"{path}")
            .history()
            .collect()[0][0]
        )
        return int(last_version)
    except Exception:
        return 0


def is_a_new_dataset(
    spark_session,
    environment,
    zone,
    namespace,
    dataset,
    options,
    cloud_environment=cloud_config.CLOUD_ENVIRONMENT,
):
    """
    Temporary method used to identify if the dataset that is been writing
    is new or already exists.
    After all data lake has been converted to Delta Lake, this method could
    me removed
    :param options: dict of properties of the operation
    :param cloud_environment: string, target cloud name
    :return: True for new datasets and False for datasets that already exists
    """
    if options.get("format") == "parquet" and options.get("delta", True):
        ds = DatasetMetadata(namespace, dataset)
        path = ds.build_lake_path(
            environment=environment,
            zone=zone,
            cloud_environment=cloud_environment,
        )
        if not check_if_file_exists(path, cloud_environment):
            return True
    return False


def execute_cmd(cmd):
    """
    :param cmd: Shell command
    :return: The result of the shell command
    """

    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True
    )
    output, error = proc.communicate()
    if output:
        return output.strip()
    if error or proc.returncode != 0:
        raise Exception(f'Error> error {error} ')


def sync_merged_files_from_mgc_to_gcp(
    ss, environment, zone, namespace, dataset
):
    """
    This method will load the last delta_log file and
    verify if every parquet file will be present at mgc storage
    After the full check off files, will read full
    dataset mgc and write full at gcp
    :param ss: Sness Spark object
    :param environment: str from ENUM Environment.<env>
    :param zone: str from ENUM Zone.<zone>
    :param namespace: str
    :para dataset: str
    """
    namespace_mgc = namespace.replace("_", "-")
    namespace_gcp = namespace.replace("-", "_")

    print('>> start read/write from mgc to gcp')
    df_mgc = ss.read.parquet(
        environment=environment,
        zone=zone,
        namespace=namespace_mgc,
        dataset=dataset,
        cloud_environment=cloud_config.CLOUD_ENVIRONMENT,
    )
    ss.write.parquet(
        df=df_mgc,
        environment=environment,
        zone=zone,
        namespace=namespace_gcp,
        dataset=dataset,
        cloud_environment='gcp',
        mode='overwrite',
    )


def enable_auto_compaction(spark: pyspark.sql.SparkSession, table_path: str):
    spark.sql(
        f"""ALTER TABLE delta.`{table_path}`
    SET TBLPROPERTIES (
    'delta.autoOptimize.autoCompact' = TRUE
    );"""
    )


@delta_config.ensure_delta_table
def execute_optimize(
    spark: pyspark.sql.SparkSession,
    table_path: str,
    columns: Optional[list[str]] = None,
):
    enable_auto_compaction(spark, table_path)
    spark.sql(f"OPTIMIZE delta.`{table_path}`")

    delta_table = DeltaTable.forPath(spark, table_path)

    if columns and len(columns) == 1:
        delta_table.optimize().executeZOrderBy(columns)
    else:
        delta_table.optimize().executeCompaction()

    delta_table.vacuum(0)


def execute_clustering(
    spark: pyspark.sql.SparkSession,
    table_path: str,
    columns: str | Iterable[str],
):

    if (
        not isinstance(columns, str)
        or not isinstance(columns, Iterable)
        or isinstance(columns, dict)
    ):
        raise TypeError(
            '`columns` must be a string or a iterable (list, tuple or set)'
        )
    if isinstance(columns, str):
        columns = (
            [i.strip() for i in columns.split(',')]
            if ',' in columns
            else [columns]
        )

    spark.sql(
        f"ALTER TABLE delta.`{table_path}` CLUSTER BY ({', '.join(columns)})"
    )
    execute_optimize(spark, table_path, columns)


@delta_config.ensure_delta_table
def check_if_is_clustered(
    spark: pyspark.sql.SparkSession,
    table_path: str,
    columns: Optional[list[str]] = None,
) -> bool:
    detail = DeltaTable.forPath(spark, table_path).detail()

    if "clusteringColumns" not in detail.columns:
        return False

    clustering_columns = (
        detail.select("clusteringColumns").first()["clusteringColumns"] or []
    )

    clustering_columns = list(
        dict.fromkeys(
            column
            for sublist in clustering_columns
            if sublist
            for column in sublist
        )
    )

    if not clustering_columns:
        return False

    return not columns or set(clustering_columns) == set(columns)
