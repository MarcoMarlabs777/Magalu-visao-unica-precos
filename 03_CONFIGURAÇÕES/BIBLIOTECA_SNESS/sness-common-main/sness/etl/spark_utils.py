import json
import sness.settings
from pyspark.sql.functions import lit, concat, trim, col, sha2
from unicodedata import normalize, category
from google.cloud import storage
from sness.settings import cloud_config

sness_properties = None


def load_sness_properties(cloud_environment=cloud_config.CLOUD_ENVIRONMENT):
    global sness_properties
    if not sness_properties:
        import os

        settings_path = os.path.dirname(sness.settings.__file__)
        with open(f'{settings_path}/config.json') as f:
            sness_properties = json.loads(f.read())
    if cloud_environment == 'mgc':
        import os

        try:
            sness_properties = {
                "bucket": os.environ['BUCKET'],
                "spark_image": os.environ['SPARK_IMAGE'],
                "spark_version": os.environ['SPARK_VERSION'],
            }
        except Exception as ex:
            raise Exception(
                f"Env variables BUCKET,SPARK_IMAGE,SPARK_VERSION\
             need to be specified at airflow \n {ex}"
            )
    return sness_properties


def download_from_gcs(path):
    """
    Used to download files from gcs
    :param path: Path to file in gcs
    :return str:
    """
    storage_client = storage.Client()
    bucket_name = path.split('/')[2]
    blob_path = '/'.join(path.split('/')[3:])

    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_path)

    return blob.download_as_string()


def download_json_from_gcs(path):
    """
    Used to download files from gcs
    :param path: Path to file in gcs
    :return dict:
    """
    return json.loads(download_from_gcs(path))


def hash_column(df, columns):
    """
    Used to hash columns
    :param df: :class:`pyspark.sql.DataFrame` instance
    :param columns: list, columns to hash
    :return df: :class:`pyspark.sql.DataFrame` instance
    """
    salt = download_from_gcs('gs://prd-sec/salt.txt')
    for column in columns:
        df = df.withColumn(column, trim(col(column)))
        df = df.withColumn(column, concat(column, lit(salt)))
        df = df.withColumn(column, sha2(column, 256))
    return df


def remove_accents(str_input):
    """
    Remove accents of the string
    """
    if not str_input:
        return ''

    return ''.join(
        (c for c in normalize('NFD', str_input) if category(c) != 'Mn')
    )


def union_dfs(left_df, right_df):
    """
    Union between two dataFrames, if there is a gap of column fields,
     it will append all missing columns as nulls
    """
    if not left_df:
        raise ValueError('leftDF cannot be None')
    if not right_df:
        raise ValueError('rightDF cannot be None')
    left_df_columns = set(left_df.schema.names)
    right_df_columns = set(right_df.schema.names)
    right_missing_columns = list(left_df_columns - right_df_columns)
    left_missing_columns = list(right_df_columns - left_df_columns)
    return __order_and_union_dfs(
        left_df, right_df, left_missing_columns, right_missing_columns
    )


def __order_df_and_add_missing_cols(df, columns_list, missing_fields):
    """
    return ordered dataFrame by the columns order
    list with null in missing columns
    """
    if not missing_fields:
        return df.select(columns_list)
    else:
        columns = []
        for column in columns_list:
            if column not in missing_fields:
                columns.append(column)
            else:
                columns.append(lit(None).alias(column))
        return df.select(columns)


def __add_missing_columns(df, missing_columns):
    """
    Add missing columns as null in the end of the columns list
    """
    list_missing_columns = []
    for column in missing_columns:
        list_missing_columns.append(lit(None).alias(column))

    return df.select(df.schema.names + list_missing_columns)


def __order_and_union_dfs(
    left_df, right_df, left_missing_columns, right_missing_columns
):
    """
    return union of data frames with ordered columns by leftDF
    """
    left_df_columns = __add_missing_columns(left_df, left_missing_columns)
    right_df_columns = __order_df_and_add_missing_cols(
        right_df, left_df_columns.schema.names, right_missing_columns
    )
    return left_df_columns.union(right_df_columns)
