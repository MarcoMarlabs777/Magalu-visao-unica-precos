import re
import os
import logging
import requests
from enum import Enum, unique
from google.cloud import storage
from sness.storage import check_if_file_exists
from sness.cloud.s3_session import S3Session

from sness.settings import cloud_config
from sness.settings.secret import get_var_secret_manager


def init_s3_session():
    s3_session = S3Session()
    s3 = s3_session.boto3_session()
    return s3


@unique
class Zone(Enum):
    """
    Enum class that represents sness data lake avaiable zones
    """

    TRANSIENT = 'transient'
    RAW = 'raw'
    TRUSTED = 'trusted'
    REFINED = 'refined'
    FEATURE = 'feature'
    SAFE = 'safe'
    WORK = 'work'

    @classmethod
    def _missing_(cls, key):
        zones = ', '.join([zone.name for zone in list(Zone)])
        raise ValueError(f'Zone can only be: {zones}')


@unique
class Environment(Enum):
    """
    Enum class that represents sness data lake avaiable environments
    """

    STAGING = 'stg'
    PRODUCTION = 'prd'

    @classmethod
    def _missing_(cls, key):
        envs = ', '.join([env.name for env in list(Environment)])
        raise ValueError(f'Environment can only be: {envs}')


@unique
class Mode(Enum):
    """
    Enum class that represents sness data lake avaiable write modes
    """

    APPEND = 'append'
    OVERWRITE = 'overwrite'

    @classmethod
    def _missing_(cls, key):
        modes = ', '.join([mode.name for mode in list(Mode)])
        raise ValueError(f'Mode can only be: {modes}')


@unique
class Cloud_Environment(Enum):
    """
    Enum class that represents the cloud used
    """

    GOOGLE = 'gcp'
    MAGACLOUD = 'mgc'

    @classmethod
    def _missing_(cls, key):
        cloud_environment = ', '.join(
            [
                cloud_environment.name
                for cloud_environment in list(Cloud_Environment)
            ]
        )
        raise ValueError(f'Cloud_Environment can only be: {cloud_environment}')


class DatasetMetadata:
    """
    Class used to centralize DatasetMetadata, one could instantiate
    this and use all along the ETL process building many lake paths
    using the same namespace/dataset, which helps in consistency and
    less repetitive coding

    :param namespace: string, namespace of the dataset
    :param dataset: string, name of the dataset itself
    """

    def __init__(self, namespace, dataset):
        self.namespace = namespace
        self.dataset = dataset
        self.environment_spec = (
            os.environ['ENVIRONMENT_SPEC']
            if 'ENVIRONMENT_SPEC' in os.environ
            else ""
        )

    def build_lake_path(
        self,
        environment,
        zone,
        mode=None,
        version=None,
        version_offset=None,
        write_needed=False,
        cloud_environment=cloud_config.CLOUD_ENVIRONMENT,
    ):
        """
        Builds sness datalake path to save
        :param environment: string, one of :class:`Environment` values
        :param zone: string, one of :class:`Zone` values
        :param mode: string, one of :class:`Mode` values.
            Ignored if is Delta Lake
        :param version: string, id of the wanted version.
            Ignored if is Delta Lake
        :param version_offset: string, version offset meaning that 0
            is the most recent. Ignored if is Delta Lake
        :parm cloud: string, for cloud used


        If no mode, version and version_offset are provided,
        the method will try to infer it.

        Returns:
            string, path to the dataset in the sness datalake:

            in GCP
                Ex: if is Delta Lake format, will return:
                    gs://prd-lake-trusted-sness/dataset_name
                    if is Parquet format, will return:
                    gs://prd-lake-trusted-sness/dataset_name/<version>
                    if is Parquet format with Mode.OVERWRITE, will return:
                    gs://prd-lake-trusted-sness/dataset_name
            in MGC
                Ex: if is Delta Lake format, will return:
                    s3a://prd-lake-trusted-sness/dataset_name
                    if is Parquet format, will return:
                    s3a://prd-lake-trusted-sness/dataset_name/<version>
                    if is Parquet format with Mode.OVERWRITE, will return:
                    s3a://prd-lake-trusted-sness/dataset_name
        """

        cloud_environment = Cloud_Environment(cloud_environment).value
        s3 = None

        lake_path = self.get_lake_path_api(
            Environment(environment).value,
            Zone(zone).value,
            self.namespace,
            self.dataset,
            cloud_environment,
        )

        cloud_prefix = lake_path.get("cloud_prefix")
        bucket_name = lake_path.get("bucket_name")
        if self.environment_spec != '':
            bucket_name = f"{bucket_name}-{self.environment_spec}"
        self.dataset = lake_path.get("bucket_dataset")

        cloud_environment = 'gcp' if cloud_prefix == 'gs' else 'mgc'

        if cloud_environment == 'gcp':
            bucket_name = self.__build_namespace_for_gcp(bucket_name)
        elif cloud_environment == 'mgc':
            s3 = init_s3_session()
            bucket_name = self.__build_namespace_for_mgc(bucket_name)

        bucket_path = f'{cloud_prefix}://{bucket_name}/{self.dataset}'

        # Check if is Delta Lake format
        is_delta = check_if_file_exists(
            f'{bucket_path}/_delta_log/', cloud_environment, s3
        )

        if is_delta:
            # path root of Delta Lake format
            return f'{bucket_path}/'
        else:
            # Legacy versioned parquets
            if not mode:
                if version:
                    mode = Mode.OVERWRITE
                else:
                    mode = self.__discover_mode(
                        bucket_name, self.dataset, cloud_environment, s3
                    )
            if mode == Mode.OVERWRITE and not version:
                version = self.__get_version_by_offset(
                    bucket_name,
                    self.dataset,
                    version_offset,
                    cloud_environment,
                    cloud_prefix,
                    s3,
                )
            blob_path = self.__build_blob_path(
                self.dataset, Mode(mode).value, version=version
            )
            return f'{cloud_prefix}://{bucket_name}/{blob_path}'

    def get_lake_path_api(
        self, environment, zone, namespace, dataset, cloud_environment
    ):
        """
        Get path of sness-path-api.
        :param environment: name of the environment
        :param zone: name of the zone
        :param namespace: name of the namespace
        :param dataset: name of the dataset
        :param cloud_environment: name of the cloud_environment
        :return: the sness-path from sness-path-api
        """
        base_url = os.environ["SNESS_PATH_API_URL"]
        headers = {
            "accept": "application/json",
            "Authorization": get_var_secret_manager(
                "SNESS_PATH_API_TOKEN_PRD"
            ),
        }
        params = {
            "environment": environment,
            "zone": zone,
            "namespace": namespace,
            "dataset": dataset,
            "cloud": cloud_environment,
        }

        try:
            response = requests.get(
                base_url, headers=headers, params=params, timeout=30
            )
            response.raise_for_status()

            lake_path = response.json()

            return lake_path[0]
        except requests.exceptions.HTTPError as err:
            if err.response.status_code == 404:
                print("Path não encontrado, Iniciando cadastro.")
                return self.create_lake_path_api(base_url, headers, params)
            else:
                print(
                    f"Erro HTTP inesperado: {err.response.status_code} - "
                    f"{err.response.text}"
                )
                return None
        except Exception as e:
            print(f"Erro ao acessar sness-path-api: {e}.")
            return None

    def create_lake_path_api(self, base_url, headers, params):
        """
        create path in sness-path-api.
        :param base_url: base url of sness-path-api
        :param headers: header create sness-path
        :param params: params to create sness-path
        :return: the created sness-path from sness-path-api
        """
        headers["Content-Type"] = "application/json"

        try:
            response = requests.post(
                base_url, headers=headers, json=params, timeout=30
            )
            response.raise_for_status()

            return response.json()
        except Exception as e:
            print(f"Erro ao acessar sness-path-api: {e}.")
            return None

    def __build_namespace_for_mgc(self, bucket):

        bucket_split = bucket.split('-', 3)
        bucket_replace = (
            f"{'-'.join(bucket_split[0:3])}-"
            f"{bucket_split[3].replace('_', '-')}"
        )

        logging.info(f'Replace {bucket} for {bucket_replace}.')
        return bucket_replace

    def __build_namespace_for_gcp(self, bucket):

        bucket_split = bucket.split('-', 3)
        bucket_replace = (
            f"{'-'.join(bucket_split[0:3])}-"
            f"{bucket_split[3].replace('-', '_')}"
        )

        logging.info(f'Replace {bucket} for {bucket_replace}.')
        return bucket_replace

    def __get_version_by_offset(
        self,
        bucket_name,
        dataset,
        offset,
        cloud_environment,
        cloud_prefix,
        s3=None,
    ):

        bucket_path = f'{cloud_prefix}://{bucket_name}/{self.dataset}'

        prefixes = self._get_dataset_version(
            bucket_name, self.dataset, cloud_environment, s3
        )

        if not prefixes:
            raise Exception(f'Path {bucket_path}/ not found ')
        prefixes.sort(reverse=True)
        if offset:
            return prefixes[offset]
        offset = 0
        for prefix in prefixes:
            if not self._has_success_file(
                bucket_name, f'{dataset}/{prefix}/', cloud_environment, s3
            ):
                continue
            else:
                return prefix
        return prefixes[offset]

    def __discover_mode(
        self, bucket_name, dataset, cloud_environment, s3=None
    ):
        versions = self._get_dataset_version(
            bucket_name, dataset, cloud_environment, s3
        )
        if len(versions) == 0:
            return Mode.APPEND
        return Mode.OVERWRITE

    def _get_dataset_version(
        self, bucket_name, dataset, cloud_environment, s3=None
    ):
        if cloud_environment == 'gcp':
            bucket = storage.Client().get_bucket(bucket_name)
            blobs = bucket.list_blobs(
                prefix=f'{dataset}/', delimiter='/', max_results=10
            )
            return self.__get_versions(blobs)
        elif cloud_environment == 'mgc':
            blobs = s3.list_objects_v2(
                Bucket=bucket_name,
                Prefix=f'{dataset}/',
                Delimiter='/',
                MaxKeys=10,
            )

            return self._get_version_maga_cloud(blobs)

    def __get_versions(self, blobs):
        # iterates over blobs for httpIterator reasons
        for _ in blobs:
            pass
        return [
            self.__parse_version(prefix)
            for prefix in blobs.prefixes
            if re.match(r'\d{14}', self.__parse_version(prefix)) is not None
        ]

    def _get_version_maga_cloud(self, blobs):
        """
        fetches the CommonPrefixes parameter and traverses using the prefix
        """
        result = []

        if "CommonPrefixes" in blobs:
            for prefix in blobs["CommonPrefixes"]:
                folder = prefix["Prefix"].split('/')
                result.append(folder[1])
        return result

    def __build_bucket_name(
        self, environment, zone, namespace, environment_spec
    ):
        if environment_spec != '':
            return f'{environment}-lake-{zone}-{namespace}-{environment_spec}'
        else:
            return f'{environment}-lake-{zone}-{namespace}'

    def __build_blob_path(self, dataset, mode, version):
        if mode == 'overwrite':
            return f'{dataset}/{version}/'
        return f"{dataset}/"

    def __parse_version(self, prefix):
        return prefix.rsplit('/', 2)[1]

    def _has_success_file(
        self, bucket_name, prefix, cloud_environment, s3=None
    ):

        if cloud_environment == 'gcp':

            bucket = storage.Client().get_bucket(bucket_name)
            blobs = bucket.list_blobs(prefix=prefix, max_results=5)
            for blob in blobs:
                if blob.name.endswith('_SUCCESS'):
                    return True
            return False
        elif cloud_environment == 'mgc':
            blobs = s3.list_objects_v2(
                Bucket=bucket_name,
                Prefix=prefix,
                MaxKeys=5,
            )

            for blob in blobs['Contents']:
                if blob['Key'].endswith('_SUCCESS'):
                    return True
            return False
