"""
GSUTIL operations
"""

import os
import subprocess
from sness.cloud.s3_session import S3Session
from botocore.exceptions import ClientError

from sness.settings import cloud_config


def init_s3_session():
    s3_session = S3Session()
    s3 = s3_session.boto3_session()
    return s3


def check_if_file_exists(
    file_path, cloud_environment=cloud_config.CLOUD_ENVIRONMENT, s3=None
):

    if cloud_environment == "gcp":
        return os.system(f'gsutil -q stat "{file_path}"') == 0
    elif cloud_environment == 'mgc':

        if s3 is None:
            s3 = init_s3_session()

        file_path_splitted = file_path.split('/')
        bucket = file_path_splitted[2]
        key = file_path_splitted[3] + '/' + file_path_splitted[4]

        try:
            result = s3.list_objects_v2(Bucket=bucket, Prefix=key)
            if result['KeyCount'] == 0:
                return False
        except ClientError:
            return True

        return True


def is_path_delta_lake(path, cloud_environment=cloud_config.CLOUD_ENVIRONMENT):
    """Check if path is a Delta Lake dataset by checking if it has a
    `_delta_log/_last_checkpoint` file.

    Args:
        path (str): Full gcs path to the dataset.

    Returns:
        bool: True if have _last_checkpoint file, False otherwise.
    """
    path = path if not path.endswith("/") else path[:-1]
    return check_if_file_exists(
        file_path=path + '/_delta_log/00000000000000000000.json',
        cloud_environment=cloud_environment,
    ) or check_if_file_exists(
        file_path=path + '/_delta_log/_last_checkpoint',
        cloud_environment=cloud_environment,
    )


def remove_file(
    file_path, cloud_environment=cloud_config.CLOUD_ENVIRONMENT, s3=None
):

    if cloud_environment == 'mgc' and s3 is None:
        s3 = init_s3_session()

    if cloud_environment == 'gcp':
        return os.system(f'gsutil -q rm "{file_path}"') == 0
    elif cloud_environment == 'mgc':

        file_path_splitted = file_path.split('/')
        bucket = file_path_splitted[2]
        key = file_path_splitted[3] + '/' + file_path_splitted[3]

        s3.delete_object(
            Bucket=bucket,
            Key=key,
        )
        return


def get_list_of_files_in_manifest(
    path, cloud_environment=cloud_config.CLOUD_ENVIRONMENT
):
    """
    Returns a list of files in the manifest

    If data is not partitioned:
    gs://<bucket>/<dataset>/_symlink_format_manifest/manifest
    If data is partitioned:
    gs://<bucket>/<dataset>/_symlink_format_manifest/
        partition=<partition>/manifest
        or
        partition1=a1/.../partitionN=aN/manifest

    :param path: path of the Delta Lake dataset
    :return: list of files in the manifest
    """

    if cloud_environment == 'gcp':
        path = path if not path.endswith("/") else path[:-1]
        manifest_path = path + '/_symlink_format_manifest'

        try:
            # find manifest files
            # if partitioned or not
            output = subprocess.check_output(
                ["gsutil", "ls", manifest_path + '/**']
            )
            # manifest files
            manifests = [
                x
                for x in output.decode().split('\n')
                if x.endswith('/manifest')
            ]

            content = []
            for m in manifests:
                # get manifest content
                output = subprocess.check_output(["gsutil", "cat", m])
                output = output.decode().split('\n')
                content += output
            # Remove empty lines
            content = list(filter(None, content))
            return content
        except subprocess.CalledProcessError as err:
            raise err
    elif cloud_environment == 'mgc':
        s3 = init_s3_session()

        path = path if not path.endswith("/") else path[:-1]

        file_path_splitted = path.split('/')
        bucket = file_path_splitted[2]
        key = file_path_splitted[3]

        result = s3.list_objects_v2(Bucket=bucket, Prefix=key + '/')

        manifests = [
            x["Key"]
            for x in result["Contents"]
            if x["Key"].endswith('/manifest')
        ]

        content = []
        for x in manifests:
            data = s3.get_object(Bucket=bucket, Key=x)
            contents = data['Body'].read()
            output = contents.decode("utf-8").split('\n')
            content += output

        # Remove empty lines
        content = list(filter(None, content))

        # return content
        return content


def count_files_on_manifest(
    path, cloud_environment=cloud_config.CLOUD_ENVIRONMENT
):
    output = get_list_of_files_in_manifest(path, cloud_environment)
    return len(output)


def mark_dataset_to_repartition(
    dataset_path, cloud_environment=cloud_config.CLOUD_ENVIRONMENT
):
    dataset_path = (
        dataset_path if not dataset_path.endswith("/") else dataset_path[:-1]
    )
    os.system("touch /tmp/_TOO_MANY_FILES")

    if cloud_environment == 'gcp':
        return (
            os.system(
                'gsutil -q cp /tmp/_TOO_MANY_FILES '
                f'{dataset_path}/_TOO_MANY_FILES'
            )
            == 0
        )
    elif cloud_environment == 'mgc':
        return (
            os.system(
                'rclone copy /tmp/_TOO_MANY_FILES '
                f'container:{dataset_path}/_TOO_MANY_FILES'
            )
            == 0
        )
