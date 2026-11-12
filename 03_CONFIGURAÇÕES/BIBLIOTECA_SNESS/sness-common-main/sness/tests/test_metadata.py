from sness.datalake.metadata import (
    DatasetMetadata,
    Zone,
    Environment,
    Mode,
    Cloud_Environment,
)
from unittest import TestCase
from unittest.mock import patch, Mock
import requests
import os


@patch.dict(
    os.environ,
    {
        "SNESS_PATH_API_URL": "http://fake-api-url.com",
        "SNESS_PATH_API_TOKEN": "fake-api-token",
    },
)
class TestMetadata:
    @patch('requests.get')
    def test_should_build_lake_path_append_mode(self, mock_get):
        mock_response_data = [
            {
                "id": 1,
                "environment": "prd",
                "zone": "trusted",
                "namespace": "ephemeral",
                "dataset": "foo",
                "cloud": "gcp",
                "cloud_prefix": "gs",
                "bucket_name": "prd-lake-trusted-ephemeral",
                "bucket_dataset": "foo",
                "bq_project": "maga-bigdata",
                "bq_namespace": "teste",
                "bq_dataset": "teste",
            }
        ]

        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = mock_response_data
        mock_get.return_value.raise_for_status.return_value = None

        lake_path = DatasetMetadata('ephemeral', 'foo').build_lake_path(
            Environment.PRODUCTION,
            Zone.TRUSTED,
            Mode.APPEND,
            cloud_environment=Cloud_Environment.GOOGLE,
        )
        assert lake_path == 'gs://prd-lake-trusted-ephemeral/foo/'

    @patch(
        'sness.datalake.metadata.DatasetMetadata._get_dataset_version',
        return_value=['1000', '3000', '2000'],
    )
    @patch('requests.get')
    def test_should_build_lake_path_overwrite_mode(self, mock_get, mock):
        mock_response_data = [
            {
                "id": 1,
                "environment": "prd",
                "zone": "refined",
                "namespace": "ephemeral",
                "dataset": "foo",
                "cloud": "gcp",
                "cloud_prefix": "gs",
                "bucket_name": "prd-lake-refined-ephemeral",
                "bucket_dataset": "foo",
                "bq_project": "maga-bigdata",
                "bq_namespace": "teste",
                "bq_dataset": "teste",
            }
        ]

        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = mock_response_data
        mock_get.return_value.raise_for_status.return_value = None

        lake_path = lake_path = DatasetMetadata(
            'ephemeral', 'foo'
        ).build_lake_path(
            Environment.PRODUCTION,
            Zone.REFINED,
            Mode.OVERWRITE,
            version_offset=1,
            cloud_environment=Cloud_Environment.GOOGLE,
        )
        assert lake_path == 'gs://prd-lake-refined-ephemeral/foo/2000/'

    @patch('requests.get')
    def test_should_build_lake_path_overwrite_mode_with_version(
        self, mock_get
    ):
        mock_response_data = [
            {
                "id": 1,
                "environment": "prd",
                "zone": "refined",
                "namespace": "ephemeral",
                "dataset": "foo",
                "cloud": "gcp",
                "cloud_prefix": "gs",
                "bucket_name": "prd-lake-refined-ephemeral",
                "bucket_dataset": "foo",
                "bq_project": "maga-bigdata",
                "bq_namespace": "teste",
                "bq_dataset": "teste",
            }
        ]

        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = mock_response_data
        mock_get.return_value.raise_for_status.return_value = None

        lake_path = lake_path = DatasetMetadata(
            'ephemeral', 'foo'
        ).build_lake_path(
            Environment.PRODUCTION,
            Zone.REFINED,
            Mode.OVERWRITE,
            version='1',
            cloud_environment=Cloud_Environment.GOOGLE,
        )
        assert lake_path == 'gs://prd-lake-refined-ephemeral/foo/1/'

    @patch('requests.get')
    def test_should_build_lake_path_with_string_params(self, mock_get):
        mock_response_data = [
            {
                "id": 1,
                "environment": "prd",
                "zone": "refined",
                "namespace": "ephemeral",
                "dataset": "foo",
                "cloud": "gcp",
                "cloud_prefix": "gs",
                "bucket_name": "prd-lake-refined-ephemeral",
                "bucket_dataset": "foo",
                "bq_project": "maga-bigdata",
                "bq_namespace": "teste",
                "bq_dataset": "teste",
            }
        ]

        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = mock_response_data
        mock_get.return_value.raise_for_status.return_value = None

        lake_path = lake_path = DatasetMetadata(
            'ephemeral', 'foo'
        ).build_lake_path(
            'prd',
            'refined',
            'overwrite',
            version='1',
            cloud_environment=Cloud_Environment.GOOGLE,
        )
        assert lake_path == 'gs://prd-lake-refined-ephemeral/foo/1/'

    @patch('requests.get')
    @patch(
        'sness.datalake.metadata.DatasetMetadata._get_dataset_version',
        return_value=['2', '1', '0'],
    )
    @patch(
        'sness.datalake.metadata.DatasetMetadata._has_success_file',
        return_value=False,
    )
    def test_should_build_lake_path_without_specified_mode(
        self, mock, mock2, mock_get
    ):
        mock_response_data = [
            {
                "id": 1,
                "environment": "prd",
                "zone": "refined",
                "namespace": "ephemeral",
                "dataset": "foo",
                "cloud": "gcp",
                "cloud_prefix": "gs",
                "bucket_name": "prd-lake-refined-ephemeral",
                "bucket_dataset": "foo",
                "bq_project": "maga-bigdata",
                "bq_namespace": "teste",
                "bq_dataset": "teste",
            }
        ]

        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = mock_response_data
        mock_get.return_value.raise_for_status.return_value = None

        lake_path = lake_path = DatasetMetadata(
            'ephemeral', 'foo'
        ).build_lake_path(
            'prd',
            'refined',
            cloud_environment=Cloud_Environment.GOOGLE,
        )
        assert lake_path == 'gs://prd-lake-refined-ephemeral/foo/2/'

    @patch('requests.get')
    @patch(
        'sness.datalake.metadata.DatasetMetadata._get_dataset_version',
        return_value=['2', '1', '0'],
    )
    @patch(
        'sness.datalake.metadata.DatasetMetadata._has_success_file',
        return_value=True,
    )
    def test_should_build_lake_path_with_temporary_folder(
        self, mock, mock2, mock_get
    ):
        mock_response_data = [
            {
                "id": 1,
                "environment": "prd",
                "zone": "refined",
                "namespace": "ephemeral",
                "dataset": "foo",
                "cloud": "gcp",
                "cloud_prefix": "gs",
                "bucket_name": "prd-lake-refined-ephemeral",
                "bucket_dataset": "foo",
                "bq_project": "maga-bigdata",
                "bq_namespace": "teste",
                "bq_dataset": "teste",
            }
        ]

        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = mock_response_data
        mock_get.return_value.raise_for_status.return_value = None

        lake_path = lake_path = DatasetMetadata(
            'ephemeral', 'foo'
        ).build_lake_path(
            'prd',
            'refined',
            cloud_environment=Cloud_Environment.GOOGLE,
        )
        assert lake_path == 'gs://prd-lake-refined-ephemeral/foo/2/'

    @patch('requests.get')
    @patch('requests.post')
    def test_get_lake_path_api_not_found_and_create(self, mock_post, mock_get):
        mock_response_data = {
            "id": 1,
            "environment": "prd",
            "zone": "trusted",
            "namespace": "ephemeral",
            "dataset": "foo",
            "cloud": "gcp",
            "cloud_prefix": "gs",
            "bucket_name": "prd-lake-trusted-ephemeral",
            "bucket_dataset": "foo",
            "bq_project": "maga-bigdata",
            "bq_namespace": "teste",
            "bq_dataset": "teste",
        }

        mock_error_response = Mock(status_code=404)
        mock_get.return_value.raise_for_status.side_effect = (
            requests.exceptions.HTTPError(response=mock_error_response)
        )

        mock_post.return_value.status_code = 201
        mock_post.return_value.json.return_value = mock_response_data
        mock_post.return_value.raise_for_status.return_value = None

        lake_path = DatasetMetadata('ephemeral', 'foo').build_lake_path(
            Environment.PRODUCTION,
            Zone.TRUSTED,
            Mode.APPEND,
            cloud_environment=Cloud_Environment.GOOGLE,
        )
        assert lake_path == 'gs://prd-lake-trusted-ephemeral/foo/'
        mock_post.assert_called_once()


class TestModeEnum(TestCase):
    def test_should_create_enum(self):
        assert Mode('append')

    def test_should_fail_to_create_enum(self):
        with self.assertRaisesRegex(
            ValueError, 'Mode can only be: APPEND, OVERWRITE'
        ):
            Mode('Batata')
