import pytest

from datetime import datetime, timedelta
from pyspark.conf import SparkConf
from unittest.mock import patch, Mock
from sness.datalake.sness_spark import SnessSpark
import os

MOCK_RESPONSE_DATA = [
    {
        "id": 1,
        "environment": "prd",
        "zone": "refined",
        "namespace": "foo",
        "dataset": "bar",
        "cloud": "gcp",
        "cloud_prefix": "gs",
        "bucket_name": "prd-lake-refined-foo",
        "bucket_dataset": "bar",
        "bq_project": "maga-bigdata",
        "bq_namespace": "foo",
        "bq_dataset": "bar",
    }
]


class TestSnessSpark:
    @patch('pyspark.sql.SparkSession.builder')
    def test_should_create_vanilla_session(self, mocked_builder):

        mocked_builder.config.return_value = mocked_builder
        mocked_builder.enableHiveSupport.return_value = mocked_builder

        SnessSpark()

        mocked_builder.enableHiveSupport().getOrCreate.assert_called()

    @patch('pyspark.sql.SparkSession.builder')
    def test_should_create_custom_session(self, mocked_builder):
        conf = SparkConf()
        conf.set('foo', 'bar')

        mocked_builder.config.return_value = mocked_builder
        mocked_builder.enableHiveSupport.return_value = mocked_builder

        SnessSpark(conf=conf)

        mocked_builder.enableHiveSupport().getOrCreate.assert_called()


@patch('sness.storage.check_if_file_exists', return_value=False)
@patch.dict(
    os.environ,
    {
        "SNESS_PATH_API_URL": "http://fake-api-url.com",
        "SNESS_PATH_API_TOKEN": "fake-api-token",
    },
)
class TestSnessDataFrameReader:
    def _setup_mocks(self, mocked_builder, mock_get, mock_secret):
        """Helper para configurar mocks comuns"""
        mocked_builder.config.return_value = mocked_builder
        mocked_builder.enableHiveSupport.return_value = mocked_builder

        mock_spark_session = Mock()
        mocked_builder.getOrCreate.return_value = mock_spark_session

        mock_reader = Mock()
        mock_spark_session.read = mock_reader

        mock_reader.load.return_value = Mock()

        mock_secret.return_value = "fake-api-token"
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = MOCK_RESPONSE_DATA
        mock_get.return_value.raise_for_status.return_value = None

        return mock_reader

    @patch('sness.datalake.metadata.get_var_secret_manager')
    @patch('requests.get')
    @patch('google.cloud.storage.Client')
    @patch('pyspark.sql.SparkSession.builder')
    def test_should_read_csv(
        self,
        mocked_builder,
        mocked_client,
        mock_get,
        mock_secret,
        mock_check_file,
    ):
        mock_reader = self._setup_mocks(mocked_builder, mock_get, mock_secret)

        SnessSpark().read.csv('prd', 'refined', 'foo', 'bar', 'gcp')

        mock_reader.load.assert_called_with(
            format='csv', path='gs://prd-lake-refined-foo/bar/'
        )

    @patch('sness.datalake.metadata.get_var_secret_manager')
    @patch('requests.get')
    @patch('google.cloud.storage.Client')
    @patch('pyspark.sql.SparkSession.builder')
    def test_should_read_json(
        self,
        mocked_spark,
        mocked_client,
        mock_get,
        mock_secret,
        mock_check_file,
    ):
        mock_reader = self._setup_mocks(mocked_spark, mock_get, mock_secret)

        SnessSpark().read.json('prd', 'refined', 'foo', 'bar', 'gcp')

        mock_reader.load.assert_called_with(
            format='json', path='gs://prd-lake-refined-foo/bar/'
        )

    @patch('sness.datalake.metadata.get_var_secret_manager')
    @patch('requests.get')
    @patch('google.cloud.storage.Client')
    @patch('pyspark.sql.SparkSession.builder')
    def test_should_read_parquet(
        self,
        mocked_spark,
        mocked_client,
        mock_get,
        mock_secret,
        mock_check_file,
    ):
        mock_reader = self._setup_mocks(mocked_spark, mock_get, mock_secret)

        SnessSpark().read.parquet(
            'prd', 'refined', 'foo', 'bar', 'gcp', delta=False
        )  # noqa

        mock_reader.load.assert_called_with(
            format='parquet',
            path='gs://prd-lake-refined-foo/bar/',
            delta=False,
        )

    @patch('sness.datalake.metadata.get_var_secret_manager')
    @patch('requests.get')
    @patch('google.cloud.storage.Client')
    @patch('pyspark.sql.SparkSession.builder')
    def test_should_read_avro(
        self,
        mocked_spark,
        mocked_client,
        mock_get,
        mock_secret,
        mock_check_file,
    ):
        mock_reader = self._setup_mocks(mocked_spark, mock_get, mock_secret)

        SnessSpark().read.avro('prd', 'refined', 'foo', 'bar', 'gcp')

        mock_reader.load.assert_called_with(
            format='avro', path='gs://prd-lake-refined-foo/bar/'
        )

    @patch('sness.datalake.metadata.get_var_secret_manager')
    @patch('requests.get')
    @patch('google.cloud.storage.Client')
    @patch('pyspark.sql.SparkSession.builder')
    def test_load(
        self,
        mocked_spark,
        mocked_client,
        mock_get,
        mock_secret,
        mock_check_file,
    ):
        mock_reader = self._setup_mocks(mocked_spark, mock_get, mock_secret)

        SnessSpark().read.load(
            'prd',
            'refined',
            'foo',
            'bar',
            'gcp',
            format='parquet',
            delta=False,
        )

        mock_reader.load.assert_called_with(
            format='parquet',
            path='gs://prd-lake-refined-foo/bar/',
            delta=False,
        )

    @patch('sness.datalake.metadata.get_var_secret_manager')
    @patch('google.cloud.storage.Client')
    @patch('pyspark.sql.SparkSession.builder')
    def test_should_fail_due_to_bad_environ(
        self, mocked_spark, mocked_client, mock_secret, mock_check_file
    ):
        mock_secret.return_value = "fake-api-token"
        with pytest.raises(ValueError):
            SnessSpark().read.csv('foo', 'trusted', 'foo', 'bar', 'gcp')

    @patch('sness.datalake.metadata.get_var_secret_manager')
    @patch('google.cloud.storage.Client')
    @patch('pyspark.sql.SparkSession.builder')
    def test_should_fail_due_to_bad_zone(
        self, mocked_spark, mocked_client, mock_secret, mock_check_file
    ):
        mock_secret.return_value = "fake-api-token"
        with pytest.raises(ValueError):
            SnessSpark().read.csv('prd', 'foo', 'foo', 'bar', 'gcp')


@patch('sness.storage.check_if_file_exists', return_value=False)
@patch.dict(
    os.environ,
    {
        "SNESS_PATH_API_URL": "http://fake-api-url.com",
        "SNESS_PATH_API_TOKEN": "fake-api-token",
    },
)
class TestSnessDataFrameStreamReader:
    def _setup_mocks(self, mocked_builder, mock_get, mock_secret):
        """Helper para configurar mocks comuns"""
        mocked_builder.config.return_value = mocked_builder
        mocked_builder.enableHiveSupport.return_value = mocked_builder

        mock_spark_session = Mock()
        mocked_builder.getOrCreate.return_value = mock_spark_session

        mock_stream_reader = Mock()
        mock_spark_session.readStream = mock_stream_reader

        mock_stream_reader.load.return_value = Mock()

        # 4. External API Mocks
        mock_secret.return_value = "fake-api-token"
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = MOCK_RESPONSE_DATA
        mock_get.return_value.raise_for_status.return_value = None

        return mock_stream_reader

    @patch('sness.datalake.metadata.get_var_secret_manager')
    @patch('requests.get')
    @patch('google.cloud.storage.Client')
    @patch('pyspark.sql.SparkSession.builder')
    def test_should_read_csv(
        self,
        mocked_builder,
        mocked_client,
        mock_get,
        mock_secret,
        mock_check_file,
    ):
        mock_stream_reader = self._setup_mocks(
            mocked_builder, mock_get, mock_secret
        )

        SnessSpark().readStream.csv('prd', 'refined', 'foo', 'bar', 'gcp')

        mock_stream_reader.load.assert_called_with(
            format='csv', path='gs://prd-lake-refined-foo/bar/'
        )

    @patch('sness.datalake.metadata.get_var_secret_manager')
    @patch('requests.get')
    @patch('google.cloud.storage.Client')
    @patch('pyspark.sql.SparkSession.builder')
    def test_should_read_json(
        self,
        mocked_builder,
        mocked_client,
        mock_get,
        mock_secret,
        mock_check_file,
    ):
        mock_stream_reader = self._setup_mocks(
            mocked_builder, mock_get, mock_secret
        )

        SnessSpark().readStream.json('prd', 'refined', 'foo', 'bar', 'gcp')

        mock_stream_reader.load.assert_called_with(
            format='json', path='gs://prd-lake-refined-foo/bar/'
        )

    @patch('sness.datalake.metadata.get_var_secret_manager')
    @patch('requests.get')
    @patch('google.cloud.storage.Client')
    @patch('pyspark.sql.SparkSession.builder')
    def test_should_read_parquet(
        self,
        mocked_builder,
        mocked_client,
        mock_get,
        mock_secret,
        mock_check_file,
    ):
        mock_stream_reader = self._setup_mocks(
            mocked_builder, mock_get, mock_secret
        )

        SnessSpark().readStream.parquet(
            'prd', 'refined', 'foo', 'bar', 'gcp', delta=False
        )  # noqa

        mock_stream_reader.load.assert_called_with(
            format='parquet',
            path='gs://prd-lake-refined-foo/bar/',
            delta=False,
        )

    @patch('sness.datalake.metadata.get_var_secret_manager')
    @patch('requests.get')
    @patch('google.cloud.storage.Client')
    @patch('pyspark.sql.SparkSession.builder')
    def test_should_load(
        self,
        mocked_spark,
        mocked_client,
        mock_get,
        mock_secret,
        mock_check_file,
    ):
        mock_stream_reader = self._setup_mocks(
            mocked_spark, mock_get, mock_secret
        )

        SnessSpark().readStream.load(
            'prd',
            'refined',
            'foo',
            'bar',
            'gcp',
            format='parquet',
            delta=False,
        )

        mock_stream_reader.load.assert_called_with(
            format='parquet',
            path='gs://prd-lake-refined-foo/bar/',
            delta=False,
        )

    @patch('sness.datalake.metadata.get_var_secret_manager')
    @patch('google.cloud.storage.Client')
    @patch('pyspark.sql.SparkSession.builder.getOrCreate')
    def test_read_fail_due_to_bad_environ(
        self, mocked_spark, mocked_client, mock_secret, mock_check_file
    ):
        mock_secret.return_value = "fake-api-token"
        with pytest.raises(ValueError):
            SnessSpark().readStream.csv('foo', 'trusted', 'foo', 'bar', 'gcp')

    @patch('sness.datalake.metadata.get_var_secret_manager')
    @patch('google.cloud.storage.Client')
    @patch('pyspark.sql.SparkSession.builder.getOrCreate')
    def test_read_fail_due_to_bad_zone(
        self, mocked_spark, mocked_client, mock_secret, mock_check_file
    ):
        mock_secret.return_value = "fake-api-token"
        with pytest.raises(ValueError):
            SnessSpark().readStream.csv('prd', 'foo', 'foo', 'bar', 'gcp')


@patch('sness.storage.check_if_file_exists', return_value=False)
@patch.dict(
    os.environ,
    {
        "SNESS_PATH_API_URL": "http://fake-api-url.com",
        "SNESS_PATH_API_TOKEN": "fake-api-token",
    },
)
class TestSnessDataFrameWriter:
    @patch('sness.datalake.metadata.get_var_secret_manager')
    @patch('requests.get')
    @patch('google.cloud.storage.Client')
    @patch('pyspark.sql.SparkSession.builder.getOrCreate')
    def test_write_csv(
        self,
        mocked_spark,
        mocked_client,
        mock_get,
        mock_secret,
        mock_check_file,
    ):
        df = Mock()
        mock_secret.return_value = "fake-api-token"
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = MOCK_RESPONSE_DATA
        mock_get.return_value.raise_for_status.return_value = None

        SnessSpark().write.csv(df, 'prd', 'refined', 'foo', 'bar', 'gcp')
        df.write.save.assert_called_with(
            format='csv', path='gs://prd-lake-refined-foo/bar/'
        )

    @patch('sness.datalake.metadata.get_var_secret_manager')
    @patch('requests.get')
    @patch('google.cloud.storage.Client')
    @patch('pyspark.sql.SparkSession.builder.getOrCreate')
    def test_write_json(
        self,
        mocked_spark,
        mocked_client,
        mock_get,
        mock_secret,
        mock_check_file,
    ):
        df = Mock()
        mock_secret.return_value = "fake-api-token"
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = MOCK_RESPONSE_DATA
        mock_get.return_value.raise_for_status.return_value = None

        SnessSpark().write.json(df, 'prd', 'refined', 'foo', 'bar', 'gcp')
        df.write.save.assert_called_with(
            format='json', path='gs://prd-lake-refined-foo/bar/'
        )

    @patch('requests.get')
    @patch('google.cloud.storage.Client')
    @patch('pyspark.sql.SparkSession.builder.getOrCreate')
    def test_write_parquet(
        self, mocked_spark, mocked_client, mock_get, mock_check_file
    ):
        df = Mock()
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = MOCK_RESPONSE_DATA
        mock_get.return_value.raise_for_status.return_value = None

        SnessSpark().write.parquet(
            df, 'prd', 'refined', 'foo', 'bar', 'gcp', delta=False
        )  # noqa
        df.write.save.assert_called_with(
            format='parquet',
            path='gs://prd-lake-refined-foo/bar/',
            delta=False,
        )

    @patch('sness.datalake.metadata.get_var_secret_manager')
    @patch('requests.get')
    @patch('google.cloud.storage.Client')
    @patch('pyspark.sql.SparkSession.builder.getOrCreate')
    def test_save(
        self,
        mocked_spark,
        mocked_client,
        mock_get,
        mock_secret,
        mock_check_file,
    ):
        df = Mock()
        mock_secret.return_value = "fake-api-token"
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = MOCK_RESPONSE_DATA
        mock_get.return_value.raise_for_status.return_value = None

        SnessSpark().write.save(
            df,
            'prd',
            'refined',
            'foo',
            'bar',
            'gcp',
            format='parquet',
            delta=False,
        )
        df.write.save.assert_called_with(
            format='parquet',
            path='gs://prd-lake-refined-foo/bar/',
            delta=False,
        )

    @patch('sness.datalake.metadata.get_var_secret_manager')
    @patch('requests.get')
    @patch(
        'sness.datalake.sness_spark._SnessSparkBase._build_version',
        return_value=1,
    )  # noqa
    @patch('pyspark.sql.SparkSession.builder.getOrCreate')
    def test_save_overwrite(
        self,
        mocked_spark,
        mocked_client,
        mock_get,
        mock_secret,
        mock_check_file,
    ):
        df = Mock()
        mock_secret.return_value = "fake-api-token"
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = MOCK_RESPONSE_DATA
        mock_get.return_value.raise_for_status.return_value = None

        SnessSpark().write.save(
            df,
            'prd',
            'refined',
            'foo',
            'bar',
            'gcp',
            format='parquet',
            mode='overwrite',
            delta=False,
        )
        df.write.save.assert_called_with(
            format='parquet',
            mode='overwrite',
            path='gs://prd-lake-refined-foo/bar/1/',
            delta=False,
        )

    @patch('pyspark.sql.SparkSession.builder.getOrCreate')
    def test_build_version(self, mock_check_file, mocked_spark):
        version = SnessSpark().write._build_version()
        assert len(version) == 14
        assert datetime.strptime(version, '%Y%m%d%H%M%S')
        v_timestamp = datetime.strptime(version, '%Y%m%d%H%M%S')
        one_minute_ago = datetime.utcnow() - timedelta(minutes=2)
        one_minute_ahead = datetime.utcnow() + timedelta(minutes=2)
        assert one_minute_ago < v_timestamp < one_minute_ahead

    @patch('sness.datalake.metadata.get_var_secret_manager')
    @patch('google.cloud.storage.Client')
    @patch('pyspark.sql.SparkSession.builder.getOrCreate')
    def test_should_fail_due_to_format_zone_restriction(
        self, mocked_spark, mocked_client, mock_secret, mock_check_file
    ):
        df = Mock()
        mock_secret.return_value = "fake-api-token"
        with pytest.raises(ValueError, match='Can only write parquet in RAW'):
            SnessSpark().write.csv(df, 'foo', 'raw', 'foo', 'bar', 'gcp')
        with pytest.raises(
            ValueError, match='Can only write parquet in TRUSTED'
        ):
            SnessSpark().write.csv(df, 'foo', 'trusted', 'foo', 'bar', 'gcp')

    @patch('sness.datalake.metadata.get_var_secret_manager')
    @patch('google.cloud.storage.Client')
    @patch('pyspark.sql.SparkSession.builder.getOrCreate')
    def test_should_fail_due_to_bad_environ(
        self, mocked_spark, mocked_client, mock_secret, mock_check_file
    ):
        df = Mock()
        mock_secret.return_value = "fake-api-token"
        with pytest.raises(
            ValueError, match='Environment can only be: STAGING, PRODUCTION'
        ):
            SnessSpark().write.parquet(
                df, 'foo', 'raw', 'foo', 'bar', 'gcp', delta=False
            )  # noqa

    @patch('sness.datalake.metadata.get_var_secret_manager')
    @patch('google.cloud.storage.Client')
    @patch('pyspark.sql.SparkSession.builder.getOrCreate')
    def test_should_fail_due_to_bad_zone(
        self, mocked_spark, mocked_client, mock_secret, mock_check_file
    ):
        df = Mock()
        mock_secret.return_value = "fake-api-token"
        with pytest.raises(
            ValueError,
            match='Zone can only be: TRANSIENT, RAW, TRUSTED, REFINED',
        ):
            SnessSpark().write.parquet(
                df, 'prd', 'foo', 'foo', 'bar', 'gcp', delta=False
            )  # noqa


@patch('sness.storage.check_if_file_exists', return_value=False)
@patch.dict(
    os.environ,
    {
        "SNESS_PATH_API_URL": "http://fake-api-url.com",
        "SNESS_PATH_API_TOKEN": "fake-api-token",
    },
)
class TestSnessDataFrameStreamWriter:
    @patch('sness.datalake.metadata.get_var_secret_manager')
    @patch('requests.get')
    @patch('google.cloud.storage.Client')
    @patch('pyspark.sql.SparkSession.builder.getOrCreate')
    def test_write_csv(
        self,
        mocked_spark,
        mocked_client,
        mock_get,
        mock_secret,
        mock_check_file,
    ):
        df = Mock()
        mock_secret.return_value = "fake-api-token"
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = MOCK_RESPONSE_DATA
        mock_get.return_value.raise_for_status.return_value = None

        writer = SnessSpark().writeStream
        writer.csv(df, 'prd', 'refined', 'foo', 'bar', 'gcp')
        df.writeStream.trigger.assert_called_with(once=True)
        df.writeStream.trigger().start.assert_called_with(
            checkpointLocation='gs://prd-cluster-config/checkpoints/refined-foo-bar',  # noqa
            format='csv',
            path='gs://prd-lake-refined-foo/bar/',
        )
        df.writeStream.trigger().start().awaitTermination.assert_called()

    @patch('sness.datalake.metadata.get_var_secret_manager')
    @patch('requests.get')
    @patch('google.cloud.storage.Client')
    @patch('pyspark.sql.SparkSession.builder.getOrCreate')
    def test_write_json(
        self,
        mocked_spark,
        mocked_client,
        mock_get,
        mock_secret,
        mock_check_file,
    ):
        df = Mock()
        mock_secret.return_value = "fake-api-token"

        mock_transient_data = [
            {
                "id": 1,
                "environment": "prd",
                "zone": "transient",
                "namespace": "foo",
                "dataset": "bar",
                "cloud": "gcp",
                "cloud_prefix": "gs",
                "bucket_name": "prd-lake-transient-foo",
                "bucket_dataset": "bar",
                "bq_project": "maga-bigdata",
                "bq_namespace": "foo",
                "bq_dataset": "bar",
            }
        ]

        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = mock_transient_data
        mock_get.return_value.raise_for_status.return_value = None

        writer = SnessSpark().writeStream
        writer.json(df, 'prd', 'transient', 'foo', 'bar', 'gcp')
        df.writeStream.trigger.assert_called_with(once=True)
        df.writeStream.trigger().start.assert_called_with(
            checkpointLocation='gs://prd-cluster-config/checkpoints/transient-foo-bar',  # noqa
            format='json',
            path='gs://prd-lake-transient-foo/bar/',
        )
        df.writeStream.trigger().start().awaitTermination.assert_called()

    @patch('sness.datalake.metadata.get_var_secret_manager')
    @patch('requests.get')
    @patch('google.cloud.storage.Client')
    @patch('pyspark.sql.SparkSession.builder.getOrCreate')
    def test_write_parquet(
        self,
        mocked_spark,
        mocked_client,
        mock_get,
        mock_secret,
        mock_check_file,
    ):
        df = Mock()
        mock_secret.return_value = "fake-api-token"
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = MOCK_RESPONSE_DATA
        mock_get.return_value.raise_for_status.return_value = None

        writer = SnessSpark().writeStream
        writer.parquet(df, 'prd', 'refined', 'foo', 'bar', 'gcp', delta=False)
        df.writeStream.trigger.assert_called_with(once=True)
        df.writeStream.trigger().start.assert_called_with(
            checkpointLocation='gs://prd-cluster-config/checkpoints/refined-foo-bar',  # noqa
            format='parquet',
            path='gs://prd-lake-refined-foo/bar/',
            delta=False,
        )
        df.writeStream.trigger().start().awaitTermination.assert_called()

    @patch('sness.datalake.metadata.get_var_secret_manager')
    @patch('requests.get')
    @patch('google.cloud.storage.Client')
    @patch('pyspark.sql.SparkSession.builder.getOrCreate')
    def test_write_save(
        self,
        mocked_spark,
        mocked_client,
        mock_get,
        mock_secret,
        mock_check_file,
    ):
        df = Mock()
        mock_secret.return_value = "fake-api-token"
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = MOCK_RESPONSE_DATA
        mock_get.return_value.raise_for_status.return_value = None

        writer = SnessSpark().writeStream
        writer.save(
            df,
            'prd',
            'refined',
            'foo',
            'bar',
            'gcp',
            format='parquet',
            delta=False,
        )
        df.writeStream.trigger.assert_called_with(once=True)
        df.writeStream.trigger().start.assert_called_with(
            checkpointLocation='gs://prd-cluster-config/checkpoints/refined-foo-bar',  # noqa
            format='parquet',
            path='gs://prd-lake-refined-foo/bar/',
            delta=False,
        )
        df.writeStream.trigger().start().awaitTermination.assert_called()

    @patch('sness.datalake.metadata.get_var_secret_manager')
    @patch('google.cloud.storage.Client')
    @patch('pyspark.sql.SparkSession.builder.getOrCreate')
    def test_should_fail_due_to_bad_environ(
        self, mocked_spark, mocked_client, mock_secret, mock_check_file
    ):
        df = Mock()
        mock_secret.return_value = "fake-api-token"
        with pytest.raises(
            ValueError, match='Environment can only be: STAGING, PRODUCTION'
        ):
            SnessSpark().writeStream.parquet(
                df, 'foo', 'refined', 'foo', 'bar', 'gcp', delta=False
            )  # noqa

    @patch('sness.datalake.metadata.get_var_secret_manager')
    @patch('google.cloud.storage.Client')
    @patch('pyspark.sql.SparkSession.builder.getOrCreate')
    def test_should_fail_due_to_bad_zone(
        self, mocked_spark, mocked_client, mock_secret, mock_check_file
    ):
        df = Mock()
        mock_secret.return_value = "fake-api-token"
        with pytest.raises(
            ValueError,
            match='Zone can only be: TRANSIENT, RAW, TRUSTED, REFINED',
        ):
            SnessSpark().writeStream.parquet(
                df, 'prd', 'foo', 'foo', 'bar', 'gcp', delta=False
            )  # noqa

    @patch('sness.datalake.metadata.get_var_secret_manager')
    @patch('google.cloud.storage.Client')
    @patch('pyspark.sql.SparkSession.builder.getOrCreate')
    def test_should_fail_due_to_format_zone_restriction(
        self, mocked_spark, mocked_client, mock_secret, mock_check_file
    ):
        df = Mock()
        mock_secret.return_value = "fake-api-token"
        with pytest.raises(ValueError, match='Can only write parquet in RAW'):
            SnessSpark().writeStream.json(
                df, 'prd', 'raw', 'foo', 'bar', 'gcp'
            )
        with pytest.raises(
            ValueError, match='Can only write parquet in TRUSTED'
        ):
            SnessSpark().writeStream.json(
                df, 'prd', 'trusted', 'foo', 'bar', 'gcp'
            )
