import logging
import pyspark.sql.functions as sf
import pyspark.sql.types as st
from datetime import datetime
from sness.datalake.metadata import DatasetMetadata
from sness.settings import cloud_config


class Changelogs:
    def __init__(
        self,
        spark_session,
        environment,
        zone,
        namespace,
        dataset,
        changelog_keys,
        changelog_columns,
        cloud_environment=cloud_config.CLOUD_ENVIRONMENT,
    ):
        self.spark_session = spark_session
        self.environment = (
            environment if type(environment) == str else environment.value
        )
        self.zone = zone if type(zone) == str else zone.value
        self.namespace = namespace
        self.dataset = dataset
        self.changelog_keys = changelog_keys
        self.changelog_columns = changelog_columns
        self.cloud_environment = cloud_environment

    def _get_dataset_previous_version_path(self):
        ds = DatasetMetadata(self.namespace, self.dataset)
        try:
            lake_path = ds.build_lake_path(
                self.environment, self.zone, self.cloud_environment
            )
            if len(lake_path.split("/")) > 5:
                return lake_path
            else:
                return None
        except IndexError:
            return None

    def register_changelog(
        self, df, cloud_environment=cloud_config.CLOUD_ENVIRONMENT
    ):
        print('Changelog activated')

        CLOUD_PREFIX = {'gcp': 'gs://', 'mgc': 's3a://:'}
        if self.changelog_keys and type(self.changelog_keys) == str:
            self.changelog_keys = [self.changelog_keys]

        if self.changelog_columns and type(self.changelog_columns) == str:
            self.changelog_columns = [self.changelog_columns]

        last_version_path = self._get_dataset_previous_version_path()

        if last_version_path:
            df_previous = self.spark_session.read.parquet(last_version_path)
            if df_previous:
                partition_time = datetime.now().strftime("%Y%m%d")
                register_log = datetime.now().strftime("%Y%m%d%H%M%S")
                for col in self.changelog_columns:
                    try:
                        df_differences = (
                            df.select([*self.changelog_keys] + [col])
                            .exceptAll(
                                df_previous.select(
                                    [*self.changelog_keys] + [col]
                                )
                            )
                            .select(
                                [*self.changelog_keys]
                                + [
                                    sf.lit(self.namespace).alias("namespace"),
                                    sf.lit(self.dataset).alias("dataset"),
                                    sf.lit(col).alias("column"),
                                    sf.trim(sf.col(col))
                                    .cast(st.StringType())
                                    .alias("value"),
                                    sf.lit(register_log).alias(
                                        "log_timestamp"
                                    ),  # noqa
                                    sf.lit(partition_time).alias(
                                        "comparison_time"
                                    ),
                                ]  # noqa
                            )
                        )

                        df_differences.write.format("parquet").save(
                            path=f"{CLOUD_PREFIX[cloud_environment]}://{self.environment}-data_changelog/{self.namespace}/{self.dataset}/{self.zone}/",
                            mode="append",  # noqa
                            partitionBy=["comparison_time"],
                        )
                    except Exception as msg:
                        logging.error(
                            f'sness_spark: Error in data changelog:\
                                            log from {col}: {str(msg)}'
                        )
