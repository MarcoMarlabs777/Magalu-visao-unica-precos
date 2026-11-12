import sys

from unittest import mock
from unittest.mock import patch

sys.modules["delta"] = mock.MagicMock()
sys.modules["delta.tables"] = mock.MagicMock()
sys.modules["delta.tables.DeltaTable"] = mock.MagicMock()
from sness.datalake.delta_operations import convert_to_delta  # noqa: E402


class TestDeltaOperations:
    @patch("pyspark.sql.SparkSession.builder.getOrCreate")
    def test_should_not_convert_to_delta(self, mocked_spark):
        converted = convert_to_delta(
            mocked_spark, "prd", "refined", "foo", "bar"
        )
        assert not converted
