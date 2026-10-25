import os
import sys

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "spark-jobs", "catalog"))


@pytest.fixture(scope="session")
def spark():
    # Plain local Spark: the unit tests need no Iceberg catalog or cluster.
    s = (SparkSession.builder.master("local[1]").appName("catalog-tests")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "1")
         .getOrCreate())
    yield s
    s.stop()
