import logging
import pytest

from pyspark.sql import SparkSession

def quiet_logging():
    """ Disable logging INFO for test """
    logger = logging.getLogger('py4j')
    logger.setLevel('WARN')

@pytest.fixture(scope="session")

def spark_context(request):
    """ Set up a local spark session """
    spark = SparkSession.builder.master("local[*]").appName('unittest').getOrCreate()
    request.addfinalizer(lambda: spark.stop())

    quiet_logging()

    return spark
