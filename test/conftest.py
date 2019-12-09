import logging
import pytest

from pyspark import SparkConf
from pyspark import SparkContext

def quiet_logging():
    """ Disable logging INFO for test """
    logger = logging.getLogger('py4j')
    logger.setLevel('WARN')

@pytest.fixture(scope="session")

def spark_context(request):
    """ Set up a local spark session """
    conf = (SparkConf().setMaster("local[2]").setAppName("unittest"))
    request.addfinalizer(lambda: sc.stop())
    sc = SparkContext(conf=conf)

    quiet_logging()

    return sc
