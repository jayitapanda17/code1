import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope='session')
def spark():
    spark_obj= SparkSession.builder \
      .master("local") \
      .appName("testing") \
      .getOrCreate()
    return spark_obj
