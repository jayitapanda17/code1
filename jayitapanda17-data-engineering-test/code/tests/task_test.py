# import unittest
# import findspark
# findspark.find()

import pytest
from code import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql import SparkSession

@pytest.mark.usefixtures("spark_session")
def test_sample_transform(spark_session):
    test_df = spark_session.createDataFrame(
        [
            ('Ram', 'India', 15),
            ('Tom', 'US', 50),
            ('Harry', 'Canada', 20),
        ],
        ['name', 'address', 'age']
    )
    #new_df = transform_data(df, spark,config)
    assert test_df.count() == 3



