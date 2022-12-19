# import modules
import os
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from task_config import *
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from dq_check import *
from helper_py.logging_helper import TaskLogger

import logging
import re

def getLogger(config, log_file_name):

    '''
    :param log_file_name
    :return: log_helper object
    '''
    log_helper = TaskLogger(config.get("CONFIG","application_name"), config.get("CONFIG","process_name"), log_file_name).getLogger()
    return log_helper

def read_input(spark,config):
    '''
    This function will take as input source path and create dataframe and return.
    :param spark:
    :param config:
    :return: DataFrame
    '''

    logger.info("Reading input data from source location")

    #define input schema
    input_schema = StructType([StructField("cookTime", StringType(), True),StructField("datePublished", StringType(), True),StructField("description", StringType(), True),StructField("image", StringType(), True),StructField("ingredients", StringType(), True),StructField("name", StringType(), True),StructField("prepTime", StringType(), True),StructField("recipeYield", StringType(), True),StructField("url", StringType(), True)])

    try:
        #df_read_data = spark.read.format(config.get("CONFIG", "source_data_format")).load(config.get("CONFIG", "raw_file_path"),header=True,schema=config.get("CONFIG", "input_schema"))
        df_read_data = spark.read.format(config.get("CONFIG", "source_data_format")).load(config.get("CONFIG", "raw_file_path"), header=True, schema=input_schema)
        df_read_data.count()

        return df_read_data
    except Exception as e:
        print("data read not successful, check log file for more information")
        logger.info(e)
        sys.exit(1)

def transform_data(df, spark,config):

    '''

    :param df:
    :param spark:
    :param config:
    :return: Dataframe
    '''

    df.printSchema()
    # Extracting only recipes that have beef as one of the ingredients.
    try:
        logger.info("Extract only recipes that have beef as one of the ingredients.")
        df_ingredient = df.where(F.lower(F.col("ingredients")).contains("beef"))
        #extracting cooktime from ISO format
        logger.info("extracting cooktime from ISO format")
        dataframe_transform = df_ingredient.withColumn('cookTime',
                                       F.coalesce(F.regexp_extract('cookTime', r'(\d+)M', 1).cast('int'),
                                                              F.lit(0))).withColumn('prepTime', F.coalesce(F.regexp_extract('prepTime', r'(\d+)M', 1).cast('int'), F.lit(0)))

        #calculating total cooktime
        logger.info("calculating total cooktime")
        df_total_cooktime = dataframe_transform.withColumn("total_cook_time", col("cookTime") + col("prepTime"))

        #applying different conditions
        new_df = df_total_cooktime.withColumn("difficulty", F.when(F.col("total_cook_time") < 30, 'easy').otherwise(
        F.when((F.col("total_cook_time") >= 30) & (F.col("total_cook_time") <= 60), 'medium').otherwise(
            F.when(F.col("total_cook_time") > 60, 'hard').otherwise(F.col("total_cook_time")))))

        #calculating average cooktime
        logger.info("calculating average cooktime")
        dfAverage = new_df.groupBy(F.col("difficulty")).agg(F.avg("total_cook_time").alias("avg_total_cooking_time"))
        dfAverage.show()

        return dfAverage
    except Exception as e:
        print("The process failed")
        logger.info(e)
        sys.exit(1)



def write_output(df, spark, config):
    '''

    :param df:
    :param spark:
    :param config:
    :return: File
    '''

    try:
        logger.info("Saving file to destination")
        df.coalesce(1).write.mode("overwrite").format(config.get("CONFIG", "target_data_format")).option("header", "true").save(config.get("CONFIG", "output_path"))
    except Exception as e:
        print("data write has failed")
        logger.info(e)
        sys.exit(1)



def main():
    # start spark code
    global logger, config_path
    config_path = r'/Users/jpanda/Desktop/Books/Code/jayitapanda17-data-engineering-test/config/task.cfg'
    try:
        config = get_config(config_path)
    except Exception as e:
        print("reading config has failed")
        raise
    LogHome = "{}/logs/{}".format(config.get("CONFIG", "base_location"), config.get("CONFIG", "application_name"))
    if not os.path.isdir(LogHome):
        sys.stdout.write("Log directory doesn't exist. Creating ...\n")
        print(os.getcwd())
        os.makedirs(LogHome)

    current_datetime = datetime.now().strftime('%Y%m%d%H%M%S')
    LogFileName = "{}/{}.{}.log".format(LogHome, "load_" + config.get("CONFIG", "process_name"), current_datetime)
    logger.info(LogFileName)
    logger = getLogger(config, LogFileName)

    #building spark session
    logger.info("Building spark session")
    spark = SparkSession.builder.appName(config.get("CONFIG", "application_name")).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")


    logger.info("Starting spark application")

    logger.info("Checking input file format, expecting JSON")
    validate_file_type(config, logger)

    logger.info("------------- READING INPUT DATA ---------------")
    df = read_input(spark, config)

    logger.info("------------- RUNNING SOURCE DATA QUALITY CHECKS---------------")
    logger.info("Checking column count")
    validate_num_source_column_count(df, spark, config, logger)

    logger.info("----------------PROCESSING AND TRANSFORMING-------------------")
    df_average = transform_data(df, spark, config)

    logger.info("-----------------RUNNING DATA QUALITY CHECKS AFTER TRANSFORMATION------------------")
    logger.info("Checking target row count")
    validate_target_row_count(df_average, spark, config, logger)

    logger.info("Checking target column count")
    validate_target_col_count(df_average, spark, config, logger)

    logger.info("-----------------WRITING DATA TO OUTPUT------------------")
    write_output(df_average, spark,config)
    logger.info("Ending spark application")
    # end spark session
    spark.stop()


# Starting point for PySpark
if __name__ == '__main__':
    main()
    sys.exit(1)










