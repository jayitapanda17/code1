#importing modules
import json
from task_config import *
import os

from task import *
def validate_file_type(config,logger):
    '''
    To check input file format
    :param1 config:
    :param2 logger:
    :return: None
    '''
    for fname in os.listdir(config.get("CONFIG","base_location")+'/input'):
        if fname.endswith('.json'):
            print(fname)
        else:
            logger.info("Received unexpected file format, exiting...\n")
            sys.exit(1)
    logger.info("All the source files are in expected format")

def validate_num_source_column_count(df, spark, config, logger):
    '''

    :param df:
    :param spark:
    :param config:
    :param logger:
    :return: None
    '''

    #GET column count
    col_count = len(list(df.columns))
    if col_count == config.get("DATA_QUALITY_CHECKS", "expected_col_count"):
        logger.info("Expected column count did not match with config file")
        sys.exit(1)

    else:
        logger.info("Column count matched, data quality check passed")


def validate_target_row_count(df, spark, config, logger):
    '''

    :param df:
    :param spark:
    :param config:
    :param logger:
    :return: None
    '''

    #GET column count
    row_count = df.count()
    if row_count == config.get("DATA_QUALITY_CHECKS", "expected_target_row_count"):
        logger.info("Expected target row count did not match with config file")
        sys.exit(1)

    else:
        logger.info("Target row count matched, data quality check passed")

def validate_target_col_count(df, spark, config, logger):
    '''

    :param df:
    :param spark:
    :param config:
    :param logger:
    :return: None
    '''

    #GET column count
    target_col_count = len(list(df.columns))
    if target_col_count == config.get("DATA_QUALITY_CHECKS", "expected_target_col_count"):
        logger.info("Expected expected_target_col_count did not match with config file")
        sys.exit(1)

    else:
        logger.info("Target column count matched, data quality check passed")
