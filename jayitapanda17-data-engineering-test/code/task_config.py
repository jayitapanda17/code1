import os
import configparser

def get_config(path):
    '''

    :param path:
    :return: config
    '''
    try:
        config = configparser.ConfigParser()
        config.read_file(open(path))

        return config
    except Exception as e:
        print("Exception in get_config  {}".format(e))