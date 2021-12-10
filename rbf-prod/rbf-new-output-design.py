from datetime import datetime
import mysql.connector as mysql
from mysql.connector import errorcode, Error, DatabaseError, DataError
import datetime
import time
import pathlib as pl
import random

write_config = {'host': '10.3.12.92', 'port': '4006', 'user': 'app_dev', 'password': 'App++1', 'database': 'logs'}
read_config = {'host': '10.3.12.92', 'port': '4008', 'user': 'app_dev', 'password': 'App++1', 'database': 'logs'}

db = 'prod-logs'

logTime = datetime.datetime.now().strftime("%Y_%m_%d.%H_%M_%S")

italic = '\33[3m'
# bold = '\33[1m'
bold = ''
reset = '\033[0m'
yellow = '\33[33m'
red = '\33[31m'
green = '\033[92m'
blue = '\033[94m'
violet = '\33[35m'
cyan = '\u001b[36m'
white = '\u001b[37m'


class logger(object):

    @staticmethod
    def title(data):
        print(bold + green + data + reset)

    @staticmethod
    def log(data):
        print(bold + red + 'console ):' + reset, bold + blue + data + reset)

    @staticmethod
    def subLog(data):
        print('\t', bold + blue + data + reset)


class logging(object):

    @staticmethod
    def formatter(logState, logData):
        print(
            bold + f"{red + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + reset} - {bold + logState + reset} - "
                   f"{bold + green + logData}" + reset)

    @classmethod
    def log(cls, logData):
        return cls.formatter(blue + 'Log', logData)

    @classmethod
    def warn(cls, logData):
        return cls.formatter(red + 'Warning', logData)

    @classmethod
    def err(cls, logData):
        return cls.formatter(red + 'Error', red + logData + reset)

    @classmethod
    def info(cls, logData):
        return cls.formatter(cyan + 'Info' + reset, logData)

    @classmethod
    def fail(cls, logData):
        return cls.formatter(red + 'Failed', red + logData + reset)

    @staticmethod
    def subLog(data):
        print('\t\t'+bold + yellow + data + reset)


logging.log('Replication Break Fixer')

