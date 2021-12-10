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
        print('\t', bold + blue + data + reset)


class streamer(object):

    @classmethod
    def pick_device(cls):
        device_list = ['Server-A', 'Server-B', 'Server-C', 'Server-D', 'Server-E', 'Server-F']
        pick = random.choice(device_list)
        return pick

    @classmethod
    def init_connect(cls, props):
        try:
            instance = mysql.connect(**props, connect_timeout=10)
            if instance.is_connected():
                # logging.log(f'Connected to {db} database successfully')
                return instance
        except Error as connect_err:
            logging.fail(f'Found error while connecting to {db} database server.')

    @classmethod
    def insert_query(cls, props):
        try:
            query = f"insert into iot_logs (device, timestamp, uniqueref) values " \
                    f"('{cls.pick_device()}',now(), (select replace(uuid(),'-',''))) "
            insert_connect = cls.init_connect(props)
            insert_cursor = insert_connect.cursor()
            insert_cursor.execute(query)
            insert_connect.commit()
            logging.log('New row inserted')
        except Error as write_err:
            logging.fail(f'Found error while inserting data into {db} database.')

    @classmethod
    def read_data(cls, props):
        try:
            query = 'select @@port'
            read_connect = cls.init_connect(props)
            read_cursor = read_connect.cursor()
            read_cursor.execute(query)
            for x in read_cursor.fetchone():
                if x == 3333:
                    logging.log(f'Reading data from Primary Server')
                if x == 4444:
                    logging.log(f'Reading data from Replica Server')
        except Error as read_err:
            logging.fail(f'Found error while reading data')

    @staticmethod
    def run():
        while True:
            streamer.insert_query(write_config)
            streamer.read_data(read_config)


if __name__ == '__main__':
    streamer.run()
