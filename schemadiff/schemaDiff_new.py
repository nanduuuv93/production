from datetime import datetime
import mysql.connector as mysql
from mysql.connector import errorcode, Error, DatabaseError, DataError
import datetime
import logging
from schemadiff.queries import grep_columns, compute_day_1, compute_day_2, compare_data, overwrite
import smtplib, ssl

logger = logging.getLogger('schema-diff')
logger.setLevel(logging.DEBUG)
log_format = logging.Formatter(f'%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')

consoleHandler = logging.StreamHandler()
consoleHandler.setLevel(logging.DEBUG)
consoleHandler.setFormatter(log_format)
logger.addHandler(consoleHandler)

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

checklist_1 = set()
checklist_dict = {}


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
        print('\t', bold + yellow + data + reset)


# logger.info('Schema Difference Comparison')

db_config = {'host': '10.3.0.70', 'port': '4006', 'user': 'app_write', 'password': 'App++1', 'database': 'logs'}


class mail(object):

    @classmethod
    def sent(cls, attach=None):
        port = 465  # For SSL
        smtp_server = "smtp.gmail.com"
        sender_email = "dbaalerts.v21@gmail.com"  # Enter your address
        receiver_email = "vexiv72171@veb27.com"  # Enter receiver address
        password = "Mysql++1"
        message = f"""
        Subject: Schema Difference Comparison in logs server
        
                 1 - logs.check_proxy table is altered - Column Removed
	             2 - logs.check_proxy2 table is altered - Column Removed
	             
        """
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
            server.login(sender_email, password)
            server.sendmail(sender_email, receiver_email, message)


class schemaDiff(object):

    @staticmethod
    def add_columns(col):
        checklist_1.add(col)

    @classmethod
    def init_connect(cls, dbConfig):
        try:
            cnx = mysql.connect(**dbConfig, connect_timeout=5)
            if cnx.is_connected():
                return cnx
        except Error as mariadb_connect_err:
            logger.error(f'Found error while connecting logs-server database instance.')

    @classmethod
    def compute_data(cls, config):
        try:
            cd_con = cls.init_connect(config)
            cd_cur = cd_con.cursor()
            cd_cur.execute(compute_day_2)
            cd_con.commit()
            logger.info('Table component updated.')
        except Error as computer_err:
            logger.error('Found error while computing data.')

    @classmethod
    def compare_data(cls, config):
        try:
            cr_con = cls.init_connect(config)
            cr_cur = cr_con.cursor()
            cr_cur.execute(compare_data)
            mail.sent()
            for data in cr_cur.fetchall():
                logging.subLog(f'{data[0]} - {data[1]} - {data[2]}')

        except Error as compare_err:
            logger.error('Found error while computing data')

    @classmethod
    def overwrite_data(cls, config):
        try:
            od_con = cls.init_connect(config)
            od_cur = od_con.cursor()
            od_cur.execute(overwrite)
            od_con.commit()
        except Error as overwrite_err:
            logger.error('Found error while overwriting day1 data.')

    @classmethod
    def report(cls):
        cls.compute_data(db_config)
        cls.compare_data(db_config)
        cls.overwrite_data(db_config)


if __name__ == '__main__':
    schemaDiff.report()
