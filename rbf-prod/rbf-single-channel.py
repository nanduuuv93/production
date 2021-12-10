from datetime import datetime
import mysql.connector as mysql
from mysql.connector import errorcode, Error, DatabaseError, DataError
import datetime
import time
import logging
from logging import handlers
import pathlib as pl

log_dir = pl.Path(r'/var/logs/rbf-logs')
log_file = 'rbf-prod.log'
full_log_path = log_dir.joinpath(log_file)
logger = logging.getLogger('all-com-replica')
logger.setLevel(logging.INFO)
log_format = logging.Formatter(f'%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')

consoleHandler = logging.StreamHandler()
consoleHandler.setLevel(logging.INFO)
consoleHandler.setFormatter(log_format)
logger.addHandler(consoleHandler)

fileHandler = handlers.TimedRotatingFileHandler(full_log_path, when='D', interval=1, backupCount=3)
fileHandler.setLevel(logging.WARNING)
fileHandler.setFormatter(log_format)
logger.addHandler(fileHandler)

logDate = datetime.datetime.now().strftime("%d-%b-%y_%H:%M:%S")
con_date = datetime.datetime.now().strftime("%d-%b-%y %H:%M:%S")

primary = {'host': '74.208.93.227', 'port': '3306', 'user': 'replicator', 'password': 'Replica++1', 'database': 'mysql'}
replica = {'host': 'localhost', 'port': '3306', 'user': 'dbaStack', 'password': 'dbaStack', 'database': 'mysql'}

db = 'Allcom-Replica'

logger.info(f'Starting Replication Break Fixer in {db} server ')

checkList: dict = {
    'connect': 0, 'repl_status': 0, 'master_status': 0, 'slave_status': 0, 'change_master': 0
}

error_list: dict = {
    'err_code': '', 'err_message': ''
}

change_master: dict = {
    'master_host': 0, 'master_port': 0, 'master_user': 0, 'master_password': 0, 'master_log_file': 0, 'master_log_pos': 0
}

channel_name: list = ['connect_usa', 'connect_uk']


class setChange(object):

    @staticmethod
    def addHost(host):
        change_master['master_host'] = host

    @staticmethod
    def addPort(port):
        change_master['master_port'] = port

    @staticmethod
    def addUser(user):
        change_master['master_user'] = user

    @staticmethod
    def addPass(pwd):
        change_master['master_password'] = pwd

    @staticmethod
    def addFile(logfile):
        change_master['master_log_file'] = logfile

    @staticmethod
    def addPos(log_pos):
        change_master['master_log_pos'] = log_pos


class fixer(object):

    @staticmethod
    def setChecklist(key, value):
        checkList[key] = value

    @staticmethod
    def getChecklist(key):
        return checkList.get(key)

    @classmethod
    def init_connect(cls, dbConfig, dbName=None):
        try:
            cnx = mysql.connect(**dbConfig, connect_timeout=5)
            if cnx.is_connected():
                logger.debug(
                    f'All good, connected to {dbName}-Server successfully.')
                cls.setChecklist('connect', 1)
                return cnx
        except Error as mariadb_connect_err:
            logger.error(f'Found error while connecting {dbName} database instance.')

    @classmethod
    def get_replica_status(cls, dbConfig, dbName=None):
        try:
            cnx2 = cls.init_connect(dbConfig, 'Replica')
            if cnx2.is_connected():
                get_replica_cursor = cnx2.cursor()
                get_replica_cursor.execute(f"show slave status")
                for status in get_replica_cursor.fetchall():
                    if status[10] == 'Yes' and status[11] == 'Yes':
                        cls.setChecklist('repl_status', 1)
                    else:
                        cls.get_master_coordinates(cnx2, 'Primary')
                        cls.analyzer(cnx2)
                        cls.setChecklist('repl_status', 0)
        except Error as mysql_replica_err:
            logger.error('Found error while collecting replication status from replica server.')

    @classmethod
    def get_master_coordinates(cls, master_con, dbName=None):
        try:
            time.sleep(1)
            if master_con.is_connected():
                get_master_cursor = master_con.cursor()
                get_master_cursor.execute(f"show slave status")
                if get_master_cursor.with_rows:
                    for info in get_master_cursor.fetchall():
                        setChange.addHost(info[1])
                        setChange.addPort(info[3])
                        setChange.addUser(info[2])
                        setChange.addFile(info[5])
                        setChange.addPos(info[6])
                master_con.commit()
        except Error as mysql_master_cords:
            logger.error('Found error while collecting master server.')

    @classmethod
    def analyzer(cls, analyzer_con):
        try:
            time.sleep(1)
            logging.warn('Analyzing issues caused failure of replication thread.')
            if analyzer_con.is_connected():
                analyzer_cursor = analyzer_con.cursor()
                analyzer_cursor.execute(f"show slave status")
                for value in analyzer_cursor.fetchall():
                    logger.warning(f"Last_Errno : {str(value[18])}")
                    logger.warning(f"Last_Error : {str(value[19])}")
                    if value[18] == 1062 and value[11] == 'No' and value[10] == 'Yes':
                        cls.fixDuplicateErr(analyzer_cursor)
                    if value[11] == 'No' and value[10] == 'Yes':
                        logger.warning('SQL_THREAD is stopped, while IO_THREAD is a active, starting SQL '
                                       'thread.')
                        cls.fixReplicaThread(analyzer_con)
                    if value[10] == 'Connecting' and value[11] == 'Yes':
                        logger.warning(
                            'Primary server seems to be away, trying to reach Primary Server, after 3 attempts'
                            ' RBF might stop')
                    if value[11] == 'No' and value[10] == 'No':
                        logger.warning('SQL_THREAD and IO_THREAD are stopped, starting threads.')
                        cls.fixReplicaThread(analyzer_con)
                    if value[10] == 'No' and value[11] == 'Yes':
                        logger.warning('IO_THREAD is stopped, while SQL_THREAD is active, starting io_thread.')
                        cls.fixReplicaThread(analyzer_con)
                    if value[34] in (1236, 1032):
                        logger.warning(f"Last_IO_Error : {str(value[34])}")
                        logger.warning(f"Last_IO_Err_Desc : {str(value[35])}")
                        cls.fixBinaryLogError(analyzer_cursor)
        except Error as mariadb_repl_fail:
            logger.error(f'Failed analyzing.')

    @classmethod
    def fixBinaryLogError(cls, replica_cursor):
        try:
            """ERROR_TYPE = 'IO' , Err_No : 1236, 
             Err_msg: This error occurs when the slave server required binary log for replication no 
            longer exists on the master database server. Got fatal error 1236 from master when reading data from 
            binary log: 'Could not find first log file name in binary log index file' """
            time.sleep(1)
            con = cls.init_connect(primary, 'Primary')
            if con.is_connected():
                cnx_cursor = con.cursor()
                cnx_cursor.execute(f'show master status')
                value = cnx_cursor.fetchall()
                logger.warning('Finding existing coordinates from Primary-Server, setting replication with existing'
                               ' coordinates.')
                for _val in value:
                    replica_cursor.execute(f"stop slave")
                    replica_cursor.execute(f"change master to master_host='{change_master['master_host']}',"
                                           f"master_port = {change_master['master_port']}, "
                                           f"master_user = '{change_master['master_user']}',"
                                           f"master_password = 'Replicator++1',"
                                           f"master_log_file = '{_val[0]}',"
                                           f"master_log_pos = {_val[1]}")
                    replica_cursor.execute(f"start slave")
                    logger.warning('Replication started with available coordinates, there might be a gap occurred. '
                                   'Please fix it using backup or find and run binary logs between time broken '
                                   'and time taken to restart the replication.')
            con.commit()
        except Error as fix_binary_err:
            logger.error('Found error while finding binary log error')

    @classmethod
    def fixDuplicateErr(cls, cursor):
        try:
            logger.warning(f"Found duplicate key error in Replica-Server, restoring replication.")
            cursor.execute(f"stop slave sql_thread")
            cursor.execute(f'SET GLOBAL SQL_SLAVE_SKIP_COUNTER = 1')
            cursor.execute(f"start slave")
            logger.warning(f'Replication restored with Primary Server.')
        except Error as fixDuplicateErr:
            logging.error('Found error while finding duplicate error.')

    @classmethod
    def fixReplicaThread(cls, con):
        try:
            time.sleep(1)
            change_statement = f"change master to master_host='{change_master['master_host']}'," \
                               f"master_port={change_master['master_port']}," \
                               f"master_user='{change_master['master_user']}'," \
                               f"master_password='Replicator++1'," \
                               f"master_log_file='{change_master['master_log_file']}'," \
                               f"master_log_pos={change_master['master_log_pos']}"
            cursor = con.cursor()
            cursor.execute(f"stop slave")
            time.sleep(1)
            cursor.execute(change_statement)
            logger.warning(f'Connection restored with Primary Server,starting replication channel.')
            time.sleep(1)
            cursor.execute(f"start slave")
            logger.warning(f'Replica-Server started successfully from broken '
                           f'coordinate, please check the log file for further analysis.')
            con.commit()
        except Error as fixDuplicateErr:
            logging.error('Found error while finding duplicate error.')

    @classmethod
    def fixReconnect(cls, con):
        try:
            time.sleep(1)
            change_statement = f"change master to master_host='{change_master['master_host']}'," \
                               f"master_port={change_master['master_port']}," \
                               f"master_user='{change_master['master_user']}'," \
                               f"master_password='Replica++1'," \
                               f"master_log_file='{change_master['master_log_file']}'," \
                               f"master_log_pos={change_master['master_log_pos']}"
            cursor = con.cursor()
            cursor.execute(f"stop slave")
            time.sleep(1)
            cursor.execute(change_statement)
            logger.warning(f'Connection restored with Primary Server,'
                           f' starting replication channel.')
            time.sleep(1)
            cursor.execute(f"start slave")
            logger.warning(f'Replica-Server started successfully from broken '
                           f'coordinate, please check the log file for further analysis.')
            con.commit()
        except Error as fixDuplicateErr:
            logging.error('Found error while finding duplicate error.')

    @staticmethod
    def run():
        fixer.get_replica_status(replica, 'Replica')


if __name__ == '__main__':
    fixer.run()

logging.info(f'Replication break fixer completed in {db} Production Database.')