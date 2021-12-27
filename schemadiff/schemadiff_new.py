import os
import time
from datetime import datetime
import mysql.connector as mysql
from mysql.connector import errorcode, Error, DatabaseError, DataError
import datetime
import logging
from schemadiff.queries import grep_columns, compute_day_1, compute_day_2, compare_data, overwrite, compute_day_2_alt
import smtplib, ssl
import pathlib as pathlib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import shutil

logger = logging.getLogger('schema-diff')
logger.setLevel(logging.DEBUG)
log_format = logging.Formatter(f'%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')

consoleHandler = logging.StreamHandler()
consoleHandler.setLevel(logging.DEBUG)
consoleHandler.setFormatter(log_format)
logger.addHandler(consoleHandler)

logTime = datetime.datetime.now().strftime("%Y_%m_%d.%H_%M_%S")


reports_path = pathlib.Path('/home/nandagopal/reports/')

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
report_list = []
result_set = []


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

db_config = {'host': 'localhost', 'port': '3306', 'user': 'schemaUser', 'password': 'schemaUser@@1',
             'database': 'write_bucket'}
pathlib.Path(reports_path)


class mailer(object):

    @classmethod
    def findReports(cls):
        try:
            files_in_attachments = (entry for entry in reports_path.iterdir() if entry.is_file())
            for file in files_in_attachments:
                if file.name.endswith('.txt'):
                    return [pathlib.Path(file), file.name]
        except (FileNotFoundError, FileExistsError, IndexError) as FNFE:
            logger.error('Seems to be file does not exists in the AUTH directory, please check.')

    @classmethod
    def deleteAttachedFile(cls, filename):
        try:
            os.chdir(reports_path)
            if pathlib.Path(filename).is_file():
                logger.info(f"Removing attachment: {filename.name}")
                pathlib.Path.unlink(filename)
        except (FileNotFoundError, FileExistsError, IndexError) as file_not_found:
            logger.error('Seems to be file does not exists in the AUTH directory, please check.')

    @classmethod
    def renameFile(cls, old, new):
        try:
            os.rename(old, new + '.old')
        except Error as rename_err:
            logger.error('Found error while renaming file from path,')

    @classmethod
    def fileAttachments(cls):
        try:
            pathlib.Path(reports_path)
            files_in_reports = (entry for entry in reports_path.iterdir() if entry.is_file())
            for file in files_in_reports:
                if file.name.endswith('.txt'):
                    file_name = pathlib.Path(file.name)
                    logger.info(f'Found Attachment : {file_name}')
        except (FileNotFoundError, FileExistsError, IndexError) as file_not_found:
            logger.error('Seems to be file does not exists in the AUTH directory, please check.')


class mail(object):
    pathlib.Path(reports_path)

    @classmethod
    def newest(cls):
        files = os.listdir(reports_path)
        paths = [os.path.join(reports_path, basename) for basename in files]
        return max(paths, key=os.path.getctime)

    @classmethod
    def addReport(cls, report):
        report_list.append(report)

    @classmethod
    def latestReport(cls):
        try:
            os.chdir(reports_path)
            list_of_files = pathlib.Path.glob(reports_path, '*.txt')
            latest_file = max(list_of_files, key=os.path.getctime)
            # logging.subLog(f'Latest File : {latest_file.name}')
            return latest_file
        except FileNotFoundError as reportFinder:
            logger.error('Found error while capturing latest report attachment from reports path.')
        except ValueError as valueErr:
            logger.debug('No reports found.')

    @classmethod
    def attachFiles(cls, attach=None):
        try:
            pathlib.Path(reports_path)
            logging.subLog('Found these files in reports directory.')
            for x in reports_path.rglob('*.txt*'):
                cls.addReport(x.name)
                logging.subLog(f'\t {x.name}')
        except Error as attach_files:
            logger.error('Found error while attaching files to mail.')

    @classmethod
    def sentMail(cls, from_address, to_address):
        try:
            os.chdir(reports_path)
            mail_context = MIMEMultipart()
            mail_context['From'] = from_address
            mail_context['To'] = to_address
            mail_context['Subject'] = 'Schema Alteration Notification'
            body_content = "Schema Alteration Alert \n Database Server : 10.95.60.192"
            mail_context.attach(MIMEText(body_content, 'plain'))
            attachment = open(cls.latestReport())
            payload = MIMEBase('application', 'octet-stream')
            payload.set_payload(attachment.read())
            encoders.encode_base64(payload)
            payload.add_header('Content-Disposition', "attachment; filename= %s" % attachment)
            mail_context.attach(payload)
            s = smtplib.SMTP('smtp.gmail.com', 587)
            s.starttls()
            s.login(from_address, "Mysql++1")
            text = mail_context.as_string()
            s.sendmail(from_address, to_address, text)
            logger.info(f'An email is dispatched with attachment {cls.latestReport().name}')
            logger.debug(f'Latest Report File : {cls.latestReport().name}')
            logger.info(f'Reports Directory : {reports_path}')
            s.quit()
        except TypeError as sent_mail:
            logger.debug('No attachments found to be dispatched.')

    @classmethod
    def sentMail2(cls):
        try:
            sender_email = "dbaalerts.v21@gmail.com"
            receiver_email = "dbaalerts.v21@gmail.com"
            os.chdir(reports_path)
            mail_context = MIMEMultipart()

            mail_context['Subject'] = 'Schema Alteration Notification'
            body_content = "Schema Alteration Alert \n Database Server : 10.95.60.192"
            mail_context.attach(MIMEText(body_content, 'plain'))

            filename = cls.latestReport().name
            with open(reports_path/filename, 'rb') as attachment:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(attachment.read())
            encoders.encode_base64(part)

            part.add_header(
                "Content-Disposition",
                f"attachment; filename= {filename}",
            )
            mail_context.attach(part)
            s = smtplib.SMTP('smtp.gmail.com', 587)
            s.starttls()
            s.login(sender_email, "Mysql++1")
            text = mail_context.as_string()
            s.sendmail(sender_email, receiver_email, text)
            logger.info(f'An email is dispatched with attachment {cls.latestReport().name}')
            logger.debug(f'Latest Report File : {cls.latestReport().name}')
            logger.info(f'Reports Directory : {reports_path}')
            s.quit()
        except TypeError as sent_mail:
            logger.debug('No attachments found to be dispatched.')


class schemaDiff(object):

    @staticmethod
    def add_result(data):
        result_set.append(data)

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
            compute_connect = cls.init_connect(config)
            compute_cursor_ = compute_connect.cursor()
            compute_cursor_.execute(compute_day_2)
            compute_connect.commit()
            logger.info('Loading Table Statistics Component')
        except Error as computer_err:
            logger.error('Found error while computing data.')

    @classmethod
    def compare_data(cls, config):
        try:
            cr_con = cls.init_connect(config)
            cr_cur = cr_con.cursor()
            cr_cur.execute(compare_data)
            for data in cr_cur.fetchall():
                if data is None:
                    logger.debug('No data found to be attached. Exiting Code!.')
                    exit()
                    quit()
                else:
                    logging.subLog(f'{data[0]} - {data[1]} - {data[2]}')
                    result = f'{data[0]} - {data[1]} - {data[2]}'
                    schemaDiff.add_result(result)
                    result_set.append(result)
                    with open(f'{reports_path}/schemaDiff_{datetime.datetime.now().strftime("%Y_%m_%d.%H:%M:%S")}.txt',
                              'a') as writer:
                        writer.writelines(result + '\n')
                    mail.sentMail2()

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
        time.sleep(2)
        cls.compare_data(db_config)
        cls.overwrite_data(db_config)


if __name__ == '__main__':
    schemaDiff.report()


