from asyncio.log import logger
from logging.handlers import TimedRotatingFileHandler
from logging import handlers
import sys
import pika
import base64
import os
from os import listdir, walk, replace
from os.path import join,splitext, exists
from time import sleep
import shutil
import logging
import yaml
import filetype
import traceback


"""Global Variable Declaration"""
global sleepTime
global inputPath
global archival_directory
global allowed_file_types
global files_to_skip
global credentials
global parameters
global connection
global channel
global connect_open


"""Reading Configuration file"""
with open('send_config.yaml', 'r') as file:
    config_file = yaml.safe_load(file)
    inputPath = r"{}".format(config_file["configuration"]["source_directory"])
    sleepTime = config_file["configuration"]["error_sleep_time"]
    archival_directory =  r"{}".format(config_file["configuration"]["long_term_storage"])
    allowed_file_types = list(config_file["configuration"]["allowed_file_types"])
    rabbit_queue = str(config_file["configuration"]["queue"])
    rabbit_host = str(config_file["configuration"]["host"])
    port = config_file["configuration"]["port"]
    devMode = config_file["configuration"]["devMode"]
    log_file_name = join("logfiles", str(config_file["configuration"]["log_file_name"]))
    username = config_file["credentials"]["username"]
    password = config_file["credentials"]["password"]



"""Initializing variables"""
files_to_skip = []
connect_open = False

folder_for_logs = 'logfiles'
credentials = pika.PlainCredentials(username, password)
parameters = pika.ConnectionParameters(rabbit_host,
                                            port,
                                            '/',
                                            credentials)

"""Initializing Logging Config"""
if not os.path.exists('logfiles'):
    os.makedirs('logfiles')

log = logging.getLogger('')
log.setLevel(logging.DEBUG)
format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(format)
log.addHandler(ch)
fh = handlers.TimedRotatingFileHandler(log_file_name, when="M", interval=1)
fh.suffix = "%Y-%m-%d-%H-%M.log"
fh.setFormatter(format)
log.addHandler(fh)
logging.getLogger("pika").propagate = False


def createConnection():
    """Creates initial connection to RabbitMQ Instance"""
    connectionSuccessful = False
    if devMode == False:
        while connectionSuccessful == False:
            logging.info("Initiating Connection to RabbitMQ Instance")
            try:
                connection = pika.BlockingConnection(parameters)
                channel = connection.channel()
                channel.queue_declare(queue=rabbit_queue)
                connectionSuccessful = True
                logging.info("Connection to RabbitMQ sucessful")
                    
            except:
                logging.error("Connection to RabbitMQ unsucesfull, retying in 5 seconds")
                sleep(sleepTime)
    else:
        while connectionSuccessful == False:
            logging.info("Dev Mode: Initiating Connection to RabbitMQ Instance")
            try:
                connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
                channel = connection.channel()
                channel.queue_declare(queue=rabbit_queue)
                connectionSuccessful = True
            except:
                logging.error("Dev Mode: Connection to RabbitMQ unsucesfull, retying in 5 seconds")
                sleep(sleepTime)

def checkConnection():
    """Checks if the connection is open, and if the connection is open returns TRUE, else FALSE"""
    if devMode == False:
        try:
            pika.BlockingConnection(parameters)
            logging.debug("Connection active")
            connect_open = True
        except:
            connect_open = False
            logging.error(f"Connection check failed, retrying in {sleepTime} seconds")
            sleep(sleepTime)
        return connect_open
    else:
        try:
            pika.BlockingConnection(pika.ConnectionParameters('localhost'))
            logging.debug("Connection active")
            connect_open = True
        except:
            logging.error(f"Connection check failed, sleeping {sleepTime} seconds")
            sleep(sleepTime)
        return connect_open


def checkDirectory(inputdir, outputdir):
    """
    This function checks that the directories passed into the config file are valid.
    """
    isExistInput = os.path.exists(inputdir)
    isExistOutput = os.path.exists(outputdir)
    if isExistInput == False or isExistOutput == False:
        logging.error("Incorrect path in source or storage directories, please correct and restart program")
        sys.exit()
        
def getFiles(dir):
    files = []
    for r, d, f in walk(dir):
        for file in f:
            if file in files_to_skip:
                continue
            else:
                # scanResults = fileScan.from_file()
                kind = filetype.guess(join(r, file))
                approvedExtension = file.endswith(tuple(allowed_file_types))
                if kind is not None:
                    scanResults = kind.mime
                else:
                    scanResults = ' '
                if bool(approvedExtension) == True and 'application' not in str(scanResults):
                    files.append(join(r, file))
                else:
                    logger.info(f"Ignoring {file} because fileype is not allowed")
                    logger.info(f'Adding {file} to skip list')
                    files_to_skip.append(file)
    return files

def checkForFiles(dir):
    """
    This function checks if there are new files in the directory
    if there are no files it will return NONE.
    """
    files = getFiles(dir)
    if len(files) > 0:
        return files
    else:
        print(f"No new files will check in {sleepTime} seconds")
        logging.info(f"No new files will check in {sleepTime} seconds")
        return None

def getFileExtension(file):
    """
    Helper function to return the Filename and extension as a Tuple
    
    """
    file_name, file_extension = splitext(file)
    return file_name, file_extension

def encodeFiles(file_name):
    """
    Encodes file into base64 for sending, returns a bite-string
    """
    try:
        with open(file_name, "rb") as file:
            encoded_string = base64.b64encode(file.read()) 
        return encoded_string
    except:
        print(f"File {file_name}, not found")

def createMesseage(file,file_name,file_extension):
    """
    Takes as input, a the file/bite-string, the file name and the extension and stores it in a dictionary for sending over. 
    """
    message = {
    "File_Name": file_name ,
    "Extension": file_extension,
    "Data": file
    }
    return str(message)

def find2nd(dir):
    """ This funciton is used to remove the (source drive or computer) from the path and tell the move file function how to create the target dir
    For example: \\server\machine1\file.txt to \machine1\file.txt
    """
    if dir.startswith('\\'):
        if dir.count('\\') > 2:
            first = dir[2:].find('\\')
            second = dir.find('\\',first + 1) 
        else:
            second = dir[2:]
    else:
        first = dir.find('\\')
        second = dir.find('\\',first + 1) 
    return second

def moveFile(dir):
    """
    This function will move the file to the archival directory, it takes in the source directory of the file as a input,
    and used the archival_directory global variable to know where to send the file
    """
    folderStart = find2nd(dir)
    fileStart = dir.rfind("\\")
    sourceDir = dir[folderStart:fileStart] 
    fileName = dir[fileStart:]
    target = archival_directory + sourceDir
    print(target)
    if not os.path.exists(target):
        os.makedirs(target)
    try:
        shutil.move(dir,target)
        logging.info(f"Moved {fileName}' to long term storage")
    except shutil.Error:
        logging.debug(f"Destination path already exists, ")
        shutil.move(dir,target + '_new')

    
def main(dir):
    """
    Main function which checks if a checks if there are files in the directory being scanned,
    then check if the connection is active, creates a new connection regardless, encodes the file,
    gets the file extension and name, creates a messeage and sends it over to the queue.
    """
    while True:
        try:
        
            files = checkForFiles(dir)
            if files is not None:
                for f in files:
                    if checkConnection() == True:
                        if devMode == False:
                            connection = pika.BlockingConnection(parameters)
                            channel = connection.channel()
                            channel.queue_declare(queue=rabbit_queue)
                        else:
                            connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
                            channel = connection.channel()
                            channel.queue_declare(queue=rabbit_queue)
                        encodedFile = encodeFiles(f)
                        fileName, extension = getFileExtension(f)
                        subtractFromDir = len(inputPath)
                        dataDict = createMesseage(encodedFile,fileName[subtractFromDir:],extension)
                        channel.basic_publish(exchange='',
                                            routing_key=rabbit_queue,
                                            body=dataDict)
                        moveFile(f)
                        print("[X] Sent ", dataDict)
                        connection.close()
                    else:
                        createConnection()
                        continue
            else:
                sleep(5)
                continue
        except pika.exceptions.ConnectionClosedByBroker as rabbitShutdown:
            logging.error("Connection to RabbitMQ lost, check that RabbitMQ instance is still working")
            logging.error(traceback.format_exc())
            logging.info(f"Will Attempt Reconection in {sleepTime} seconds")
            sleep(sleepTime)
        except:
            logging.error("Fatal Error")
            logging.error(traceback.format_exc())
            logging.info(f"Will Attempt Reconection in {sleepTime} seconds")
            sleep(sleepTime)
        continue


if __name__ == '__main__':
    try:
        checkDirectory(inputPath,archival_directory)
        checkConnection()
        main(inputPath)
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)