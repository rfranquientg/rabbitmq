from asyncio.log import logger
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
# import magic

global sleepTime
global inputPath
global longTermStorageLocation
global allowed_file_types
global files_to_skip

files_to_skip = []

logging.basicConfig(filename = 'script.log',level=logging.DEBUG,format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
root = logging.getLogger()
root.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
root.addHandler(handler)
logging.getLogger("pika").propagate = False

# fileScan = magic.Magic(uncompress=True)

with open('send_config.yaml', 'r') as file:
    config_file = yaml.safe_load(file)
    inputPath = r"{}".format(config_file["configuration"]["source_directory"])
    sleepTime = config_file["configuration"]["error_sleep_time"]
    longTermStorageLocation =  r"{}".format(config_file["configuration"]["long_term_storage"])
    allowed_file_types = list(config_file["configuration"]["allowed_file_types"])
    rabbit_queue = config_file["configuration"]["queue"]
    rabbit_host = config_file["configuration"]["host"]

def createConnection():
    connectionSuccessful = False
    global connection
    global channel
    while connectionSuccessful == False:
        logging.info("Initiating Connection to RabbitMQ Instance")
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(rabbit_host))
            channel = connection.channel()
            # channel.queue_declare(queue=rabbit_queue)
            connectionSuccessful = True
            logging.info("Connection to RabbitMQ sucessful")
                
        except:
            logging.error("Connection to RabbitMQ unsucesfull, retying in 5 seconds")
            sleep(sleepTime)

def checkConnection():
    global connect_open
    try:
        pika.BlockingConnection(pika.ConnectionParameters(rabbit_host))
        logging.debug("Connection active")
        connect_open = True
    except:
        connect_open = False
        logging.error(f"Connection check failed, retrying in {sleepTime} seconds")
        sleep(sleepTime)
    return connect_open

def checkDirectory(inputdir, outputdir):
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
                # scanResults = fileScan.from_file(join(r, file))
                scanResults = ' '
                approvedExtension = file.endswith(tuple(allowed_file_types))
                if bool(approvedExtension) == True and 'executable' not in str(scanResults):
                    files.append(join(r, file))
                else:
                    logger.info(f"Ignoring {file} because fileype is not allowed")
                    logger.info(f'Adding {file} to skip list')
                    files_to_skip.append(file)
    return files

def checkForFiles(dir):
    files = getFiles(dir)
    if len(files) > 0:
        return files
    else:
        print(f"No new files will check in {sleepTime} seconds")
        logging.info(f"No new files will check in {sleepTime} seconds")
        return None

def getFileExtension(file):
    file_name, file_extension = splitext(file)
    return file_name, file_extension

def encodeFiles(file_name):
    try:
        with open(file_name, "rb") as file:
            encoded_string = base64.b64encode(file.read()) 
        return encoded_string
    except:
        print(f"File {file_name}, not found")

def createMesseage(file,file_name,file_extension):
    message = {
    "File_Name": file_name ,
    "Extension": file_extension,
    "Data": file
    }
    return str(message)

def moveFile(dir):
    fileStart = dir.rfind('\\')
    folderStart = dir.rfind('$') + 1
    sourceDir = dir[folderStart:fileStart] 
    fileName = dir[fileStart:]
    target = longTermStorageLocation + sourceDir
    if not os.path.exists(target):
        os.makedirs(target)
    shutil.move(dir,target)
    logging.info(f"Moved {fileName}' to long term storage")
    
def main(dir):
    while True:
        files = checkForFiles(dir)
        if files is not None:
            for f in files:
                if checkConnection() == True:
                    encodedFile = encodeFiles(f)
                    fileName, extension = getFileExtension(f)
                    subtractFromDir = len(inputPath)
                    dataDict = createMesseage(encodedFile,fileName[subtractFromDir:],extension)
                    channel.basic_publish(exchange='',
                                        routing_key=rabbit_queue,
                                        body=dataDict)
                    moveFile(f)
                    print("[X] Sent ", dataDict)
                else:
                    createConnection()
                    continue
        else:
            sleep(5)
            continue
            
if __name__ == '__main__':
    try:
        checkDirectory(inputPath,longTermStorageLocation)
        createConnection()
        main(inputPath)
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)