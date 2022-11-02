from sqlite3 import connect
from sre_constants import SUCCESS
import sys
import pika
import base64
import os
from os import listdir, walk, replace
from os.path import join,splitext, exists
from time import sleep
import shutil
import logging



logging.basicConfig(filename = 'script.log',level=logging.DEBUG,format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
root = logging.getLogger()
root.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
root.addHandler(handler)
logging.getLogger("pika").propagate = False

sleepTime = 5
inputPath = r"\\127.0.0.1\c$\LAB_SHARED_FOLDER"
# inputPath = r"\\RABBITMQ\Users\Administrator\Desktop\lab_shared_folder"
longTermStorageLocation = r"\\127.0.0.1\c$\long_term"


def createConnection():
    connectionSuccessful = False
    global connection
    global channel
    while connectionSuccessful == False:
        logging.info("Initiating Connection to RabbitMQ Instance")
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
            channel = connection.channel()
            channel.queue_declare(queue='hello')
            connectionSuccessful = True
            logging.info("Connection to RabbitMQ sucessful")
                
        except:
            logging.error("Connection to RabbitMQ unsucesfull, retying in 5 seconds")
            sleep(5)
            

def checkConnection():
    global connect_open
    try:
        pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        logging.debug("Connection active")
        connect_open = True
    except:
        connect_open = False
        logging.error(f"Connection check failed, retrying in {sleepTime} seconds")
        sleep(sleepTime)
    return connect_open



def getFiles(dir):
    files = []
    for r, d, f in walk(dir):
        for file in f:
            # if file.endswith(".docx"):
            files.append(join(r, file))
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
    # target = longTermStorageLocation + fileName
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
                        dataDict = createMesseage(encodedFile,fileName,extension)
                        channel.basic_publish(exchange='',
                                            routing_key='hello',
                                            body=dataDict)
                        moveFile(f)
                        print("[X] Sent ", dataDict)
                    else:
                        createConnection()
        else:
            sleep(5)
            continue
            
            

        

if __name__ == '__main__':
    try:
        createConnection()
        main(inputPath)
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)