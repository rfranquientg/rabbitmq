from distutils import extension
import sys
from numpy import source
import pika
import base64
import os
from os import listdir, walk, replace
from os.path import join,splitext, exists
from time import sleep
import shutil

inputPath = r"\\127.0.0.1\c$\LAB_SHARED_FOLDER"
longTermStorageLocation = r"\\127.0.0.1\c$\long_term"
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.queue_declare(queue='hello')

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
    pass


def main(dir):
    while True:
        # sendOrNo = input("Do you want to send the File?")
        answer = input('Send Files???')
        files = checkForFiles(dir)
        if files is not None:
            for f in files:
                encodedFile = encodeFiles(f)
                fileName, extension = getFileExtension(f)
                dataDict = createMesseage(encodedFile,fileName,extension)
                moveFile(f)
                # channel.basic_publish(exchange='',
                #                     routing_key='hello',
                #                     body=dataDict)
                # print("[X] Sent ", dataDict)
                
        else:
            continue
    

if __name__ == '__main__':
    try:
        main(inputPath)
        # main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)