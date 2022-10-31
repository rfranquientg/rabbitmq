from email import message
import sys, os
import pika
import base64
import json


connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.queue_declare(queue='hello')


def getFileExtension(file):
    file_name, file_extension = os.path.splitext(file)
    
    return file_name, file_extension


def encodeFiles(file_name):
    with open(file_name, "rb") as pdf_file:
        encoded_string = base64.b64encode(pdf_file.read()) 
    return encoded_string

def createMesseage(file):
    file_name, file_extension = getFileExtension(file)
    message = {
    "File_Name": file_name ,
    "Extension": file_extension,
    "Data": encodeFiles(file) #.decode("utf-8")
    }
    return str(message)
    
def main():
    while True:
        sendOrNo = input("Do you want to send the File?")
        if 'y' in sendOrNo:
            mssg = createMesseage('MARM.csv')
            channel.basic_publish(exchange='',
                                routing_key='hello',
                                body=mssg)
            print(f" [x] Sent {mssg}'")
        else:
            continue



if __name__ == '__main__':

    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)