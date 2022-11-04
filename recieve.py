import pika, sys, os
import base64
import ast
from  os.path import join
import logging
import yaml
from time import sleep
#This is the path to output folder



logging.basicConfig(filename = 'script.log',level=logging.DEBUG,format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
root = logging.getLogger()
root.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
root.addHandler(handler)
logging.getLogger("pika").propagate = False

with open('recieve_config.yaml', 'r') as file:
    config_file = yaml.safe_load(file)
    target_directory = r"{}".format(config_file["configuration"]["target_directory"])
    sleepTime = config_file["configuration"]["error_sleep_time"]
    
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
            sleep(sleepTime)

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

def decodeFile(mssg, networkShare = False):
    print("Decoded String here")
    temp = str(mssg, "utf-8")
    messageDict =  ast.literal_eval(temp)
    print(messageDict['File_Name'])
    if networkShare == True:
        newStart = messageDict['File_Name'].index('$') + 2
    else:
        newStart = 0
    indexOfLastFolder = messageDict['File_Name'].rfind("\\")
    dirTest = messageDict['File_Name'][newStart:indexOfLastFolder]
    filename = messageDict['File_Name'][indexOfLastFolder + 1:] + messageDict['Extension']
    outputDir = join(target_directory,dirTest)
    endPath =join(outputDir, filename)
    print(endPath)
    if not os.path.exists(outputDir):
        os.makedirs(outputDir)
    with open(endPath, "wb") as f:
        f.write(base64.b64decode(messageDict["Data"]))

### If you want to eliminate the source directory structure all you need to do is send only upto that part or send the directory to subract on this side. 
def decodeFile2(mssg):
    decodedMesseageStr = str(mssg, "utf-8")
    messageDict =  ast.literal_eval(decodedMesseageStr)


def main():
    def callback(ch, method, properties, body):
            # print(" [x] Received %r" % body)
            if checkConnection() == True:
                decodeFile(body)
            else:
                while checkConnection() == False:
                    createConnection()
    while True:
        createConnection()
        channel.queue_declare(queue='hello')
        channel.basic_consume(queue='hello', on_message_callback=callback, auto_ack=True)
        print(' [*] Waiting for messages. To exit press CTRL+C')
        try:
            channel.start_consuming()
            logging.info(' [*] Waiting for messages. To exit press CTRL+C')
        except pika.exceptions.ConnectionClosedByBroker:
            while checkConnection() == False:

                createConnection()
                sleep(sleepTime)
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