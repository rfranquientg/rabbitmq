import pika, sys, os
import base64
import ast
from  os.path import join
import logging
import yaml
from time import sleep

"""Global Variable Declaration"""
global credentials
global sleepTime
global inputPath
global parameters
global connection

"""Reading Configuration file"""
with open('recieve_config.yaml', 'r') as file:
    config_file = yaml.safe_load(file)
    sleepTime = config_file["configuration"]["error_sleep_time"]
    target_directory = config_file["configuration"]["target_directory"]
    rabbit_queue = str(config_file["configuration"]["queue"])
    rabbit_host = str(config_file["configuration"]["host"])
    port = config_file["configuration"]["port"]
    devMode = config_file["configuration"]["devMode"]
    username = config_file["credentials"]["username"]
    password = config_file["credentials"]["password"]


"""Initializing Logging"""
logging.basicConfig(filename = 'recieve.log',level=logging.DEBUG,format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
root = logging.getLogger()
root.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
root.addHandler(handler)
logging.getLogger("pika").propagate = False

"""Initializing Variables"""
credentials = pika.PlainCredentials(username, password)
parameters = pika.ConnectionParameters(rabbit_host,
                                            port,
                                            '/',
                                            credentials)
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
    global connect_open
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

def decodeFile(mssg):
    """Decode the dict messeage recieved and parses the information to a file."""
    decodedMesseageStr = str(mssg, "utf-8")
    messageDict =  ast.literal_eval(decodedMesseageStr)
    indexOfLastFolder = messageDict['File_Name'].rfind("\\")
    dirTest = messageDict['File_Name'][:indexOfLastFolder]
    filename = messageDict['File_Name'][indexOfLastFolder + 1:] + messageDict['Extension']
    logging.info(f"Recieved {filename}")
    outputDir = join(target_directory,dirTest)
    endPath =join(outputDir, filename)
    if not os.path.exists(outputDir):
        os.makedirs(outputDir)
    with open(endPath, "wb") as f:
        f.write(base64.b64decode(messageDict["Data"]))
    logging.info(f"File {filename} stored successfully")
    logging.info(' [*] Waiting for messages.')

def main():
    """Main Function"""
    def callback(ch, method, properties, body):
     # """Function that is called everytime  a new messeage is recieved"""
            # print(" [x] Received %r" % body)
            if checkConnection() == True:
                decodeFile(body)
            else:
                while checkConnection() == False:
                    createConnection()
    while True:
        if devMode == False:
            connection = pika.BlockingConnection(parameters)
        else:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        try:
            channel = connection.channel()
            channel.queue_declare(queue=rabbit_queue)
            channel.basic_consume(queue=rabbit_queue, on_message_callback=callback, auto_ack=True)
            createConnection()
            logging.info(' [*] Waiting for messages. To exit press CTRL+C')
            channel.start_consuming()
        except pika.exceptions.ConnectionClosedByBroker:
            while checkConnection() == False:
                channel.stop_consuming()
                createConnection()
                sleep(sleepTime)
        except:
            logging.error('Connection Failure')
            connection.close()
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