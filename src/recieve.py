import pika, sys, os
import base64
import ast
from  os.path import join
import logging
import yaml
from time import sleep
from logging import handlers
import traceback
from typing import ByteString, List, Dict, Any

"""Global Variable Declaration"""
global credentials
global sleepTime
global inputPath
global parameters



"""Reading Configuration file"""
with open('recieve_config.yaml', 'r') as file:
    config_file = yaml.safe_load(file)
    sleepTime: float = config_file["configuration"]["error_sleep_time"]
    target_directory: str = config_file["configuration"]["target_directory"]
    rabbit_queue: str = str(config_file["configuration"]["queue"])
    rabbit_host: str = str(config_file["configuration"]["host"])
    port: int = config_file["configuration"]["port"]
    log_file_name: str = join("logfiles", str(config_file["configuration"]["log_file_name"]))
    devMode: bool = config_file["configuration"]["devMode"]
    username: str = config_file["credentials"]["username"]
    password: str = config_file["credentials"]["password"]

"""Initializing Variables"""
folder_for_logs: str = 'logfiles'
credentials: pika = pika.PlainCredentials(username, password)
parameters: pika = pika.ConnectionParameters(rabbit_host,
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
    global connectionSuccessful
    global channel
    global connection
    """Creates initial connection to RabbitMQ Instance"""
    connectionSuccessful = False
    while connectionSuccessful == False:
        logging.info("Initiating Connection to RabbitMQ Instance")
        try:
            if devMode == False:
                connection = pika.BlockingConnection(parameters)
            else:
                logging.info("Attempting Devmode Connection to RabbitMQ")
                connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
            channel = connection.channel()
            channel.queue_declare(queue=rabbit_queue)
            connectionSuccessful = True
            logging.info("Connection to RabbitMQ sucessful")
        except:
            logging.error("Connection to RabbitMQ unsucesfull, retying in 5 seconds")
            sleep(sleepTime)

def checkConnection() -> bool:
    """Checks if the connection is open, and if the connection is open returns TRUE, else FALSE"""
    global connect_open
    try:
        if devMode == False:
            pika.BlockingConnection(parameters)
        else:
            pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        logging.debug("Connection active")
        connect_open = True
    except:
        connect_open = False
        logging.error(f"Connection check failed, retrying in {sleepTime} seconds")
        sleep(sleepTime)
    return connect_open

def decodeFile(mssg: ByteString):
    """Decode the dict messeage recieved and parses the information to a file."""
    decodedMesseageStr = str(mssg, "utf-8")
    messageDict =  ast.literal_eval(decodedMesseageStr)
    indexOfLastFolder = messageDict['File_Name'].rfind("\\")
    dirFromSource = messageDict['File_Name'][:indexOfLastFolder]
    filename = messageDict['File_Name'][indexOfLastFolder + 1:] + messageDict['Extension']
    logging.info(f"Recieved {filename}")
    outputDir = target_directory + dirFromSource
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
        try:
            createConnection()
            channel.basic_consume(queue=rabbit_queue, on_message_callback=callback, auto_ack=True)
            logging.info(' [*] Waiting for messages. To exit press CTRL+C')
            channel.start_consuming()
        except pika.exceptions.ConnectionClosedByBroker:
            logging.error('Connection closed by Broker')
            while checkConnection() == False:
                logging.error('Attempting to restore connection')
                createConnection()
                sleep(sleepTime)
        except:
            logging.error('Unknown Connection Failure, network might be down..')
            logging.error(traceback.format_exc())
            createConnection()
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