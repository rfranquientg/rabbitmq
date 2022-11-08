import pika, sys, os
import base64
import ast
from  os.path import join
import logging
import yaml
from time import sleep
#This is the path to output folder

logging.basicConfig(filename = 'recieve.log',level=logging.DEBUG,format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
root = logging.getLogger()
root.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
root.addHandler(handler)
logging.getLogger("pika").propagate = False

global credentials 

with open('recieve_config.yaml', 'r') as file:
    config_file = yaml.safe_load(file)
    target_directory = r"{}".format(config_file["configuration"]["target_directory"])
    sleepTime = config_file["configuration"]["error_sleep_time"]
    rabbit_queue = config_file["configuration"]["queue"]
    rabbit_host = config_file["configuration"]["host"]
    
def createConnection():
    connectionSuccessful = False
    global connection
    global channel
    while connectionSuccessful == False:
        logging.info("Initiating Connection to RabbitMQ Instance")
        try:
            credentials = pika.PlainCredentials('rolando', 'password')
            parameters = pika.ConnectionParameters(rabbit_host,
                                   5672,
                                   '/',
                                   credentials)
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            channel.queue_declare(queue=rabbit_queue)
            connectionSuccessful = True
            logging.info("Connection to RabbitMQ sucessful")
        except:
            logging.error("Connection to RabbitMQ unsucesfull, retying in 5 seconds")
            sleep(sleepTime)

def checkConnection():
    global connect_open
    try:
        credentials = pika.PlainCredentials('rolando', 'password')
        parameters = pika.ConnectionParameters(rabbit_host,
                                            5672,
                                            '/',
                                            credentials)
        pika.BlockingConnection(parameters)
        logging.debug("Connection active")
        connect_open = True
    except:
        connect_open = False
        logging.error(f"Connection check failed, retrying in {sleepTime} seconds")
        sleep(sleepTime)
    return connect_open

def decodeFile(mssg):
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

def main():
    def callback(ch, method, properties, body):
            # print(" [x] Received %r" % body)
            if checkConnection() == True:
                decodeFile(body)
            else:
                while checkConnection() == False:
                    createConnection()
    while True:
        credentials = pika.PlainCredentials('rolando', 'password')
        parameters = pika.ConnectionParameters(rabbit_host,
                                   5672,
                                   '/',
                                   credentials)

        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        channel.queue_declare(queue=rabbit_queue)
        channel.basic_consume(queue=rabbit_queue, on_message_callback=callback, auto_ack=True)
        try:
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
        connection.close()
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)