import pika, sys, os
import base64
import ast
from  os.path import join

#This is the path to output folder

outputPath= 'output'

def decodeFile(mssg, networkShare = True):
    print("Decoded String here")
    temp = str(mssg, "utf-8")
    messageDict =  ast.literal_eval(temp)
    if networkShare == True:
        newStart = messageDict['File_Name'].index('$') + 2
    else:
        newStart = 0
    indexOfLastFolder = messageDict['File_Name'].rfind("\\")
    dirTest = messageDict['File_Name'][newStart:indexOfLastFolder]
    filename = messageDict['File_Name'][indexOfLastFolder + 1:] + messageDict['Extension']
    outputDir = join(outputPath,dirTest)
    endPath =join(outputDir, filename)
    print(endPath)
    if not os.path.exists(outputDir):
        os.makedirs(outputDir)
    with open(endPath, "wb") as f:
        f.write(base64.b64decode(messageDict["Data"]))

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='hello')
    
    def callback(ch, method, properties, body):
        # print(" [x] Received %r" % body)
        decodeFile(body)
    
    channel.basic_consume(queue='hello', on_message_callback=callback, auto_ack=True)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:            
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)