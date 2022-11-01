import pika, sys, os
import base64
import ast
from  os.path import join



def decodeFile(mssg):
    print("Decoded String here")
    temp = str(mssg, "utf-8")
    messageDict =  ast.literal_eval(temp)
    indexOfLastFolder = messageDict['File_Name'].rfind("\\")
    dirTest = messageDict['File_Name'][:indexOfLastFolder]
    outputDir = join(".\output",dirTest)
    print(outputDir)
    if not os.path.exists(outputDir):
        os.makedirs(outputDir)
    with open(f"./output/{messageDict['File_Name']}{messageDict['Extension']}", "wb") as f:
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