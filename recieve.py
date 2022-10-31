from email import message
import pika, sys, os
import base64
import json
import ast



def decodeFile(mssg):
    print("Decoded String here")
    temp = str(mssg, "utf-8")
    print(temp)
    messageDict =  ast.literal_eval(temp)
    print(messageDict['File_Name'])
    print()
    with open(f"./output/{messageDict['File_Name']}{messageDict['Extension']}", "wb") as f:
        f.write(base64.b64decode(messageDict["Data"]))

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='hello')
    
    def callback(ch, method, properties, body):
        print(" [x] Received %r" % body)
        decodeFile(body)
        # decodeFile(str(base64.b64decode(body)))
    
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