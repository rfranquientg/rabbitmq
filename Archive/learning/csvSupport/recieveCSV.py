import pika, sys, os
import pandas as pd
from io import StringIO
import csv




def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='hello')
    

   
    def wirte_to_csv(csvString):
        temp = csvString.decode()
        if isinstance(temp,str):
            csvStringIO = StringIO(temp)
            df = pd.read_csv(csvStringIO, sep=",", header=None)
            df.to_csv("Output.csv")
        else:
            print("Not String", csvString)
            pass

    global csvText

    def callback(ch, method, properties, body):
        print(" [x] Received %r" % body)
        csvText=body
        wirte_to_csv(csvText)
    
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