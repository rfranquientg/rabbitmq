
import pika
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.queue_declare(queue='hello')

while True:
    sendOrNo = input("Do you want to send the csv?")
    with open('input.csv') as f:
        s = f.read() + '\n' # add trailing new line character
    # mssg = 'name,age\nDan,33\nBob,19\nSheri,42'
    mssg = repr(s)
    channel.basic_publish(exchange='',
                        routing_key='hello',
                        body=mssg)
    print(f" [x] Sent {mssg}'")

connection.close()

