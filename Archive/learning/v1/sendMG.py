
import pika
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.queue_declare(queue='hello')

while True:
    mssg = input("What do you want to send to the other program ?")
    channel.basic_publish(exchange='',
                        routing_key='hello',
                        body=mssg)
    print(f" [x] Sent {mssg}'")

connection.close()

