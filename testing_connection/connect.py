#!/usr/bin/env python
import pika
import time

credentials = pika.PlainCredentials('rolando', 'password')
parameters = pika.ConnectionParameters('10.200.5.66',
                                   5672,
                                   '/',
                                   credentials)

connection = pika.BlockingConnection(parameters)
channel = connection.channel()

channel.queue_declare(queue='pikaFileTransfer2')
time.sleep(3)
print("Connection Success")
channel.basic_publish(exchange='',
                  routing_key='pikaFileTransfer2',
                  body='Hello W0rld!')
print(" [x] Sent 'Hello World!'")
k=input("press close to exit2") 
print("Send Success")
time.sleep(3)
connection.close()