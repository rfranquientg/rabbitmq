from distutils import extension
import sys
import pika
import base64
import os
from os import listdir, walk, replace
from os.path import join,splitext, exists
from time import sleep
import ast
import json
from time import sleep
import logging
### Testing the read of a config file
sleepTime = 1

import pika 
import time

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='hello')

def checkConnection():
    sleep(sleepTime)
    global connect_open
    try:
        pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        logging.debug("Connection active")
        connect_open = True
    except:
        connect_open = False
        logging.error(f"Connection retrying in {sleepTime} seconds")
        sleep(sleepTime)
    return connect_open

while True:
    print(checkConnection())