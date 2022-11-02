# from distutils import extension
# import sys
# import pika
# import base64
# import os
# from os import listdir, walk, replace
# from os.path import join,splitext, exists
# from time import sleep
import ast
import json
from time import sleep

### Testing the read of a config file


f = open('config.json')
  

data = json.load(f)
  
print(data['person'])
# Closing file
sleep(10)
f.close()
