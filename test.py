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
import magic 


dir = r"\\DESKTOP-ROY\rabbitmq_source"


print("Directory Exists:",os.path.exists(dir))



for root, dirs, files in os.walk(dir):
    print("Root",root)
    print("Dirs", dirs)
    print("Files", files )