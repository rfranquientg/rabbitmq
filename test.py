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





print("Directory Exists:",os.path.exists(r"\\127.0.0.1\\c$\LAB_SHARED_FOLDER"))