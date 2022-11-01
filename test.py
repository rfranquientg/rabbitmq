from distutils import extension
import sys
import pika
import base64
import os
from os import listdir, walk, replace
from os.path import join,splitext, exists
from time import sleep



def getFiles(dir):
    files = []
    for r, d, f in walk(dir):
        for file in f:
            # if file.endswith(".docx"):
            files.append(join(r, file))
    return files


og = getFiles('output')
test = ['output\\LAB_SHARED_FOLDER\\Machine1\\0PLANT_TEXT.xlsx', 'output\\LAB_SHARED_FOLDER\\Machine1\\MARM.csv', 'output\\LAB_SHARED_FOLDER\\Machine1 - Copy\\0PLANT_TEXT.csv', 'output\\LAB_SHARED_FOLDER\\Machine1 - Copy\\MARM.csv', 'output\\LAB_SHARED_FOLDER\\Machine1 - Copy (2)\\0PLANT_TEXT.csv', 'output\\LAB_SHARED_FOLDER\\Machine1 - Copy (2)\\MARM.csv', 'output\\LAB_SHARED_FOLDER\\Machine1 - Copy - Copy\\0PLANT_TEXT.csv', 'output\\LAB_SHARED_FOLDER\\Machine1 - Copy - Copy\\MARM.csv', 'output\\LAB_SHARED_FOLDER\\Machine2\\notes.txt', 'output\\LAB_SHARED_FOLDER\\Machine2 - Copy\\notes.txt', 'output\\LAB_SHARED_FOLDER\\Machine2 - Copy (2)\\notes.txt', 'output\\LAB_SHARED_FOLDER\\Machine2 - Copy - Copy\\notes.txt', 'output\\LAB_SHARED_FOLDER\\Machine3\\0PROFIT_CTR_ATTR.csv', 'output\\LAB_SHARED_FOLDER\\Machine3 - Copy\\0PROFIT_CTR_ATTR.csv', 'output\\LAB_SHARED_FOLDER\\Machine3 - Copy (2)\\0PROFIT_CTR_ATTR.csv', 'output\\LAB_SHARED_FOLDER\\Machine3 - Copy - Copy\\0PROFIT_CTR_ATTR.csv', 'output\\LAB_SHARED_FOLDER\\Machine4\\helloWorld.txt', 'output\\LAB_SHARED_FOLDER\\Machine4 - Copy\\helloWorld.txt', 'output\\LAB_SHARED_FOLDER\\Machine4 - Copy (2)\\helloWorld.txt', 'output\\LAB_SHARED_FOLDER\\Machine4 - Copy - Copy\\helloWorld.txt']


s = set(og)
temp3 = [x for x in test if x not in s]
print(temp3)