from send import  getFiles, checkDirectory, getFileExtension, encodeFiles, createMesseage, find2nd, moveFile
import os

"""Directories for testing"""

sourceDir = r'test_dirs\source'
archiveDir = r'test_dirs\archive'
invalidSource = r'test_dirs\sourced'
invalidArchive = r'test_dirs\archived'
dir2 = r"\\DESKTOP-ROY\rabbitmq_target\machine1\lab1\test.txt"

def test_getFiles():
    output = getFiles(sourceDir)
    print(output)
    assert output == ['test_dirs\\source\\mike.txt'], "Expected Files are not there"

def test_checkDirectory_valid():
    assert checkDirectory(sourceDir, archiveDir) == True, "Valid Directories not being accepted"

def test_checkDirectory_invalid():
    assert checkDirectory(invalidSource, invalidArchive) == False, "Invalid directories being accepted"

def test_getFileExtension():
    assert getFileExtension('helloWorld.txt') == ('helloWorld', '.txt')

def test_encodeFiles():
    output = encodeFiles(os.path.join(sourceDir,'mike.txt'))
    assert output == b'IkhlbGxvIFdvcmxkIg==', "Encoded ByteString is incorrect"

def test_create_messeage() :
    output = createMesseage(b'IkhlbGxvIFdvcmxkIg==','mike','.txt')
    assert output == "{'File_Name': 'mike', 'Extension': '.txt', 'Data': b'IkhlbGxvIFdvcmxkIg=='}", 'Encoded Messeage is incorrect'

def test_find2nd():
    output = find2nd(sourceDir)
    output2 = find2nd(dir2)
    assert output == 0, 'Relative Directory not parsed correctly'
    assert output2 == 13, 'Full Path not parsed correctly'

def test_moveFile():
    sourceFile = os.path.join(sourceDir,'rolando.txt')
    file = open(sourceFile,'w+')
    file.close()
    moveFile(sourceFile, archiveDir)
    file_exists = os.path.isfile(os.path.join(archiveDir,'rolando.txt'))
    os.remove(os.path.join(archiveDir,'rolando.txt'))
    assert file_exists == True, 'File was not moved to archive directory'






test_getFiles()
test_checkDirectory_valid()
test_checkDirectory_invalid()
test_getFileExtension()
test_encodeFiles()
test_create_messeage()
test_find2nd()
test_moveFile()