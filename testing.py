# from gettext import find
# from os.path import join


# def find2nd(dir):
#     if dir.startswith('\\'):
#         if dir.count('\\') > 2:
#             first = dir[2:].find('\\')
#             second = dir.find('\\',first + 1) 
#         else:
#             second = dir[2:]
#     else:
#         first = dir.find('\\')
#         second = dir.find('\\',first + 1) 
#     return second


# dir = r"C:\test_dir\source\machine1\lab1\test.txt"

dir2 = r"\\DESKTOP-ROY\rabbitmq_target\machine1\lab1\test.txt"

# longTermStorageLocation = r"C:\test_dir\archive"


# def functionTester(dir):
#     folderStart = find2nd(dir)
#     fileStart = dir.rfind("\\")
#     sourceDir = dir[folderStart:fileStart] 
#     fileName = dir[fileStart:]
#     target = longTermStorageLocation + sourceDir
#     print(target)

# functionTester(dir2)

# import hashlib

# h = hashlib.sha3_224()
# h.update(b"\x00" * 1)
# h.update(b"\x00" * 4294967295)
# d = h.digest()
# d.hex()




def find2nd(dir: str) -> int:
    """ This funciton is used to remove the (source drive or computer) from the path and tell the move file function what path to use for the Archive Directory
    For example: \\server\machine1\file.txt to \machine1\file.txt, to do this it returns an integer referencing the posistion of the second backslash
    """
    if dir.startswith('\\'):
        if dir.count('\\') > 2:
            first = dir[2:].find('\\')
            second = dir.find('\\',first + 1) 
        else:
            second = dir[2:]
    else:
        first = dir.find('\\')
        second = dir.find('\\',first + 1) 
        if second == -1:
            return 0
    return second

sourceDirect = r'test_dirs\source'


folderStart = find2nd(dir2)
print(folderStart)
# fileStart = dir2.rfind("\\")
# sourceDir = dir2[folderStart:fileStart] 
# fileName = sourceDir[fileStart:]
# print(sourceDir)