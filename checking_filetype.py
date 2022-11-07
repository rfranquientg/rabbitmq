import filetype
from os.path import exists

file_exists = exists('send_old.exe')
print(file_exists)

def main():
    kind = filetype.guess('send_old.exe')
    print(kind)
    if kind is None:
        print('Cannot guess file type!')
        return

    print('File extension: %s' % kind.extension)
    print('File MIME type: %s' % kind.mime)

if __name__ == '__main__':
    main()