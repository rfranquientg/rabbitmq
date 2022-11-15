import sys, os
import pika
import base64


connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.queue_declare(queue='hello')


def encodePDF(pdf_name):
    with open(pdf_name, "rb") as pdf_file:
        encoded_string = base64.b64encode(pdf_file.read()) 
    return encoded_string


def main():
    while True:
        sendOrNo = input("Do you want to send the PDF?")
        if 'y' in sendOrNo:
            mssg = encodePDF('big_pdf.pdf')
            channel.basic_publish(exchange='',
                                routing_key='hello',
                                body=mssg)
            print(f" [x] Sent {mssg}'")
        else:
            continue



if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
