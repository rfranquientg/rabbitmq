import pika, sys, os
import base64



def decodePDF(mssg):
        with open("output.pdf", "wb") as f:
            f.write(base64.b64decode(mssg))

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='hello')
    
    def callback(ch, method, properties, body):
        print(" [x] Received %r" % body)
        pdfCode=body
        decodePDF(pdfCode)
    
    channel.basic_consume(queue='hello', on_message_callback=callback, auto_ack=True)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)