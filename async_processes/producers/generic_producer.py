import pika
from json import dumps
from config import Shoppin

def create_rmq_connection():
    # create rabbit mq connection
    params = pika.URLParameters(Shoppin.Queue.rabbit_mq_url)
    params.socket_timeout = 1300
    rmq_connection = pika.BlockingConnection(params)
    rmq_channel = rmq_connection.channel()
    return rmq_connection, rmq_channel

def publish_event(rabbit_mq_url, queue_name, data, rmq_channel_param=None):
    """Publish event to the rabbitmq queue."""
    try:
        if rmq_channel_param:
            try:
                channel = rmq_channel_param
                channel.queue_declare(queue=queue_name, durable = True)
                channel.basic_publish(exchange='', routing_key=queue_name, body=dumps(data, default=str),
                    properties=pika.BasicProperties(delivery_mode=2))
                return {'status': 1, 'message': 'successfully pushed to queue --> {}'.format(queue_name)}
            except pika.exceptions.ConnectionClosed as e:
                print("Connection closed:- ",e)
            except Exception as e:
                print(e)

        params = pika.URLParameters(rabbit_mq_url)
        params.socket_timeout = 1300
        connection = pika.BlockingConnection(params)
        channel = connection.channel()

        channel.queue_declare(queue=queue_name, durable=True)
        channel.basic_publish(exchange='', routing_key=queue_name, body=dumps(data, default=str),
            properties=pika.BasicProperties(delivery_mode=2))
        
        connection.close()
        
        return {'status': 1, 'message': 'successfully pushed to queue --> {}'.format(queue_name)}
    except Exception as e:
        print(e)
        return {'status': 0, 'message': 'unsuccessfull, so can not push to queue --> '.format(queue_name)}