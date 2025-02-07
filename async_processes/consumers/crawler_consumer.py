# external imports
import os
import sys
import json
import pika
import traceback
import time

sys.path.insert(0, os.getcwd())

# internal imports
from config import Shoppin
from components.crawler.crawler_functions import CrawlerBuilder
import db

def callback(ch, method, properties, body):
    try:
        start_t = time.time()

        print('Received: {}'.format(body))
        message = body.decode('utf-8')
        message = json.loads(message)
        ch.basic_ack(delivery_tag=method.delivery_tag)

        domain              = message.get('domain_name')
        is_lazy_loading     = message.get('is_lazy_loading')
        domain_id           = message.get('domain_id')
        process_id          = message.get('process_id')

        conn = db.db_connection()
        cursor = conn.cursor()

        # call crawl class builder to start process
        crawler_builder = CrawlerBuilder(conn, cursor, domain = domain, is_lazy_loading = is_lazy_loading, 
                                         domain_id = domain_id, process_id = process_id)
        crawler_builder.start_crawling()
        crawler_builder.save_crawling_results()
        print("Saved")

        cursor.close()
        conn.close()

        print("Total time: ", time.time() - start_t)
        print("Products crawled...")

    except Exception as e:
        print(traceback.format_exc())


while True:
    try:
        params = pika.URLParameters(Shoppin.Queue.rabbit_mq_url)
        params.socket_timeout = 300
        connection = pika.BlockingConnection(params)
        channel = connection.channel()
        channel.queue_declare(queue=Shoppin.Queue.crawl_domain_queue, durable=True)
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=Shoppin.Queue.crawl_domain_queue, on_message_callback=callback)
        print('Waiting...')
        channel.start_consuming()
    except KeyboardInterrupt:
        print('CTRL+C pressed. Closing consumer.')
        break
    except Exception as e:
        print(e)
        print(traceback.print_exc())
        print("Connection Broken! Restarting...")
