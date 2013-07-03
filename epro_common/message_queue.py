# coding: utf-8

import sys
import traceback
import logging

import pika
import bson

logger = logging.getLogger(__name__)

class MessageQueue(object):
    """
    pikaを抽象化したメッセージキュー
    """

    def __init__(self, host, queue, durable=True):
        self.queue = queue

        logger.info('Connecting to host: %s, queue: %s' % (host, queue))
        connection = pika.BlockingConnection(pika.ConnectionParameters(
                host=host))
        self.channel = connection.channel()

        self.channel.queue_declare(queue=self.queue, durable=durable)

    def consume(self, callback):
        def _callback(ch, method, properties, body):
            # とりあえずメッセージボディはbsonにする
            obj = bson.loads(body)

            # callbackでAttributeErrorが発生したときに
            # 終了しないため、特別にハンドルする
            try:
                callback(obj)
            except AttributeError as ex:
                traceback.print_exc(file=sys.stderr)
                raise Exception("AttributeError occured in callback")

            # callbackが正常に終了したらAckを返す
            ch.basic_ack(delivery_tag=method.delivery_tag)

        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(_callback, queue=self.queue)
        self.channel.start_consuming()

    def publish(self, obj):
        body = bson.dumps(obj)

        self.channel.basic_publish(exchange='',
                routing_key=self.queue,
                body=body)
