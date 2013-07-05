# coding: utf-8

import sys
import traceback
import logging
import json
from datetime import datetime

import pika

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
            obj = decode(body)

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
        body = encode(obj)

        self.channel.basic_publish(exchange='',
                routing_key=self.queue,
                body=body)

def encode(obj):
    """
    >>> decode(encode({u'a': 1, 'b': u'hoge', 'time': datetime(2013, 4, 3)}))
    {u'a': 1, u'b': u'hoge', u'time': datetime.datetime(2013, 4, 3, 0, 0)}
    """
    return json.dumps(obj, cls=JSONEncoder)

def decode(body):
    return json.loads(body, object_hook=object_hook)


class JSONEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, datetime):
            return {
                '__class__': 'datetime',
                'year': obj.year,
                'month': obj.month,
                'day': obj.day,
                'hour': obj.hour,
                'minute': obj.minute,
                'second': obj.second,
                'microsecond': obj.microsecond,
            }

        return super(self, JSONEncoder).default(obj)

def object_hook(dct):
    if '__class__' in dct:
        if dct['__class__'] == 'datetime':
            return datetime(
                    year=dct['year'],
                    month=dct['month'],
                    day=dct['day'],
                    hour=dct['hour'],
                    minute=dct['minute'],
                    second=dct['second'],
                    microsecond=dct['microsecond'],
                    )
    return dct
