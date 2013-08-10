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

        if ':' in host:
            host, port = host.split(':', 1)
            port = int(port)
        else:
            port = None

        logger.info('Connecting to host: %s, port: %s, queue: %s' %
                (host, port, queue))

        self.connection_parameters = pika.ConnectionParameters(
                host=host, port=port)
        self.durable = durable
        self._connect()

    def _connect(self):
        self.connection = pika.BlockingConnection(self.connection_parameters)
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue, durable=self.durable)

    def consume(self, callback):
        def _callback(ch, method, properties, body):
            # とりあえずメッセージボディはjsonにする
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

        if not self.connection.is_open:
            self._connect()

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
