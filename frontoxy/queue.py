# -*- coding: utf-8 -*-

import pika
import pika.exceptions as exceptions
import json


class SimpleQueue(object):

    def __init__(self, queue_url, queue_name):
        self._queue_url = queue_url
        self._queue_name = queue_name


    def open(self):
        # Init
        self._connection = pika.BlockingConnection(pika.URLParameters(self._queue_url))
        self._channel = self._connection.channel()

        # Work queue
        self._channel.queue_declare(
            queue=self._queue_name,
            durable=True,
            arguments={
                'x-max-priority': 100,
            })
        self._channel.basic_qos(prefetch_count=1)


    def close(self):
        self._channel.close()
        self._connection.close()


    def publish(self, item, priority=0, retry=2):
        body = json.dumps(item)

        try:
            self._channel.basic_publish(exchange=u'',
                                        routing_key=self._queue_name,
                                        body=body,
                                        properties=pika.BasicProperties(
                                            delivery_mode=2,
                                            priority=priority
                                        ))

        except exceptions.ConnectionClosed as err:
            if retry <= 0:
                raise err

            self.open()
            self.publish(item, retry - 1)


    def get(self, retry=2):
        try:
            method_frame, properties, body = self._channel.basic_get(self._queue_name, no_ack=True)
            if not body:
                return

            return json.loads(body)

        except exceptions.ConnectionClosed as err:
            if retry <= 0:
                raise err

            self.open()
            return self.get(retry - 1)


    def consume(self, callback):
        self._channel.basic_consume(callback,
                                    queue=self._queue_name,
                                    no_ack=True)

        self._channel.start_consuming()



class RetryQueue(object):

    def __init__(self, queue_url, queue_name, retry_delay):
        self._queue_url = queue_url
        self._queue_name = queue_name
        self._retry_delay = retry_delay

        self._exchange_retry_name = u'{0}_retry_exchange'.format(self._queue_name)
        self._queue_retry_name = u'{0}_retry'.format(self._queue_name)


    def open(self):
        # Init
        self._connection = pika.BlockingConnection(pika.URLParameters(self._queue_url))
        self._channel = self._connection.channel()

        # Work queue
        self._channel.queue_declare(
            queue=self._queue_name,
            durable=True,
            arguments={
                'x-max-priority': 100,
            })
        self._channel.basic_qos(prefetch_count=1)

        # Retry queue
        self._channel.exchange_declare(
            exchange=self._exchange_retry_name,
            type=u'direct'
        )

        self._channel.queue_declare(
            queue=self._queue_retry_name,
            durable=True,
            arguments={
                'x-message-ttl': self._retry_delay,
                'x-dead-letter-exchange': '',
                'x-dead-letter-routing-key': self._queue_name,
            })

        self._channel.queue_bind(
            exchange=self._exchange_retry_name,
            queue=self._queue_retry_name
        )


    def close(self):
        self._channel.close()
        self._connection.close()


    def publish(self, item, priority, retry=2):
        body = json.dumps(item)

        self._publish(u'', self._queue_name, body, priority, retry)


    def retry(self, item, priority, retry=2):
        body = json.dumps(item)

        self._publish(self._exchange_retry_name, self._queue_retry_name, body, priority, retry)


    def _publish(self, exchange_name, queue_name, body, priority, retry):
        try:
            self._channel.basic_publish(exchange=exchange_name,
                                        routing_key=queue_name,
                                        body=body,
                                        properties=pika.BasicProperties(
                                            delivery_mode=2,
                                            priority=priority
                                        ))

        except exceptions.ConnectionClosed as err:
            if retry <= 0:
                raise err

            self.open()
            self._publish(exchange_name, queue_name, body, priority, retry - 1)


    def get(self, retry = 2):
        try:
            method_frame, properties, body = self._channel.basic_get(self._queue_name, no_ack=True)
            if not body:
                return

            return json.loads(body)

        except exceptions.ConnectionClosed as err:
            if retry <= 0:
                raise err

            self.open()
            return self.get(retry - 1)


    def consume(self, callback):
        self._channel.basic_consume(callback,
                                    queue=self._queue_name,
                                    no_ack=True)

        self._channel.start_consuming()
