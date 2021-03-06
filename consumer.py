from copy import copy
import json
import logging
import os
import random

from confluent_kafka import Consumer, KafkaException, KafkaError


KAFKA_SETTINGS = {
    'bootstrap.servers': os.environ.get('KAFKA_BROKERS_URL'),
    'session.timeout.ms': 6000,
    'default.topic.config': {'auto.offset.reset': 'smallest'},
}


logger = logging.getLogger(__name__)


class NoHandlerFoundException(Exception):
    pass


class BaseConsumer(object):
    CONSUMER_GROUP_ID = NotImplemented
    CONSUMER_CLIENT_ID = os.environ.get('DYNO', 'local')
    TOPICS = NotImplemented
    EVENT_TYPE_HANDLER_MAPPING = {}
    DEFAULT_EVENT_HANDLER = NotImplemented

    @property
    def consumer(self):
        if not hasattr(self, '_consumer'):
            self._consumer = self._get_consumer()
        return self._consumer

    @property
    def group_id(self):
        if not hasattr(self, '_group_id'):
            self._group_id = self._get_group_id()
        return self._group_id

    def poll(self):
        msg = self.consumer.poll(timeout=1.0)
        if msg is None:
            return
        elif msg.error():
            # Error or event
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event
                logger.info('{} [{}] reached end at offset {}'.format(
                    msg.topic(), msg.partition(), msg.offset()))
            else:
                # Error
                raise KafkaException(msg.error())
        else:
            # Proper message
            self.process_message(msg)

    def process_message(self, msg):
        logger.debug("Processing message value: {}".format(msg.value()))
        try:
            handler = self.get_handler(msg)
        except NoHandlerFoundException:
            pass  # Just ignore it
        else:
            handler.process_message(msg)

    def get_handler(self, msg):
        payload = json.loads(msg.value())
        event_id = payload.get('event_id')
        if event_id:
            logger.info("Consuming event with id '{}'".format(event_id))
        # In your producer, make sure to produce messages with an 'event_type' key
        event_type = payload.get('event_type')
        if event_type:
            event_handler = self.EVENT_TYPE_HANDLER_MAPPING.get(event_type)
            if not event_handler:
                logger.debug("No handler found for message of type '{}'".format(event_type))
                raise NoHandlerFoundException()
        else:
            event_handler = self.DEFAULT_EVENT_HANDLER
        return event_handler

    def cleanup(self):
        self.consumer.close()  # tis important to commit offsets

    def _get_consumer(self):
        kafka_conf = copy(KAFKA_SETTINGS)
        kafka_conf.update({
            'group.id': self.group_id,
            'client.id': self.CONSUMER_CLIENT_ID,
        })
        consumer = Consumer(**kafka_conf)
        consumer.subscribe(self.TOPICS)
        logger.info("Initializing consumer in group '{}' listening on topics {}".format(
            self.group_id, self.TOPICS))
        return consumer

    def _get_group_id(self):
        consumer_id_suffix = os.environ.get('KAFKA_CONSUMER_GROUP_ID_SUFFIX', self._get_random_id())
        return self.CONSUMER_GROUP_ID + '.' + consumer_id_suffix

    def _get_random_id(self):
        return '%10x' % random.randrange(16 ** 10)
