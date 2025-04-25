
import boto3
import backoff
import logging
import random
import json
from random import choice
from confluent_kafka import Producer
from confluent_kafka.error import ProduceError
import json
import atexit
from datetime import datetime
from decimal import Decimal
from confluent_kafka.serialization import StringSerializer


def json_serializer(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError

class KafkaLoader:

    def __init__(self):
        config = {
            # User-specific properties that you must set
            'bootstrap.servers': '',
            'sasl.username': '',
            'sasl.password': '',

            'retry.backoff.ms': 100,
            'request.timeout.ms': 10000,
            'linger.ms': 100,  # 0ms: send msg immediately, 100ms: send msg if buffer is full or 100ms passed
            
            # Fixed properties
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms':   'PLAIN',
            'acks': 1 # check only leader partition
        }

        # Create Producer instance
        self.producer = Producer(config)
        atexit.register(self.close)
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
            
    def close(self) -> None:
        """Close the producer connection."""
        self.producer.flush()
        self.logger.info("Producer closed")
        
    def delivery_report(self, err, msg):
        """
        Reports the success or failure of a message delivery.

        Args:
            err (KafkaError): The error that occurred on None on success.
            msg (Message): The message that was produced or failed.
        """

        if err is not None:
            self.logger.error("Delivery failed for User record {}: {}".format(msg.key(), err))
            return
        # self.logger.info('User record {} successfully produced to {} [{}] at offset {}'.format(
        #     msg.key(), msg.topic(), msg.partition(), msg.offset()))

    def upload(self, chain: str, data_type: str, data: dict):
        topic = f"{chain}_{data_type}"
        json_str = json.dumps(data, default=json_serializer)
        self.producer.produce(topic=topic, key=None, value=json_str.encode("utf-8"), on_delivery=self.delivery_report)

        # Block until the messages are sent.
        self.producer.poll(1) # ms
        
    def batch_upload(self, chain: str, data_type: str, data_list: list):
        topic = f"{chain}-raw-{data_type.replace('_','-')}"
        for _data in data_list:
            json_str = json.dumps(_data, default=json_serializer)
            try:
                self.producer.produce(topic=topic, key=None, value=json_str.encode("utf-8"), on_delivery=self.delivery_report)
            except ProduceError as e:
                self.logger.error(f"[{topic}] Error! {e.kafka_message}")
                raise e
            except Exception as e:
                self.logger.error(f"[{topic}] Error! {e}\n{_data.keys()}")
                raise e
            # Block until the messages are sent.
            self.producer.poll(0) # ms
        
        self.producer.flush()