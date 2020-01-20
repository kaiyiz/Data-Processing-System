import argparse
import atexit
import json
import logging

import requests
import schedule
import time
import sys

from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError

formatter = logging.Formatter('[%(asctime)s] - %(funcName)s - %(message)s', datefmt='%a, %d %b %Y %H:%M:%S')
fh = logging.FileHandler('./producer.log')
sh = logging.StreamHandler(sys.stdout)
fh.setFormatter(formatter)
sh.setFormatter(formatter)

logger = logging.getLogger('data-producer')
logger.setLevel(logging.DEBUG)

logger.addHandler(fh)
logger.addHandler(sh)

API_BASE = 'http://localhost:3001'


def fetch_weather_data(producer, topic_name):
    """
    Helper function to retrieve data and send it to Kafka
    :param producer: KafkaProducer
    :param topic_name: string / topic name of Kafka
    :return: None
    """
    logger.debug("Start to fetch weather data.")

    try:
        response = requests.get('%s/weather' % API_BASE)
        weather_data = response.json()['weather-data']
        logger.info('Retrieved weather data: %s', weather_data)

        timestamp = time.time()

        payload = {
            'WeatherData': str(weather_data),
            'Timestamp': timestamp
        }

        producer.send(topic=topic_name, value=json.dumps(payload).encode('utf-8'), timestamp_ms=int(time.time()) * 1000)
        logger.debug('Sent weather information to Kafka')

    except KafkaTimeoutError as time_error:
        logger.warning('Failed to send message to kafka, caused by: %s', time_error.message)

    except Exception as e:
        logger.error('Failed to fetch weather data because %s', e)


def shutdown_hook(producer):
    try:
        producer.flush(10)
    except KafkaError as kafka_error:
        logging.warning('Failed to flush pending message to kafka, caused by: %s', kafka_error.message)
    finally:
        try:
            producer.close(10)
            logger.info('Kafka producer closed successfully!')
        except Exception as e:
            logging.warning('Failed to close kafka connection, caused by: %s', e)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('topic_name', help='the kafka topic push to')
    parser.add_argument('kafka_broker', help='the address of the kafka broker')

    # Parse argument
    args = parser.parse_args()
    topic_name_str = args.topic_name
    kafka_broker = args.kafka_broker

    # Instantiate a simple kafka producer
    producer = KafkaProducer(bootstrap_servers=kafka_broker)
    schedule.every(1).second.do(fetch_weather_data, producer, topic_name_str)

    # Setup proper shutdown hook
    atexit.register(shutdown_hook, producer)

    while True:
        schedule.run_pending()
        time.sleep(1)
