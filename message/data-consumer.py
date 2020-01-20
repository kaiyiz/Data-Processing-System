import argparse

from kafka import KafkaConsumer


def consume(topic_name, kafka_broker):
    consumer = KafkaConsumer(topic_name, bootstrap_servers=kafka_broker, auto_offset_reset='smallest')

    for message in consumer:
        print(message)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('topic_name')
    parser.add_argument('kafka_broker')

    args = parser.parse_args()
    topic_name = args.topic_name
    kafka_broker = args.kafka_broker

    consume(topic_name, kafka_broker)
