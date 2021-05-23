"""

# start service
cd /home/z/kafka_2.13-2.8.0
bin/zookeeper-server-start.sh config/zookeeper.properties > /tmp/zookeeper.log 2>&1 &
bin/kafka-server-start.sh config/server.properties > /tmp/kafka.log 2>&1 &

# create topic
bin/kafka-topics.sh --create --topic quickstart-events --bootstrap-server localhost:9092

# produce
bin/kafka-console-producer.sh --topic quickstart-events --bootstrap-server localhost:9092

# consume
bin/kafka-console-consumer.sh --topic quickstart-events --from-beginning --bootstrap-server localhost:9092

KafkaProducer:
The producer is thread safe and sharing a single producer instance across threads will generally be faster than having multiple instances.
send() is asynchronous, and u should call flush() to block.
"""
import json

from kafka import KafkaProducer


class Sender:
    def __init__(self, bootstrap_servers):
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                                      value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    def send(self, topic, data_list):
        for data in data_list:
            print(f'send {data}')
            self.producer.send(topic, data)
        self.producer.flush()


if __name__ == '__main__':
    s = Sender(['localhost:9092'])
    topic = 'quickstart-events'
    data_list = [
        {'1': 2},
        {'3': 4}
    ]
    s.send(topic, data_list)