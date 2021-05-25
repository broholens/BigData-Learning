"""

# start service
cd /home/z/kafka_2.13-2.8.0
bin/zookeeper-server-start.sh config/zookeeper.properties > /tmp/zookeeper.log 2>&1 &
bin/kafka-server-start.sh config/server.properties > /tmp/kafka.log 2>&1 &

# create topic
bin/kafka-topics.sh --create --topic student --bootstrap-server localhost:9092

# produce
bin/kafka-console-producer.sh --topic student --bootstrap-server localhost:9092

# consume
bin/kafka-console-consumer.sh --topic student --from-beginning --bootstrap-server localhost:9092

KafkaProducer:
The producer is thread safe and sharing a single producer instance across threads will generally be faster than having multiple instances.
send() is asynchronous, and u should call flush() to block.
"""
import json

from kafka import KafkaProducer

from bigdata.kafka_sender.faker import Faker


class Sender(KafkaProducer):

    batch_records_num = 10000

    def __init__(self, bootstrap_servers):
        super().__init__(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.faker = Faker()

    def send_data(self, topic):
        while 1:
            for _ in range(self.batch_records_num):
                self.send(topic, self.faker.gen_one_student())
            self.flush()
            print(f'send {self.batch_records_num} records to {topic} success')


if __name__ == '__main__':
    s = Sender(['localhost:9092'])
    topic = 'student'
    s.send_data(topic)