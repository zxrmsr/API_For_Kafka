from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer, KafkaConsumer
import pandas as pd
import json

class KafkaAPI:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

    def create_topic(self, topic_name):
        topic_list = [NewTopic(name=topic_name, num_partitions=1, replication_factor=1)]
        self.admin_client.create_topics(new_topics=topic_list, validate_only=False)

    def delete_topic(self, topic_name):
        self.admin_client.delete_topics(topics=[topic_name])

    def write_to_topic(self, df, topic_name):
        for index, row in df.iterrows():
            data = json.dumps(row.to_dict(), default=str).encode('utf-8')
            self.producer.send(topic=topic_name, value=data)

    def read_from_topic(self, topic_name):
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=self.producer.bootstrap_servers,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=False
        )

        messages = []
        for message in consumer:
            messages.append(message.value)
        
        consumer.close()
        return pd.DataFrame(messages)