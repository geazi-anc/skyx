from json import loads
from kafka import KafkaConsumer


consumer = KafkaConsumer(
    "airtraffic",
    bootstrap_servers="localhost:29092",
    value_deserializer=lambda x: loads(x.decode("utf-8"))
)

for msg in consumer:
    print(msg.value)
