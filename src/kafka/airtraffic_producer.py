import random
from json import dumps
from time import sleep
from kafka import KafkaProducer


producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda x: dumps(x).encode("utf-8")
)


while True:
    cities = [
        "São Paulo, Brazil",
        "Tokyo, Japan",
        "Berlin, Germany",
        "Rome, Italy",
        "Seoul, South Korea"
    ]

    aircraft_names = [
        "Convair B-36 Peacemaker",
        "Lockheed C-5 Galaxy",
        "Northrop B-2 Spirit",
        "Boeing B-52 Stratofortress",
        "McDonnell XF-85 Goblin"
    ]

    aircraft = {
        "aircraft_name": random.choice(aircraft_names),
        "from": random.choice(cities),
        "to": random.choice(cities),
        "passengers": random.randint(50, 101)
    }

    future = producer.send("airtraffic", value=aircraft)
    print(future.get(timeout=60))

    sleep(random.randint(1, 6))
