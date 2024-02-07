import os

import dotenv
from kafka import KafkaProducer


def main():
    producer = KafkaProducer(
        bootstrap_servers=os.getenv('URL'),
        api_version=(3, 3, 0),
        client_id=os.getenv('myGroup'),
    )
    print(f"Start sending on {os.getenv('TOPIC')} from {os.getenv('URL')}\n")
    for _ in range(1):
        print("Start send")
        producer.send(os.getenv('TOPIC'), b'{"msg": "Some msg"}', timestamp_ms=1000)
        print("End send")


if __name__ == '__main__':
    dotenv.load_dotenv('.env')
    main()
