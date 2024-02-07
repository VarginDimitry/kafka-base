import json
import os
from pprint import pprint

import dotenv
from kafka import KafkaConsumer


def main():
    consumer = KafkaConsumer(
        os.getenv('TOPIC'),
        bootstrap_servers=[os.getenv('URL')],
        enable_auto_commit=True,
        api_version=(3, 3, 0),
        group_id=os.getenv('myGroup'),
        client_id=os.getenv('myGroup'),
    )
    print(f"Start getting on {os.getenv('TOPIC')} from {os.getenv('URL')}\n")
    for msg in consumer:
        print(msg)
        pprint(json.loads(msg.value.decode()))


if __name__ == '__main__':
    dotenv.load_dotenv('.env')
    main()
