import json

from kafka import KafkaProducer
from fastapi import FastAPI

# produce json messages
producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda m: json.dumps(m).encode('ascii'))

app = FastAPI()


def on_send_success(record_metadata):
    print(record_metadata.topic)
    print(record_metadata.partition)
    print(record_metadata.offset)


def on_send_error(err):
    print(err)
    # handle exception


@app.get("/")
async def read_root():
    message = {"message": "Message added"}
    producer.send('json-topic', message)\
        .add_callback(on_send_success)\
        .add_errback(on_send_error)
    producer.flush()
    return message
