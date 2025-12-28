import json
import csv
from confluent_kafka import Producer
conf ={
    'bootstrap.servers':'kafka:9092',
    'acks':'all',
    'retries':3,
}

producer = Producer(conf)

topic_name = 'bank-transactions'
csv_file_path = 'bank_transactions_simulated_eng.csv'

def delivery_report(err , msg):
    if err :
        print(f"delivery failed : {err}")
    else :
        print(f"Message Deliver to {msg.topic()} [{msg.partition()}]")

with open(csv_file_path , 'r') as file:
    reader = csv.DictReader(file)
    for row in reader:
        producer.produce(
            topic_name,
            value = json.dumps(row).encode('utf-8'),
            callback = delivery_report
        )
        producer.poll(0)
producer.flush()
print("all data Sent succesfully")
