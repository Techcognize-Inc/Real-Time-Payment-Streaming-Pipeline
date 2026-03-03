import json     # convert dictionary to JSON
import time     # control TPS(Transactions Per Second)
import random   # generate random values
from faker import Faker  # generate fake Realistic data
from kafka import KafkaProducer  # send data to Kafka
from datetime import datetime  # add event timestamp

fake = Faker()    # This creates a fake data generator. for example, fake.uuid4() generates a random UUID, and fake.name() generates a random name.
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  
    value_serializer=lambda v: json.dumps(v).encode('utf-8') # value_serializer: converts Python dictionary to JSON bytes
)

banks = ["HDFC", "ICICI", "SBI", "AXIS"] 

def generate_payment():  # Payment Generator Function so this will return payment data with the following fields:
    return {
        "transaction_id": fake.uuid4(),  # Unique identifier 
        "bank_code": random.choice(banks), 
        "amount": round(random.uniform(100, 50000), 2), 
        "payment_type": random.choice(["NEFT", "IMPS", "RTGS"]), 
        "status": random.choices(
            ["SUCCESS", "FAILED"],
            weights=[95, 5] # 95% chance of SUCCESS and 5% chance of FAILED. Later in Flink we calculate: And trigger alert if > 5%.So we intentionally simulate failure.
        )[0],
        "event_time": datetime.utcnow().isoformat() 
    }

print(" Starting Payment Producer...")

while True: # Infinite Loop 
    payment = generate_payment()

    producer.send(            # Send to Kafka topic
        "payments.raw",
        key=payment["bank_code"].encode("utf-8"),  # key is bank_code, send to same partition for same bank_code, Preserve ordering per bank, Allow Flink keyed state. Without key → random partitioning.
        value=payment
    )

    print(f"Sent: {payment}")

    time.sleep(0.1)  # 0.1 seconds = 10 transactions per second.