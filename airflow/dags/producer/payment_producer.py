import json  # convert dictionary to JSON
import time  # control TPS(Transactions Per Second)
import random  # generate random values
from faker import Faker  # generate fake Realistic data
from kafka import KafkaProducer  # send data to Kafka
from datetime import datetime  # add event timestamp

fake = Faker()

banks = ["HDFC", "ICICI", "SBI", "AXIS"]


def generate_payment():  # Payment Generator Function so this will return payment data with the following fields:
    return {
        "transaction_id": fake.uuid4(),  # Unique identifier
        "bank_code": random.choice(banks),
        "amount": round(random.uniform(100, 50000), 2),
        "payment_type": random.choice(["NEFT", "IMPS", "RTGS"]),
        "status": random.choices(
            ["SUCCESS", "FAILED"],
            weights=[
                95,
                5,
            ],  ## Later in Flink we calculate: And trigger alert if > 5%.So we intentionally simulate failure.
        )[0],
        "event_time": datetime.utcnow().isoformat(),
    }


def run_producer():  ## new line if fails remove this

    producer = KafkaProducer(  ## move to line 9 if fails.
        bootstrap_servers="kafka:9092",
        value_serializer=lambda v: json.dumps(v).encode(
            "utf-8"
        ),  # value_serializer: converts Python dictionary to JSON bytes
    )
    print("Starting Payment Producer...")
    for i in range(1000):
        payment = generate_payment()
        producer.send(
            "payments_raw", key=payment["bank_code"].encode("utf-8"), value=payment
        )
        print(f"Sent: {payment}")
        time.sleep(0.1)


if __name__ == "__main__":
    run_producer()  # call the producer function
