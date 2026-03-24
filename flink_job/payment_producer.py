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


def run_producer(high_failure=False):
    # high_failure=True raises failure rate to 30% to trigger alerts (validation exercise)

    producer = KafkaProducer(
        bootstrap_servers="kafka:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        # key_serializer routes messages to Kafka partitions by bank_code hash
        # 4 banks → 4 partitions → each bank consistently lands on same partition
    )

    failure_weights = [70, 30] if high_failure else [95, 5]
    mode = "HIGH FAILURE (30%)" if high_failure else "NORMAL (5%)"
    print(f"Starting Payment Producer — mode: {mode}")

    for i in range(1000):
        payment = {
            "transaction_id": fake.uuid4(),
            "bank_code": random.choice(banks),
            "amount": round(random.uniform(100, 50000), 2),
            "payment_type": random.choice(["NEFT", "IMPS", "RTGS"]),
            "status": random.choices(["SUCCESS", "FAILED"], weights=failure_weights)[0],
            "event_time": datetime.utcnow().isoformat(),
        }
        producer.send(
            "payments_raw",
            key=payment["bank_code"].encode(
                "utf-8"
            ),  # 👉 Kafka partitions by bank_code
            value=payment,
        )
        print(f"Sent: {payment}")
        time.sleep(0.1)

    producer.flush()
    print("Done.")


if __name__ == "__main__":
    import sys

    high_failure = "--high-failure" in sys.argv
    run_producer(high_failure=high_failure)
