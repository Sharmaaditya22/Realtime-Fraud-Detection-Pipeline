from kafka import KafkaProducer
from faker import Faker
from loguru import logger
import time
import random
import json

# Configuration
TOPIC_NAME = "raw_transactions"
BOOTSTRAP_SERVER = "localhost:9092"

bad_merchants_data = [
    ("Evil Corp", "High Risk"),
    ("Fake Store Ltd", "Banned"),
    ("Scam Hub", "Under Investigation")
]

# Initialize Faker
fake = Faker()

def get_producer():
    """Creates a Kafka Producer with JSON serialization."""
    return KafkaProducer(
        bootstrap_servers=[BOOTSTRAP_SERVER],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

def generate_transaction():
    """Generates a single fake transaction."""
    user_id = fake.random_int(min=1, max=1000)
    
    # --- 1. AMOUNT LOGIC (Existing) ---
    # 90% chance of normal amount, 10% chance of high amount
    if random.random() < 0.90:
        amount = round(random.uniform(10.0, 500.0), 2)
    else:
        amount = round(random.uniform(10000.0, 20000.0), 2)

    # --- 2. MERCHANT LOGIC (New) ---
    # 10% chance to pick a specific "Bad Merchant"
    if random.random() < 0.10:
        # random.choice picks one tuple like ("Evil Corp", "High Risk")
        # [0] grabs just the name "Evil Corp"
        merchant_name = random.choice(bad_merchants_data)[0]
    else:
        # 90% chance of a random legit company
        merchant_name = fake.company()

    transaction = {
        "transaction_id": fake.uuid4(),
        "user_id": user_id,
        "user_name": fake.name(),
        "amount": amount,
        "currency": "USD",
        "merchant": merchant_name,  # <--- Using the decided variable
        "location": fake.city(),
        "timestamp": time.time()
    }
    return transaction


if __name__ == "__main__":
    producer = get_producer()
    logger.info(f"Starting producer. sending data to {TOPIC_NAME}")
    try:
        while True:
            txn=generate_transaction()
            producer.send(TOPIC_NAME, value=txn)
            status= '[FRAUD]' if txn['amount'] > 10000 else '[NORMAL]'
            logger.info(f"status: {status}, transaction amount: {txn['amount']}, user_id: {txn['user_id']}, merchant: {txn['merchant']}")
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping producer...")
        producer.close()
