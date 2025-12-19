import asyncio
import aiohttp
from kafka import KafkaProducer
import os
from dotenv import load_dotenv
import json
import logging
from pathlib import Path

# config
BASE_DIR = Path(__file__).resolve().parent.parent
dotenv_path = BASE_DIR / ".env"
load_dotenv(dotenv_path)

API_KEY = os.getenv("TFNSW_API_KEY")
KAFKA_BROKER = os.getenv("KAFKA_BROKER")
API_URL = "https://api.transport.nsw.gov.au/v1/carpark"
KAFKA_TOPIC = "raw_events"

FACILITY_IDS = [x for x in range(6, 40)]

if not API_KEY:
    logging.error("API key not found! Set TFNSW_API_KEY in .env")
    raise ValueError("API key not found! Set TFNSW_API_KEY in .env")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    retries=5,
)

semaphore = asyncio.Semaphore(2)  # max 2 concurrent requests

# Fetch with semaphore
async def fetch(session, facility_id):
    params = {"facility": facility_id}
    url = API_URL
    try:
        async with semaphore: 
            await asyncio.sleep(0.5)
            async with session.get(url, headers={"Authorization": f"apikey {API_KEY}"}, params=params) as resp:
                resp.raise_for_status()
                data = await resp.json()
                logging.info(f"Fetched facility {facility_id}")
                return data
    except Exception as e:
        logging.error(f"Error fetching facility {facility_id}: {e}")
        return None

def send_to_kafka(producer, data_list):
    for record in data_list:
        if record is not None:
            try:
                producer.send(KAFKA_TOPIC, value=record)
            except Exception as e:
                logging.error(f"Error sending record to Kafka: {e}")
    producer.flush()
    logging.info(f"Sent {len([d for d in data_list if d is not None])} records to Kafka")

async def run_producer():
    async with aiohttp.ClientSession() as session:
        tasks = [fetch(session, fid) for fid in FACILITY_IDS] #concurent execution
        results = await asyncio.gather(*tasks)
        send_to_kafka(producer, results)
        logging.info(f"Completed fetching and sending data for {len(FACILITY_IDS)} facilities")
        print(f"Completed fetching and sending data for {len(FACILITY_IDS)} facilities")

def main():
    asyncio.run(run_producer())

if __name__ == "__main__":
    main()
