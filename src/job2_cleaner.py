import os
import json
import logging
import pandas as pd
from kafka import KafkaConsumer
from dotenv import load_dotenv
from pathlib import Path
from db_utils import create_events_table, insert_events

#config
BASE_DIR = Path(__file__).resolve().parent.parent
dotenv_path = BASE_DIR / ".env"
load_dotenv(dotenv_path)

KAFKA_BROKER = os.getenv("KAFKA_BROKER")
KAFKA_TOPIC = "raw_events"

def clean_record_pandas(raw):

    df = pd.DataFrame([raw])

    # facility_id
    df['facility_id'] = pd.to_numeric(df.get('facility_id'), errors='coerce')

    # name
    df['name'] = df.get('facility_name')

    # spots
    df['spots'] = pd.to_numeric(df.get('spots'), errors='coerce').fillna(0)

    # occupied
    df['total'] = df.get('occupancy').apply(lambda x: int(x.get('total', 0)) if isinstance(x, dict) else 0)

    # available
    df['available'] = (df['spots'] - df['total']).clip(lower=0)

    # status
    df['almost_full_threshold'] = (df['spots'] * 0.1).astype(int)
    df['status'] = df.apply(
        lambda row: "Full" if row['available'] < 1 
        else "Almost Full" if row['available'] <= row['almost_full_threshold']
        else "Available",
        axis=1
    )

    # coords
    df['lat'] = df.get('location').apply(lambda x: float(x.get('latitude', 0)) if isinstance(x, dict) else 0.0)
    df['lon'] = df.get('location').apply(lambda x: float(x.get('longitude', 0)) if isinstance(x, dict) else 0.0)

    # timestamp
    df['timestamp'] = df.get('MessageDate')

    # final selection
    df_cleaned = df[['facility_id', 'name', 'spots', 'total', 'available', 'status', 'lat', 'lon', 'timestamp']]

    return df_cleaned.to_dict(orient='records')[0]  


def run_consumer():
    create_events_table()
    
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id="job2_cleaner_group",
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )

    logging.info("Consumer started, waiting for messages...")
    print("Consumer started, waiting for messages...")

    for message in consumer:
        raw_record = message.value
        print("Message received")
        cleaned = clean_record_pandas(raw_record)
        print(f"Cleaned record: {cleaned}")
        insert_events([cleaned])
        logging.info(f"Processed facility {cleaned['facility_id']}: {cleaned['status']}")
        print(f"Processed facility {cleaned['facility_id']}: {cleaned['status']}")


if __name__ == "__main__":
    run_consumer()
