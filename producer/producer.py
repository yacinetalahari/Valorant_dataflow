import pandas as pd
import json
import time
from kafka import KafkaProducer
from config import KAFKA_BROKER, KAFKA_TOPIC, CSV_FILE_PATH, STREAM_DELAY_SECONDS


def get_kafka_producer():
    """Initializes and returns a KafkaProducer instance."""
    print("Attempting to connect to Kafka...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            # Serializer converts Python objects (dictionaries) into bytes for Kafka
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=5,
            retry_backoff_ms=1000,
            api_version=(0, 10, 1)
        )
        print("✅ Kafka Producer connected successfully.")
        return producer
    except Exception as e:
        print(f"❌ Could not connect to Kafka: {e}")
        time.sleep(5)
        return get_kafka_producer()


def stream_data(producer):
    """Reads Valorant stats CSV and streams data to Kafka."""
    print(f"Loading data from {CSV_FILE_PATH}...")

    try:
        df = pd.read_csv(CSV_FILE_PATH, encoding='utf-8')
    except FileNotFoundError:
        print(f"❌ Error: CSV file not found at {CSV_FILE_PATH}. Check the 'data' folder.")
        return

    # CORRECTION ICI: Les bonnes colonnes de ton CSV
    # Ton CSV a: player, assists, damage_received, headshots, tier, traded, kills, matches, deaths, damage
    df.columns = ['player', 'assists', 'damage_received', 'headshots', 'tier', 'traded', 'kills', 'matches', 'deaths', 'damage']

    print(f"Streaming {len(df)} records to topic '{KAFKA_TOPIC}'...")
    print(f"Columns: {df.columns.tolist()}")

    for index, row in df.iterrows():
        # Convert row to dictionary and add a timestamp
        data = row.to_dict()
        data['timestamp'] = time.time()

        # Send the JSON data to the Kafka topic
        producer.send(KAFKA_TOPIC, value=data)

        print(f"-> Sent: Player {data['player']}, Kills: {data['kills']}, Damage: {data['damage']}")

        time.sleep(STREAM_DELAY_SECONDS)

    print("\n✅ Finished streaming all records.")
    producer.flush()


if __name__ == "__main__":
    producer = get_kafka_producer()

    while True:  # Boucle infinie
        stream_data(producer)
        print("⏳ Attente 30 secondes avant prochain envoi...")
        time.sleep(30)