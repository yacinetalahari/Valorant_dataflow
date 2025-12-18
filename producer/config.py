import os

# Kafka Configuration
# 'kafka:29092' is the internal Docker address for the Kafka container
KAFKA_BROKER = 'kafka:29092'
KAFKA_TOPIC = 'valorant_stats_topic'
# Data Path
# This path points to the 'valorant_stats.csv' inside the container
# This variable helps the producer.py script find the file correctly,
# even when running inside Docker.
CSV_FILE_PATH = '/app/data/valorant_stats.csv'
# Streaming Delay (0.5 seconds between sending each player's stat)
STREAM_DELAY_SECONDS = 0.5