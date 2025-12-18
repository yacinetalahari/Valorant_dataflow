from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, round, when, lit
from          pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pymongo import MongoClient
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Configuration ---
KAFKA_BOOTSTRAP_SERVERS = 'kafka:29092'
KAFKA_TOPIC = 'valorant_stats_topic'
MONGO_URI = 'mongodb://mongodb:27017/'
MONGO_DATABASE = 'valorant_stats'
MONGO_COLLECTION = 'realtime_stats'

# 1. Define the Schema of the Incoming Kafka Message
# BASÉ SUR LES COLONNES DE TON CSV:
# player, assists, damage_received, headshots, tier, traded, kills, matches, deaths, damage
schema = StructType([
    StructField("player", StringType(), True),
    StructField("assists", DoubleType(), True),
    StructField("damage_received", DoubleType(), True),  # Correct: damage_received
    StructField("headshots", DoubleType(), True),
    StructField("tier", StringType(), True),
    StructField("traded", DoubleType(), True),
    StructField("kills", DoubleType(), True),
    StructField("matches", DoubleType(), True),
    StructField("deaths", DoubleType(), True),
    StructField("damage", DoubleType(), True),  # Correct: damage
    StructField("timestamp", DoubleType(), True)
])


# Function to save the processed data to MongoDB
def save_to_mongo(df, batch_id):
    """
    Writes the processed DataFrame batch to MongoDB.
    This function runs for every micro-batch processed by Spark.
    """
    if df.count() > 0:
        logger.info(f"--- Processing Batch ID: {batch_id} with {df.count()} records ---")

        # Convert Spark DataFrame to Pandas DataFrame for easier MongoDB insertion
        pandas_df = df.toPandas()
        records = pandas_df.to_dict('records')

        try:
            # Connect to MongoDB (using default credentials)
            client = MongoClient(MONGO_URI)
            db = client[MONGO_DATABASE]
            collection = db[MONGO_COLLECTION]

            # Upsert (update or insert) based on the 'player' column
            for record in records:
                # Add processing timestamp
                record['processed_at'] = current_timestamp().isoformat()

                # Use 'player' as the unique key
                collection.update_one(
                    {'player': record['player']},
                    {'$set': record},
                    upsert=True
                )

            client.close()
            logger.info(f"✅ Successfully saved/updated {len(records)} player records in MongoDB.")

        except Exception as e:
            logger.error(f"❌ Error writing to MongoDB: {e}")


# Main Spark Streaming setup
def run_spark_job():
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("ValorantRealtimeDataFlow") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint") \
        .getOrCreate()

    # Set log level to reduce noise
    spark.sparkContext.setLogLevel("WARN")

    logger.info("--- Spark Session Initialized. Reading from Kafka... ---")

    # Read Stream from Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # Convert binary value to string and parse JSON
    raw_df = kafka_df.selectExpr("CAST(value AS STRING) as json_value")

    parsed_df = raw_df.select(
        from_json(col("json_value"), schema).alias("data")
    ).select("data.*")

    # --- Real-Time Data Transformation ---

    # Calculate K/D Ratio (handle division by zero)
    processed_df = parsed_df.withColumn(
        "kd_ratio",
        round(
            col("kills") / when(col("deaths") == 0, lit(1)).otherwise(col("deaths")),
            2
        )
    ).withColumn(
        "damage_per_kill",
        round(
            col("damage") / when(col("kills") == 0, lit(1)).otherwise(col("kills")),
            2
        )
    ).withColumn(
        "headshot_percentage",
        round(
            (col("headshots") / when(col("kills") == 0, lit(1)).otherwise(col("kills"))) * 100,
            2
        )
    ).withColumn("processing_time", current_timestamp())

    # Select final columns to be saved
    final_df = processed_df.select(
        "player",
        "kills",
        "deaths",
        "assists",
        "headshots",
        "damage",
        "damage_received",
        "tier",
        "matches",
        "traded",
        "kd_ratio",
        "damage_per_kill",
        "headshot_percentage",
        "processing_time"
    )

    # --- Write Stream to MongoDB (Sink) ---
    query = final_df.writeStream \
        .foreachBatch(save_to_mongo) \
        .outputMode("append") \
        .trigger(processingTime='10 seconds') \
        .start()

    logger.info("--- Spark Streaming job started. Waiting for data... ---")
    query.awaitTermination()


if __name__ == "__main__":
    run_spark_job()