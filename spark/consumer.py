from kafka import KafkaConsumer
import json
from pymongo import MongoClient
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

consumer = KafkaConsumer(
    'valorant_stats_topic',
    bootstrap_servers=['kafka:29092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

client = MongoClient('mongodb://mongodb:27017/valorant_stats')
db = client.valorant_stats
collection = db.realtime_stats


def clean_numeric_value(value):
    """Convert string numbers like '1,155' to float 1155.0"""
    if isinstance(value, (int, float)):
        return float(value)
    elif isinstance(value, str):
        # Remove commas and convert
        cleaned = value.replace(',', '').strip()
        try:
            return float(cleaned)
        except ValueError:
            return 0.0
    else:
        return 0.0


logger.info("Starting Kafka consumer...")
for message in consumer:
    data = message.value

    # ========== NETTOYAGE DES DONNÉES ==========
    # Convertir les strings en nombres pour les colonnes critiques
    numeric_fields = ['assists', 'damage_received', 'traded', 'kills', 'deaths',
                      'headshots', 'damage', 'matches']

    for field in numeric_fields:
        if field in data:
            data[field] = clean_numeric_value(data[field])

    # ========== CALCULS PRINCIPAUX ==========
    # Calculate K/D ratio
    if data.get('kills') and data.get('deaths'):
        data['kd_ratio'] = round(data['kills'] / max(data['deaths'], 1), 2)

    # ========== CALCULS AVANCÉS ==========
    try:
        # Headshot percentage
        if data.get('headshots') and data.get('kills'):
            data['headshot_percentage'] = round((data['headshots'] / max(data['kills'], 1)) * 100, 2)

        # Damage per kill
        if data.get('damage') and data.get('kills'):
            data['damage_per_kill'] = round(data['damage'] / max(data['kills'], 1), 2)

        # Survival rate (assuming 10 rounds per match average)
        if data.get('deaths') and data.get('matches'):
            total_rounds = max(data['matches'] * 10, 1)
            data['survival_rate'] = round((1 - (data['deaths'] / total_rounds)) * 100, 2)

        # Impact score (weighted formula)
        # MAINTENANT les valeurs sont des nombres, pas des strings!
        if data.get('kills') and data.get('assists') and data.get('damage'):
            data['impact_score'] = round(
                data['kills'] * 0.4 +
                data['assists'] * 0.3 +
                data['damage'] / 1000 * 0.3,
                2
            )

    except (KeyError, TypeError, ZeroDivisionError) as e:
        logger.warning(f"Could not calculate all metrics for {data.get('player', 'Unknown')}: {e}")

    # ========== SAUVEGARDE ==========
    # Save to MongoDB
    collection.update_one(
        {'player': data['player']},
        {'$set': data},
        upsert=True
    )

    # Enhanced log message
    log_msg = f"Processed: {data['player']} - K/D: {data.get('kd_ratio', 'N/A')}"

    if 'headshot_percentage' in data:
        log_msg += f" | HS%: {data['headshot_percentage']}"
    if 'impact_score' in data:
        log_msg += f" | Impact: {data['impact_score']}"

    logger.info(log_msg)