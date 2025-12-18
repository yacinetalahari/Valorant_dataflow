from producer import producer
import json
import time

producer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

test_player = {
    "player": "LIVE_DEMO_1",
    "kills": 30,
    "deaths": 3,
    "tier": "Radiant",
    "assists": 10,
    "damage": 4500,
    "matches": 5
}

producer.send('valorant_stats_topic', test_player)
producer.flush()
print(f"✅ ENVOYÉ: {test_player['player']} | Kills: {test_player['kills']} | Deaths: {test_player['deaths']}")