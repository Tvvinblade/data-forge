import json
import time
from kafka import KafkaProducer
import os

# Папка с файлами
DATA_DIR = "/root/data-forge/test_pipeline/test_json"

# Kafka настройки
KAFKA_BROKER = "10.8.0.1:29092"
TOPICS = {
    "browser_events.jsonl": "browser_events",
    "device_events.jsonl": "device_events",
    "geo_events.jsonl": "geo_events",
    "location_events.jsonl": "location_events"
}

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

for filename, topic in TOPICS.items():
    filepath = os.path.join(DATA_DIR, filename)
    print(f"Отправка данных из {filename} в топик {topic}")
    with open(filepath, "r") as f:
        for line in f:
            try:
                event = json.loads(line.strip())
                producer.send(topic, value=event)
            except json.JSONDecodeError:
                print(f"⚠️ Ошибка чтения JSON в {filename}: {line[:100]}")
    time.sleep(0.5)

producer.flush()
print("Все события успешно отправлены в Kafka.")
