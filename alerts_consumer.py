from kafka import KafkaConsumer
from configs import kafka_config
import json

# Створення Kafka Consumer
consumer = KafkaConsumer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda v: json.loads(v.decode('utf-8')) if v else None,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='alerts_reader_group',
)

MY_NAME = "roman.ianivskyi"
alert_topics = [
    f"{MY_NAME}_temperature_alerts",
    f"{MY_NAME}_humidity_alerts",
]

consumer.subscribe(alert_topics)
print(f"Subscribed to topics: {', '.join(alert_topics)}...")

# Зчитування та виведення сповіщень
try:
    for message in consumer:
        alert = message.value
        print(
            f"\n[ALERT | {message.topic}]"
            f"\n  Sensor ID: {alert.get('sensor_id')}"
            f"\n  Message:   {alert.get('message')}"
            f"\n  Timestamp: {alert.get('timestamp')}"
            f"\n  Raw data:  {alert}"
        )

except Exception as e:
    print(f"An error occurred: {e}")
finally:
    consumer.close()