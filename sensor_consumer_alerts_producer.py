from kafka import KafkaConsumer, KafkaProducer
from configs import kafka_config
import json
import uuid

# Спільні параметри підключення до Kafka
kafka_conn = {
    "bootstrap_servers": kafka_config['bootstrap_servers'],
    "security_protocol": kafka_config['security_protocol'],
    "sasl_mechanism": kafka_config['sasl_mechanism'],
    "sasl_plain_username": kafka_config['username'],
    "sasl_plain_password": kafka_config['password'],
}

# Створення Kafka Consumer та Producer
consumer = KafkaConsumer(
    **kafka_conn,
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda v: json.loads(v.decode('utf-8')) if v else None,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='sensor_alerts_group',
)

producer = KafkaProducer(
    **kafka_conn,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: json.dumps(v).encode('utf-8'),
)

# Назви топіків
MY_NAME = "roman.ianivskyi"
sensors_topic = f"{MY_NAME}_building_sensors"
temp_alerts_topic = f"{MY_NAME}_temperature_alerts"
humidity_alerts_topic = f"{MY_NAME}_humidity_alerts"

consumer.subscribe([sensors_topic])
print(f"Subscribed to topic '{sensors_topic}'")

try:
    for message in consumer:
        data = message.value
        sensor_id = data.get("sensor_id")
        temperature = data.get("temperature")
        humidity = data.get("humidity")
        timestamp = data.get("timestamp")

        print(f"Received: {data}")

        if temperature is not None and temperature > 40:
            temp_alert = {
                "sensor_id": sensor_id,
                "temperature": temperature,
                "timestamp": timestamp,
                "message": f"Temperature threshold exceeded: {temperature}°C (max allowed: 40°C)",
            }
            producer.send(temp_alerts_topic, key=str(uuid.uuid4()), value=temp_alert)
            producer.flush()
            print(f" -> Sent temperature alert: {temp_alert}")

        if humidity is not None and (humidity < 20 or humidity > 80):
            humidity_alert = {
                "sensor_id": sensor_id,
                "humidity": humidity,
                "timestamp": timestamp,
                "message": f"Humidity out of range: {humidity}% (allowed: 20%-80%)",
            }
            producer.send(humidity_alerts_topic, key=str(uuid.uuid4()), value=humidity_alert)
            producer.flush()
            print(f" -> Sent humidity alert: {humidity_alert}")

except Exception as e:
    print(f"An error occurred: {e}")
finally:
    consumer.close()
    producer.close()