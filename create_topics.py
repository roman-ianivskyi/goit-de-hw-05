from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config


MY_NAME = "roman.ianivskyi"


# Створення клієнта Kafka
admin_client = KafkaAdminClient(
 bootstrap_servers=kafka_config['bootstrap_servers'],
 security_protocol=kafka_config['security_protocol'],
 sasl_mechanism=kafka_config['sasl_mechanism'],
 sasl_plain_username=kafka_config['username'],
 sasl_plain_password=kafka_config['password']
)

num_partitions = 2
replication_factor = 1


topics = ["building_sensors", "temperature_alerts", "humidity_alerts"]

new_topics = [
    NewTopic(
     name=f"{MY_NAME}_{topic}",
     num_partitions=num_partitions,
     replication_factor=replication_factor)
    for topic in topics
]

# Створення нового топіку
try:
    admin_client.create_topics(new_topics=new_topics)
    print("Topics created successfully.")
except Exception as e:
    print(f"An error occurred: {e}")

# Перевіряємо список існуючих топіків
print([t for t in admin_client.list_topics() if MY_NAME in t])

# Закриття зв'язку з клієнтом
admin_client.close()
