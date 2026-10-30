"""Explicit topic bootstrap: startup never races auto-topic creation."""
import time
from confluent_kafka import KafkaException, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic


def ensure_topic(broker, topic):
    admin = AdminClient({"bootstrap.servers": broker})
    for future in admin.create_topics([NewTopic(topic, num_partitions=3, replication_factor=1)]).values():
        try:
            future.result(timeout=30)
        except KafkaException as error:
            if error.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        metadata = admin.list_topics(topic=topic, timeout=5).topics.get(topic)
        if metadata and metadata.error is None and metadata.partitions:
            return
        time.sleep(0.2)
    raise TimeoutError("topic metadata unavailable")
