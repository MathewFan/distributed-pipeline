"""Bounded batches; synchronous commits occur only after sink acknowledgement."""
import logging
import os
import signal
import time
from urllib.parse import urlparse
import clickhouse_connect
from confluent_kafka import Consumer, TopicPartition
from prometheus_client import Counter, Histogram, start_http_server
from pipeline.core import BatchProcessor, Record
from pipeline.sink import ClickHouseSink
from pipeline.broker import ensure_topic

outcomes = Counter("pipeline_records_total", "Committed outcomes and retries", ["outcome"])
latency = Histogram("pipeline_batch_seconds", "Sink and synchronous commit duration")


def main():
    logging.basicConfig(level=logging.INFO)
    url = urlparse(os.getenv("CLICKHOUSE_URL", "http://clickhouse:8123"))
    client = clickhouse_connect.get_client(host=url.hostname, port=url.port or 8123,
        username=os.getenv("CLICKHOUSE_USER", "pipeline"), password=os.environ["CLICKHOUSE_PASSWORD"],
        connect_timeout=5, send_receive_timeout=10)
    sink = ClickHouseSink(client)
    consumer = Consumer({"bootstrap.servers": os.getenv("BROKER", "redpanda:9092"),
        "group.id": os.getenv("GROUP_ID", "transactions-v2"), "auto.offset.reset": "earliest",
        "enable.auto.commit": False, "enable.auto.offset.store": False,
        "max.poll.interval.ms": 300000, "session.timeout.ms": 10000})
    def commit(offsets):
        results = consumer.commit(offsets=[TopicPartition(t, p, o) for (t, p), o in offsets.items()], asynchronous=False)
        if any(partition.error for partition in (results or [])):
            raise RuntimeError("offset commit failed")
    processor = BatchProcessor(sink, commit, observe=lambda kind, count: outcomes.labels(kind).inc(count))
    stopping = False
    def stop(*_):
        nonlocal stopping
        stopping = True
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    start_http_server(9101)
    topic = os.getenv("TOPIC", "transactions")
    ensure_topic(os.getenv("BROKER", "redpanda:9092"), topic)
    consumer.subscribe([topic])
    try:
        while not stopping:
            messages = consumer.consume(num_messages=100, timeout=1)
            records = []
            for msg in messages:
                if msg.error():
                    raise RuntimeError(str(msg.error()))
                records.append(Record(msg.topic(), msg.partition(), msg.offset(), msg.value() or b""))
            with latency.time():
                processor.process(records)
    finally:
        consumer.close()
        client.close()


if __name__ == "__main__": main()
