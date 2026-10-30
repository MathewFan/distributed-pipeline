"""Real Redpanda + ClickHouse crash/restart and conservation check."""
import json
import os
import time
import uuid
import clickhouse_connect
from confluent_kafka import Producer, Consumer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from pipeline.core import BatchProcessor, Record
from pipeline.sink import ClickHouseSink
from tests.test_core import event


def main():
    broker = os.getenv("BROKER", "redpanda:9092")
    topic = "recovery-" + uuid.uuid4().hex
    admin = AdminClient({"bootstrap.servers": broker})
    for future in admin.create_topics([NewTopic(topic, num_partitions=2, replication_factor=1)]).values():
        future.result(timeout=30)
    producer = Producer({"bootstrap.servers": broker, "enable.idempotence": True})
    errors = []
    for partition in range(2):
        for payload in [event(), b"invalid-json"]:
            producer.produce(topic, partition=partition, value=payload,
                             on_delivery=lambda err, _: errors.append(err) if err else None)
    assert producer.flush(30) == 0 and not errors, errors
    client = clickhouse_connect.get_client(host="clickhouse", username="pipeline",
                                           password=os.environ["CLICKHOUSE_PASSWORD"])
    sink = ClickHouseSink(client)
    group = topic + "-group"
    def consumer():
        result = Consumer({"bootstrap.servers": broker, "group.id": group,
            "auto.offset.reset": "earliest", "enable.auto.commit": False,
            "enable.auto.offset.store": False})
        result.subscribe([topic])
        return result
    def read_four(source):
        rows, deadline = [], time.monotonic() + 45
        while len(rows) < 4 and time.monotonic() < deadline:
            msg = source.poll(1)
            if msg is None: continue
            assert not msg.error(), msg.error()
            rows.append(Record(msg.topic(), msg.partition(), msg.offset(), msg.value()))
        assert len(rows) == 4, rows
        return rows
    first = consumer()
    rows = read_four(first)
    def crash(_): raise RuntimeError("injected failure after both sink writes")
    try:
        BatchProcessor(sink, crash).process(rows)
        raise AssertionError("expected injected crash")
    except RuntimeError:
        pass
    finally:
        first.close()
    restarted = consumer()
    replay = read_four(restarted)
    assert {(r.partition, r.offset) for r in rows} == {(r.partition, r.offset) for r in replay}
    def commit(offsets):
        result = restarted.commit(offsets=[TopicPartition(t, p, o) for (t, p), o in offsets.items()], asynchronous=False)
        assert all(not item.error for item in result)
    BatchProcessor(sink, commit).process(replay)
    committed = restarted.committed([TopicPartition(topic, i) for i in range(2)], timeout=10)
    assert [p.offset for p in committed] == [2, 2], committed
    restarted.close()
    def count(table):
        return client.query(f"SELECT count() FROM {table} WHERE topic = {{topic:String}}",
                            parameters={"topic": topic}).first_row[0]
    assert count("transactions") == 2
    assert count("quarantine") == 2
    assert count("transactions") + count("quarantine") == 4
    # Background merges may already have removed physical duplicates; no raw count assertion.
    print(json.dumps({"input": 4, "valid": 2, "quarantined": 2, "committed_offsets": [2, 2],
                      "crash_replay": "passed", "topic": topic}))
    client.close()


if __name__ == "__main__": main()
