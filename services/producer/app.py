"""Synthetic event generator with delivery acknowledgements and bounded queues."""
from contextlib import asynccontextmanager
from datetime import datetime, timezone
import json
import os
import random
import threading
import uuid
from confluent_kafka import Producer
from fastapi import FastAPI, Response
from prometheus_client import Counter, Gauge, generate_latest, CONTENT_TYPE_LATEST

acked = Counter("producer_delivered_total", "Broker-confirmed successful deliveries")
failed = Counter("producer_failed_total", "Final delivery failures")
backpressure = Counter("producer_backpressure_total", "Queue-full events")
queued = Gauge("producer_queue_messages", "Messages awaiting broker acknowledgement")


def make_event():
    return dict(schema_version=1, tx_id=str(uuid.uuid4()), user_id=random.randint(1, 10000),
                amount=f"{random.randint(0, 999999) / 100:.2f}", merchant=random.choice(["shop", "travel", "food"]),
                ts=datetime.now(timezone.utc).isoformat())


@asynccontextmanager
async def lifespan(app):
    rate = float(os.getenv("RATE_PER_SEC", "200"))
    if not 0 < rate <= 10000:
        raise ValueError("RATE_PER_SEC must be in (0,10000]")
    producer = Producer({"bootstrap.servers": os.getenv("BROKER", "redpanda:9092"),
        "enable.idempotence": True, "acks": "all", "delivery.timeout.ms": 10000,
        "queue.buffering.max.messages": 10000})
    stopping = threading.Event()
    def delivered(error, _message):
        (failed if error else acked).inc()
    def run():
        while not stopping.is_set():
            payload = json.dumps(make_event()).encode()
            while not stopping.is_set():
                try:
                    producer.produce(os.getenv("TOPIC", "transactions"), value=payload, on_delivery=delivered)
                    break
                except BufferError:
                    backpressure.inc()
                    producer.poll(0.1)
            producer.poll(0)
            queued.set(len(producer))
            stopping.wait(1 / rate)
        producer.flush(12)
        queued.set(len(producer))
    worker = threading.Thread(target=run, name="event-producer")
    worker.start()
    app.state.worker = worker
    try:
        yield
    finally:
        stopping.set()
        worker.join(timeout=15)
        if worker.is_alive():
            raise RuntimeError("producer failed to shut down")


app = FastAPI(lifespan=lifespan)


@app.get("/health")
def health():
    return Response(status_code=200 if app.state.worker.is_alive() else 503)


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
