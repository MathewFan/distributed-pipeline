import os, time, random, json, threading
from datetime import datetime, timezone
from fastapi import FastAPI
from prometheus_client import Counter, Histogram, generate_latest
from fastapi.responses import PlainTextResponse
from confluent_kafka import Producer

BROKER = os.getenv("BROKER","redpanda:9092")
TOPIC  = os.getenv("TOPIC","transactions")
RATE   = float(os.getenv("RATE_PER_SEC","200"))

producer = Producer({"bootstrap.servers": BROKER})
app = FastAPI()

events_out = Counter("events_out_total","events produced")
latency_hist= Histogram("produce_latency_seconds","produce latency")

def make_event():
    amt = round(random.uniform(1, 1000), 2)
    return {
        "tx_id": f"tx_{random.randrange(1_000_000)}",
        "user_id": random.randint(1, 50_000),
        "amount": amt,
        "merchant": random.choice(["AMZ","APL","UBER","STBX","NFLX"]),
        "ts": datetime.now(timezone.utc).isoformat()
    }

def loop():
    if RATE <= 0:
        return
    per = 1.0 / RATE
    while True:
        start = time.perf_counter()
        payload = json.dumps(make_event()).encode()
        producer.produce(TOPIC, payload)
        producer.poll(0)
        events_out.inc()
        elapsed = time.perf_counter() - start
        latency_hist.observe(elapsed)
        sleep_for = per - elapsed
        if sleep_for > 0:
            time.sleep(sleep_for)

threading.Thread(target=loop, daemon=True).start()

@app.get("/metrics", response_class=PlainTextResponse)
def metrics():
    return PlainTextResponse(generate_latest().decode())

@app.get("/")
def root():
    return {"status":"ok","broker":BROKER,"topic":TOPIC,"rate_per_sec":RATE}
