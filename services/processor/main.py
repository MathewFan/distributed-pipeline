import os
import faust
import clickhouse_connect
from prometheus_client import Counter, start_http_server

BROKER = os.getenv("BROKER","redpanda:9092")
TOPIC  = os.getenv("TOPIC","transactions")

processed = Counter("events_processed_total","events processed")
start_http_server(9101)

app = faust.App("processor", broker=f"kafka://{BROKER}", value_serializer="raw")

class Tx(faust.Record, serializer="json"):
    tx_id: str
    user_id: int
    amount: float
    merchant: str
    ts: str

topic = app.topic(TOPIC, value_type=Tx)

client = clickhouse_connect.get_client(host="clickhouse", port=8123, username="default", password="")
client.command("""
CREATE TABLE IF NOT EXISTS transactions (
  ts DateTime64(3,'UTC'),
  tx_id String,
  user_id UInt32,
  amount Float64,
  merchant LowCardinality(String)
) ENGINE = MergeTree
ORDER BY (ts, user_id)
""")

@app.agent(topic)
async def consume(stream):
    async for tx in stream:
        client.insert(
            "transactions",
            [[tx.ts, tx.tx_id, tx.user_id, tx.amount, tx.merchant]],
            column_names=["ts","tx_id","user_id","amount","merchant"]
        )
        processed.inc()

if __name__ == "__main__":
    app.main()
