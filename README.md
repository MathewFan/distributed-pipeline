# Replay-safe streaming analytics

Redpanda → bounded Python consumer → ClickHouse. This project explores the failure window between writing analytics and committing Kafka offsets, with a reproducible crash/restart experiment rather than an exactly-once claim.

## Run

Requires Docker Compose v2. No cloud account is needed.

```sh
docker compose up --build -d
curl http://localhost:8000/health
curl http://localhost:8000/metrics
curl -u pipeline:local-demo-only 'http://localhost:8123/?query=SELECT%20count()%20FROM%20transactions'
python3 -m unittest discover -s tests -v
docker compose run --rm processor python -m tests.integration
docker compose down -v
```

The local-only password and loopback ports are development defaults. Broker traffic stays inside the Compose network. Production needs provisioned credentials, TLS, durable volumes, replication and capacity planning; this single-node demo does not model those guarantees. `down -v` discards the demo data.

## Failure semantics

The consumer uses explicit synchronous commits with automatic commit and automatic offset storage disabled. Each poll returns at most 100 records. Valid events and invalid-event metadata are written before committing each partition's next offset. Sink writes retry with bounded backoff; exhausted retries terminate the worker so uncommitted records replay. A failed commit also triggers replay. SIGTERM finishes the current batch before closing the consumer.

ClickHouse `ReplacingMergeTree` tables use `(topic, partition, offset)` as their identity. The public `transactions` and `quarantine` views use `FINAL`, so duplicate inserts from retries do not inflate logical counts before background merges. Raw tables can contain physical duplicates. Always query the views for correctness; `FINAL` has a read cost. These are source-position guarantees, not business transaction deduplication: the same tx_id published at two offsets is two input records. Topic deletion/recreation with offset reuse requires a new namespace/table or explicit retention policy.

There is no cross-system transaction. A crash can persist sink rows without a committed Kafka offset. Replay restores progress and logical deduplication; it does not make all external side effects exactly once. Invalid records go to durable quarantine metadata, with source position and a bounded reason. Raw invalid payloads are intentionally omitted. Use the retained Kafka source to investigate/replay them.

## Event contract

Schema version 1: UUID `tx_id`, positive integer `user_id`, a decimal **string** `amount` with at most two decimal places, a 1–128 character `merchant`, and timezone-aware ISO `ts` between 1970 and 2200. Money stays Decimal through validation and ClickHouse insertion; float inputs, fractional cents and nonfinite values are quarantined. The generator uses UUIDs and UTC timestamps.

## Observable delivery

The generator starts and stops through FastAPI lifespan. Broker delivery callbacks, rather than local enqueue calls, update `producer_delivered_total` and `producer_failed_total`. Its queue is bounded at 10,000 messages; queue-full backpressure pauses production while callbacks drain. Shutdown flushes within a deadline. `/health` reports worker liveness, not broker readiness. Prometheus at localhost:9090 scrapes producer and processor; `/metrics` exposes queue depth, backpressure, committed valid/quarantine counts, retries and batch latency. No throughput or latency number is claimed without a benchmark.

## Recovery experiment

`tests.integration` creates a fresh two-partition topic, publishes two valid and two invalid records, writes both sink tables, deliberately fails before committing, closes the consumer, and restarts the same group. It verifies the same offsets replay, then checks committed next offsets `[2, 2]` and conservation: **4 inputs = 2 valid + 2 quarantined**. It runs against real Redpanda and ClickHouse in CI. Dependency-free unit tests separately cover partial sink failure, retry exhaustion, commit failure, decimal validation and empty polls.

The earlier Faust per-record worker has been replaced with an explicit consumer so commit ordering and the failure window are directly visible. The placeholder k6 HTTP load script was removed: it did not measure broker-to-sink throughput. Increase `RATE_PER_SEC` (1–10000), observe the metrics, and report machine/setup details before drawing performance conclusions.
