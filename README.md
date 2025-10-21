# distributed-pipeline

Redpanda (Kafka API) + Faust processor + ClickHouse pipeline.
- Bring up: `docker compose -f docker-compose.yml -f rp-override.yml up -d`
- Services: redpanda, clickhouse, producer, processor (Faust worker)
