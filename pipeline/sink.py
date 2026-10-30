"""ClickHouse source-position deduplication. FINAL is essential for live reads."""
SCHEMA = [
    """CREATE TABLE IF NOT EXISTS transactions_raw (
      topic String, partition Int32, offset UInt64, tx_id UUID, user_id UInt64,
      amount Decimal(18,2), merchant String, ts DateTime64(3, 'UTC')
    ) ENGINE=ReplacingMergeTree ORDER BY (topic, partition, offset)""",
    """CREATE TABLE IF NOT EXISTS quarantine_raw (
      topic String, partition Int32, offset UInt64, reason String
    ) ENGINE=ReplacingMergeTree ORDER BY (topic, partition, offset)""",
    "CREATE VIEW IF NOT EXISTS transactions AS SELECT * FROM transactions_raw FINAL",
    "CREATE VIEW IF NOT EXISTS quarantine AS SELECT * FROM quarantine_raw FINAL",
]


class ClickHouseSink:
    def __init__(self, client):
        self.client = client
        for sql in SCHEMA:
            client.command(sql)

    def write(self, valid, invalid):
        if valid:
            self.client.insert("transactions_raw", valid,
                               column_names=["topic", "partition", "offset", "tx_id", "user_id", "amount", "merchant", "ts"])
        if invalid:
            self.client.insert("quarantine_raw", invalid,
                               column_names=["topic", "partition", "offset", "reason"])
