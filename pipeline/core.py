"""Dependency-free validation and write-before-commit processing core."""
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import json
import time
import uuid


class InvalidEvent(ValueError):
    pass


def validate(raw):
    try:
        event = json.loads(raw)
        if not isinstance(event, dict) or type(event.get("schema_version")) is not int or event.get("schema_version") != 1:
            raise InvalidEvent("schema_version must be 1")
        tx_id = str(uuid.UUID(event["tx_id"]))
        user = event["user_id"]
        merchant = event["merchant"]
        if type(user) is not int or not 1 <= user <= 2**63 - 1:
            raise InvalidEvent("user_id out of range")
        if not isinstance(merchant, str) or not 1 <= len(merchant) <= 128:
            raise InvalidEvent("merchant length out of range")
        # A string contract prevents silent JSON float rounding before validation.
        if not isinstance(event["amount"], str) or len(event["amount"]) > 24:
            raise InvalidEvent("amount must be a decimal string")
        amount = Decimal(event["amount"])
        if not amount.is_finite() or amount < 0 or amount > Decimal("999999999999.99"):
            raise InvalidEvent("amount out of range")
        if amount != amount.quantize(Decimal("0.01")):
            raise InvalidEvent("amount has fractional cents")
        stamp = datetime.fromisoformat(event["ts"].replace("Z", "+00:00"))
        if stamp.tzinfo is None:
            raise InvalidEvent("ts must include timezone")
        stamp = stamp.astimezone(timezone.utc)
        if not 1970 <= stamp.year <= 2200:
            raise InvalidEvent("ts out of range")
        return tx_id, user, amount, merchant, stamp
    except InvalidEvent:
        raise
    except (ValueError, TypeError, KeyError, InvalidOperation, OverflowError, AttributeError) as error:
        # Parser exception text can echo the raw input (for example ISO dates).
        raise InvalidEvent("invalid event structure or value") from error


@dataclass(frozen=True)
class Record:
    topic: str
    partition: int
    offset: int
    value: bytes


class BatchProcessor:
    """Replay is safe only when sink reads deduplicate by source position."""
    def __init__(self, sink, commit, retries=3, sleep=time.sleep, observe=None):
        self.sink, self.commit = sink, commit
        self.retries, self.sleep = retries, sleep
        self.observe = observe or (lambda *_: None)

    def process(self, records):
        if not records:
            return
        valid, invalid, offsets = [], [], {}
        for record in records:
            key = (record.topic, record.partition, record.offset)
            try:
                valid.append((*key, *validate(record.value)))
            except InvalidEvent as error:
                # Bounded metadata only: do not persist potentially sensitive bad payloads.
                invalid.append((*key, str(error)[:256]))
            partition = (record.topic, record.partition)
            offsets[partition] = max(offsets.get(partition, 0), record.offset + 1)
        for attempt in range(self.retries + 1):
            try:
                self.sink.write(valid, invalid)
                break
            except Exception:
                if attempt == self.retries:
                    raise
                self.observe("retry", 1)
                self.sleep(min(0.1 * 2**attempt, 2))
        # A failure here deliberately replays persisted rows after restart.
        self.commit(offsets)
        self.observe("valid", len(valid))
        self.observe("quarantine", len(invalid))
