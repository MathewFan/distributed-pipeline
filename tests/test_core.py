import json
import unittest
from pipeline.core import BatchProcessor, InvalidEvent, Record, validate


def event(**updates):
    result = dict(schema_version=1, tx_id="c0470d33-77c6-4fbc-8c38-877274725cc4",
                  user_id=3, amount="123456789.01", merchant="demo", ts="2026-01-01T00:00:00Z")
    result.update(updates)
    return json.dumps(result).encode()


class Sink:
    def __init__(self, failures=0):
        self.valid, self.bad, self.failures = {}, {}, failures
    def write(self, valid, invalid):
        self.valid.update((row[:3], row) for row in valid)
        if self.failures:
            self.failures -= 1
            raise OSError("crash after first table write")
        self.bad.update((row[:3], row) for row in invalid)


class RecoveryTests(unittest.TestCase):
    def test_partial_sink_retry_and_partition_offsets(self):
        sink, commits = Sink(1), []
        rows = [Record("t", 0, 2, event()), Record("t", 0, 3, b"bad"), Record("t", 1, 9, event())]
        BatchProcessor(sink, lambda offsets: commits.append(offsets), sleep=lambda _: None).process(rows)
        self.assertEqual(commits, [{("t", 0): 4, ("t", 1): 10}])
        self.assertEqual((len(sink.valid), len(sink.bad)), (2, 1))

    def test_no_commit_after_exhausted_sink_failure(self):
        sink, commits = Sink(99), []
        with self.assertRaises(OSError):
            BatchProcessor(sink, commits.append, sleep=lambda _: None).process([Record("t", 0, 0, event())])
        self.assertEqual(commits, [])

    def test_crash_between_write_and_commit_replays_same_identity(self):
        sink = Sink()
        rows = [Record("t", 0, 0, event()), Record("t", 0, 1, b"bad")]
        def crash(_): raise OSError("commit unavailable")
        with self.assertRaises(OSError): BatchProcessor(sink, crash).process(rows)
        BatchProcessor(sink, lambda _: None).process(rows)
        self.assertEqual((len(sink.valid), len(sink.bad)), (1, 1))

    def test_rejects_float_precision_bad_dates_and_nonfinite_amounts(self):
        for update in [dict(amount=1.1), dict(amount="NaN"), dict(amount="0.001"),
                       dict(amount="-1.00"), dict(ts="2026-01-01"), dict(user_id=True),
                       dict(schema_version=2), dict(tx_id="x"), dict(merchant=""), dict(ts="1500-01-01T00:00:00Z")]:
            with self.subTest(update=update), self.assertRaises(InvalidEvent): validate(event(**update))
        self.assertEqual(str(validate(event())[2]), "123456789.01")

    def test_empty_batch_does_not_commit(self):
        BatchProcessor(Sink(), lambda _: self.fail("unexpected commit")).process([])


if __name__ == "__main__": unittest.main()
