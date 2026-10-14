"""Тесты очереди кандидатов музея: дедупликация, статусы, живучесть файла."""
from __future__ import annotations

import json
import os
import tempfile
import unittest

from hubd.candidates import Candidates, candidate_key


class Clock:
    def __init__(self, start: float = 1_000_000.0):
        self.now = start

    def __call__(self) -> float:
        return self.now


class CandidatesTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "candidates.jsonl")
        self.clock = Clock()
        self.queue = Candidates(self.path, clock=self.clock)

    def tearDown(self):
        self.tmp.cleanup()

    def add(self, digest="a" * 64, topic="rfc/rfc3170/status", body=b"body", url="http://s.invalid/x"):
        return self.queue.add(topic=topic, url=url, digest=digest, body=body)

    def test_first_candidate_is_added_and_second_identical_is_not(self):
        first = self.add()
        self.assertIsNotNone(first)
        self.assertEqual(first["status"], "new")
        self.assertEqual(first["key"], candidate_key("rfc/rfc3170/status", "http://s.invalid/x", "a" * 64))
        self.assertIsNone(self.add(), "повторный обход с тем же содержимым дублей не даёт")
        self.assertEqual(len(self.queue.all()), 1)

    def test_same_source_with_new_content_is_a_new_candidate(self):
        self.add(digest="a" * 64)
        self.add(digest="b" * 64)
        self.assertEqual(len(self.queue.all()), 2)

    def test_source_returning_to_old_version_is_not_queued_twice(self):
        self.add(digest="a" * 64)
        self.add(digest="b" * 64)
        self.assertIsNone(self.add(digest="a" * 64), "откат источника — тот же кандидат")
        self.assertEqual(self.queue.counts()["new"], 2)

    def test_queue_survives_restart(self):
        self.add()
        again = Candidates(self.path)
        self.assertEqual(len(again.all()), 1)
        self.assertIsNone(again.add(topic="rfc/rfc3170/status", url="http://s.invalid/x",
                                    digest="a" * 64, body=b"body"),
                          "дедупликация работает и после перезапуска")

    def test_statuses_are_set_by_triage_and_counted(self):
        row = self.add()
        self.assertTrue(self.queue.set_status(row["key"], "accepted"))
        self.assertFalse(self.queue.set_status(row["key"], "accepted"), "повторно не меняем")
        second = self.add(digest="c" * 64)
        self.assertIsNotNone(second)
        self.queue.set_status(second["key"], "rejected")
        counts = self.queue.counts()
        self.assertEqual(counts, {"new": 0, "accepted": 1, "rejected": 1})
        self.assertEqual(len(self.queue.by_status("accepted")), 1)

    def test_status_change_is_persisted(self):
        row = self.add()
        self.queue.set_status(row["key"], "accepted")
        self.assertEqual(Candidates(self.path).counts()["accepted"], 1)

    def test_unknown_status_is_refused(self):
        row = self.add()
        with self.assertRaises(ValueError):
            self.queue.set_status(row["key"], "может быть")

    def test_broken_lines_are_not_candidates(self):
        self.add()
        with open(self.path, "a", encoding="utf-8") as fh:
            fh.write("это не json\n\n")
        again = Candidates(self.path)
        self.assertEqual(len(again.all()), 1)

    def test_sample_is_kept_for_text_and_dropped_for_binary(self):
        text = self.add(digest="d" * 64, body="RFC 3170: текст".encode("utf-8"))
        self.assertIn("RFC 3170", text["sample"])
        binary = self.add(digest="e" * 64, body=b"\x00\x01\x02binary")
        self.assertEqual(binary["sample"], "", "из бинарного выдержку не выдумываем")
        self.assertEqual(binary["bytes"], 9)

    def test_rows_are_valid_jsonl(self):
        self.add()
        self.add(digest="f" * 64, topic="other/topic")
        with open(self.path, encoding="utf-8") as fh:
            rows = [json.loads(line) for line in fh if line.strip()]
        self.assertEqual(len(rows), 2)
        self.assertEqual({row["topic"] for row in rows}, {"rfc/rfc3170/status", "other/topic"})


if __name__ == "__main__":
    unittest.main()
