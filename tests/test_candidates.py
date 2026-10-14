"""Тесты очереди кандидатов музея: дедупликация, статусы, живучесть файла."""
from __future__ import annotations

import contextlib
import io
import json
import os
import tempfile
import unittest

from hubd.__main__ import main
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

    def test_decision_keeps_status_and_reason_together(self):
        row = self.add()
        result = self.queue.decide(row["key"], "rejected", "наша же публикация")
        self.assertIsNotNone(result)
        self.assertEqual(result["status"], "rejected")
        self.assertEqual(result["previous"], "new")
        self.assertEqual(result["reason"], "наша же публикация")
        self.assertTrue(result["changed"])
        self.assertFalse(result["repeat"])
        again = Candidates(self.path)
        stored = again.by_status("rejected")[0]
        self.assertEqual(stored["note"], "наша же публикация", "причина живёт в строке очереди")
        self.assertEqual(again.counts()["rejected"], 1)

    def test_decision_without_reason_does_not_invent_one(self):
        row = self.add()
        result = self.queue.decide(row["key"], "accepted", "")
        self.assertIsNotNone(result)
        self.assertTrue(result["changed"])
        self.assertEqual(result["reason"], "")
        self.assertEqual(Candidates(self.path).by_status("accepted")[0]["note"], "")

    def test_repeat_decision_on_decided_candidate_does_not_touch_the_file(self):
        row = self.add()
        self.queue.decide(row["key"], "rejected", "не изменение содержимого")
        with open(self.path, "rb") as fh:
            before = fh.read()
        repeat = self.queue.decide(row["key"], "rejected", "не изменение содержимого")
        self.assertIsNotNone(repeat)
        self.assertFalse(repeat["changed"], "решение уже принято — переписывать нечего")
        self.assertTrue(repeat["repeat"])
        self.assertEqual(repeat["reason"], "не изменение содержимого")
        with open(self.path, "rb") as fh:
            self.assertEqual(fh.read(), before)

    def test_repeat_decision_with_another_reason_updates_only_the_reason(self):
        row = self.add()
        self.queue.decide(row["key"], "rejected", "первая причина")
        again = self.queue.decide(row["key"], "rejected", "уточнённая причина")
        self.assertIsNotNone(again)
        self.assertTrue(again["changed"])
        self.assertTrue(again["repeat"])
        self.assertEqual(again["status"], "rejected")
        self.assertEqual(Candidates(self.path).by_status("rejected")[0]["note"],
                         "уточнённая причина")

    def test_decision_without_reason_keeps_the_note_that_is_already_there(self):
        row = self.add()
        self.queue.decide(row["key"], "rejected", "не наша находка")
        carried = self.queue.decide(row["key"], "accepted")
        self.assertIsNotNone(carried)
        self.assertTrue(carried["carried"], "прежнюю запись не выдаём за причину нового решения")
        self.assertEqual(carried["reason"], "не наша находка")

    def test_decide_on_unknown_candidate_returns_none(self):
        self.assertIsNone(self.queue.decide("нет такого", "rejected", "причина"))

    def test_decide_refuses_unknown_status(self):
        row = self.add()
        with self.assertRaises(ValueError):
            self.queue.decide(row["key"], "может быть", "причина")

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


class CandidatesCliTestCase(unittest.TestCase):
    """Интерфейс триажа глазами CLI: код возврата, причина в отчёте, строка в файле."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "candidates.jsonl")
        self.queue = Candidates(self.path, clock=Clock())
        row = self.queue.add(topic="museum/site", url="https://museum.invalid/",
                             digest="a" * 64, body="музей".encode("utf-8"))
        self.assertIsNotNone(row)
        self.row = row

    def tearDown(self):
        self.tmp.cleanup()

    def run_cli(self, *argv):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = main(["candidates", "--file", self.path, *argv])
        return code, out.getvalue()

    def test_reject_with_reason_is_accepted_and_reported(self):
        code, out = self.run_cli("--reject", self.row["key"], "--reason", "наша же публикация")
        self.assertEqual(code, 0, out)
        self.assertIn("отклонён кандидат %s (был new), причина: «наша же публикация»" % self.row["key"], out)
        self.assertIn("причина: наша же публикация", out)
        stored = Candidates(self.path).by_status("rejected")[0]
        self.assertEqual(stored["note"], "наша же публикация")
        self.assertEqual(stored["status"], "rejected")

    def test_accept_with_reason_is_accepted_and_reported(self):
        code, out = self.run_cli("--accept", self.row["key"], "--reason", "подтверждённая errata")
        self.assertEqual(code, 0, out)
        self.assertIn("принят кандидат %s (был new), причина: «подтверждённая errata»" % self.row["key"], out)
        self.assertEqual(Candidates(self.path).by_status("accepted")[0]["note"], "подтверждённая errata")

    def test_decision_without_reason_still_works_as_before(self):
        code, out = self.run_cli("--reject", self.row["key"])
        self.assertEqual(code, 0, out)
        self.assertIn("отклонён кандидат %s (был new), без причины" % self.row["key"], out)
        self.assertEqual(Candidates(self.path).by_status("rejected")[0]["note"], "")

    def test_repeat_decision_is_not_an_error_and_keeps_the_reason(self):
        self.run_cli("--reject", self.row["key"], "--reason", "не наша находка")
        code, out = self.run_cli("--reject", self.row["key"], "--reason", "не наша находка")
        self.assertEqual(code, 0, out)
        self.assertIn("уже отклонён", out)
        self.assertEqual(Candidates(self.path).by_status("rejected")[0]["note"], "не наша находка")

    def test_unknown_candidate_is_not_silently_reported_as_decided(self):
        code, out = self.run_cli("--reject", "deadbeef", "--reason", "проба интерфейса")
        self.assertEqual(code, 1, out)
        self.assertIn("кандидат deadbeef не найден", out)
        self.assertEqual(Candidates(self.path).counts()["new"], 1, "чужое решение файл не тронуло")

    def test_reason_without_decision_is_refused(self):
        code, out = self.run_cli("--reason", "причина в пустоту")
        self.assertEqual(code, 2, out)
        self.assertIn("--reason без решения", out)


if __name__ == "__main__":
    unittest.main()
