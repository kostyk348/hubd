"""Карантин сломанного источника и отчёт «без отпечатка».

Архивесса указала цену: 13,7 МБ тела, которое рвётся, — это до 41 МБ трафика за
неудачный обход. Поэтому после трёх неудач подряд источник уходит на паузу, и она
растёт (час, два, четыре… до суток), а успех её снимает. Второе её требование —
видеть молчащие источники с временем последнего успешного чтения: «добавлен час
назад» и «сломан месяц назад» выглядят одинаково, пока нет этой строки.
"""
from __future__ import annotations

import os
import tempfile
import unittest

from hubd.scan import Watcher


class _Clock:
    def __init__(self, start: float = 1_800_000_000.0):
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class QuarantineTest(unittest.TestCase):
    def _watcher(self, tmp: str, fetcher, clock=None) -> Watcher:
        return Watcher(os.path.join(tmp, "scan.json"), fetcher=fetcher,
                       clock=clock or _Clock(), min_interval=0)

    def test_three_failures_put_source_on_pause(self):
        calls = []

        def broken(url, headers):
            calls.append(url)
            raise ConnectionResetError("connection reset by peer")

        with tempfile.TemporaryDirectory() as tmp:
            watcher = self._watcher(tmp, broken)
            for _ in range(3):
                result = watcher.check("https://example.test/big.xml")
                self.assertEqual(result["status"], 0)
            self.assertEqual(len(calls), 3)
            paused = watcher.check("https://example.test/big.xml")
            self.assertTrue(paused.get("skipped"), "четвёртый обход не должен идти в сеть")
            self.assertEqual(len(calls), 3, "после трёх неудач запросов больше быть не должно")
            self.assertIn("карантин", paused["reason"])

    def test_success_clears_failures_and_pause(self):
        state = {"broken": True}

        def flaky(url, headers):
            if state["broken"]:
                raise ConnectionResetError("reset")
            return 200, {}, b"<xml/>"

        with tempfile.TemporaryDirectory() as tmp:
            clock = _Clock()
            watcher = self._watcher(tmp, flaky, clock)
            for _ in range(3):
                watcher.check("https://example.test/big.xml")
            self.assertTrue(watcher.check("https://example.test/big.xml").get("skipped"))

            clock.advance(2 * 3600)          # пауза истекла
            state["broken"] = False
            result = watcher.check("https://example.test/big.xml")
            self.assertEqual(result["status"], 200)
            entry = watcher.state["https://example.test/big.xml"]
            self.assertEqual(entry["fails"], 0)
            self.assertEqual(entry["quarantine_until"], 0)

    def test_missing_fingerprints_show_last_success(self):
        clock = _Clock()

        def ok(url, headers):
            return 200, {}, b"<xml/>"

        with tempfile.TemporaryDirectory() as tmp:
            watcher = self._watcher(tmp, ok, clock)
            watcher.check("https://example.test/fine.xml")
            watchlist = [
                {"url": "https://example.test/fine.xml", "topic": "fine"},
                {"url": "https://example.test/never.xml", "topic": "never"},
            ]
            missing = watcher.missing_fingerprints(watchlist)
            self.assertEqual([item["url"] for item in missing],
                             ["https://example.test/never.xml"])
            self.assertEqual(missing[0]["last_ok"], "никогда")
            self.assertEqual(missing[0]["topic"], "never")


if __name__ == "__main__":
    unittest.main()
