"""Тесты сканера источников: условный GET, дедупликация, вежливость, события для хаба."""
from __future__ import annotations

import os
import tempfile
import time
import unittest
import urllib.error

from hubd.scan import Watcher, load_watchlist


class FakeSource:
    """Поддельный источник: отдаёт заданные ответы и помнит заголовки запроса."""

    def __init__(self):
        self.answers: list[tuple[int, dict, bytes]] = []
        self.seen_headers: list[dict] = []
        self.times: list[float] = []

    def __call__(self, url: str, headers: dict):
        self.seen_headers.append(dict(headers))
        self.times.append(time.monotonic())
        if not self.answers:
            return 200, {}, b""
        return self.answers.pop(0)


class ScanTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.state = os.path.join(self.tmp.name, "scan.json")

    def tearDown(self):
        self.tmp.cleanup()

    def test_first_scan_only_introduces_source(self):
        src = FakeSource()
        src.answers = [(200, {"ETag": '"v1"'}, b"version 1")]
        watcher = Watcher(self.state, fetcher=src, min_interval=0)
        events = watcher.scan([{"url": "http://example.invalid/feed", "topic": "feed"}])
        self.assertEqual(events, [], "первое знакомство — не новость")
        entry = watcher.state["http://example.invalid/feed"]
        self.assertEqual(entry["status"], 200)
        self.assertEqual(entry["etag"], '"v1"')
        self.assertEqual(entry["changes"], 0)

    def test_second_scan_sends_conditional_headers_and_silent_on_304(self):
        src = FakeSource()
        src.answers = [(200, {"ETag": '"v1"'}, b"version 1"), (304, {}, b"")]
        watcher = Watcher(self.state, fetcher=src, min_interval=0)
        watchlist = [{"url": "http://example.invalid/feed", "topic": "feed"}]
        watcher.scan(watchlist)
        events = watcher.scan(watchlist)
        self.assertEqual(events, [])
        self.assertEqual(src.seen_headers[1].get("If-None-Match"), '"v1"')

    def test_changed_body_produces_event(self):
        src = FakeSource()
        src.answers = [(200, {"ETag": '"a"'}, b"old"), (200, {"ETag": '"b"'}, b"new content")]
        watcher = Watcher(self.state, fetcher=src, min_interval=0)
        watchlist = [{"url": "http://example.invalid/feed", "topic": "feed"}]
        watcher.scan(watchlist)
        events = watcher.scan(watchlist)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["payload"], b"new content")
        self.assertEqual(events[0]["topic"], "feed")
        self.assertEqual(watcher.state["http://example.invalid/feed"]["changes"], 1)

    def test_identical_body_without_etag_is_not_a_change(self):
        src = FakeSource()
        src.answers = [(200, {}, b"same"), (200, {}, b"same")]
        watcher = Watcher(self.state, fetcher=src, min_interval=0)
        watchlist = [{"url": "http://example.invalid/feed", "topic": "feed"}]
        watcher.scan(watchlist)
        self.assertEqual(watcher.scan(watchlist), [], "одинаковое содержимое — не событие")

    def test_http_error_is_recorded_not_raised(self):
        src = FakeSource()
        src.answers = [(200, {}, b"ok"), (500, {}, b"")]
        watcher = Watcher(self.state, fetcher=src, min_interval=0)
        watchlist = [{"url": "http://example.invalid/feed", "topic": "feed"}]
        watcher.scan(watchlist)
        watcher.scan(watchlist)
        self.assertEqual(watcher.state["http://example.invalid/feed"]["error"], "HTTP 500")
        self.assertEqual(watcher.state["http://example.invalid/feed"]["changes"], 0)

    def test_politeness_pause_between_requests_to_one_host(self):
        src = FakeSource()
        src.answers = [(200, {}, b"a"), (200, {}, b"b")]
        watcher = Watcher(self.state, fetcher=src, min_interval=0.25)
        watchlist = [{"url": "http://example.invalid/one", "topic": "one"},
                     {"url": "http://example.invalid/two", "topic": "two"}]
        watcher.scan(watchlist)
        gap = src.times[1] - src.times[0]
        self.assertGreaterEqual(gap, 0.2, "второй запрос к тому же хосту должен подождать")
        self.assertGreater(watcher.state["http://example.invalid/two"]["waited"], 0)

    def test_state_survives_restart(self):
        src = FakeSource()
        src.answers = [(200, {"ETag": '"v9"'}, b"body")]
        Watcher(self.state, fetcher=src, min_interval=0).scan(
            [{"url": "http://example.invalid/feed", "topic": "feed"}])
        again = Watcher(self.state, fetcher=FakeSource(), min_interval=0)
        self.assertEqual(again.state["http://example.invalid/feed"]["etag"], '"v9"')

    def test_load_watchlist_accepts_object_and_list(self):
        import json
        obj = os.path.join(self.tmp.name, "w1.json")
        lst = os.path.join(self.tmp.name, "w2.json")
        with open(obj, "w", encoding="utf-8") as fh:
            json.dump({"sources": [{"url": "http://a.invalid"}]}, fh)
        with open(lst, "w", encoding="utf-8") as fh:
            json.dump([{"url": "http://b.invalid"}], fh)
        self.assertEqual(len(load_watchlist(obj)), 1)
        self.assertEqual(len(load_watchlist(lst)), 1)


    def test_source_without_validators_suppresses_events(self):
        src = FakeSource()
        src.answers = [(200, {}, b"one"), (200, {}, b"two")]
        watcher = Watcher(self.state, fetcher=src, min_interval=0)
        watchlist = [{"url": "http://example.invalid/live", "topic": "live"}]
        watcher.scan(watchlist)
        self.assertEqual(watcher.scan(watchlist), [], "без ETag/Last-Modified событие ничего не значит")
        self.assertEqual(len(watcher.last_suppressed), 1)
        self.assertIn("allow_volatile", watcher.last_suppressed[0]["reason"])

    def test_allow_volatile_lets_events_through(self):
        src = FakeSource()
        src.answers = [(200, {}, b"one"), (200, {}, b"two")]
        watcher = Watcher(self.state, fetcher=src, min_interval=0)
        watchlist = [{"url": "http://example.invalid/live", "topic": "live", "allow_volatile": True}]
        watcher.scan(watchlist)
        events = watcher.scan(watchlist)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["payload"], b"two")
        self.assertEqual(watcher.last_suppressed, [])

    # --- живучесть обхода ----------------------------------------------------
    def test_network_failure_is_recorded_and_does_not_break_scan(self):
        class Broken(FakeSource):
            def __call__(self, url, headers):
                raise urllib.error.URLError("сеть недоступна")

        watcher = Watcher(self.state, fetcher=Broken(), min_interval=0)
        watchlist = [{"url": "http://example.invalid/feed", "topic": "feed"}]
        self.assertEqual(watcher.scan(watchlist), [])
        self.assertEqual(len(watcher.last_errors), 1)
        self.assertIn("недоступен", watcher.last_errors[0]["error"])
        self.assertEqual(watcher.state["http://example.invalid/feed"]["status"], 0)

    def test_broken_source_does_not_hide_change_from_the_next_one(self):
        class Mixed(FakeSource):
            def __call__(self, url, headers):
                if "broken" in url:
                    raise OSError("соединение сброшено")
                return super().__call__(url, headers)

        src = Mixed()
        src.answers = [(200, {"ETag": '"a"'}, b"old"), (200, {"ETag": '"b"'}, b"new")]
        watcher = Watcher(self.state, fetcher=src, min_interval=0)
        watchlist = [{"url": "http://example.invalid/broken", "topic": "broken"},
                     {"url": "http://example.invalid/fine", "topic": "fine"}]
        watcher.scan(watchlist)
        events = watcher.scan(watchlist)
        self.assertEqual([e["topic"] for e in events], ["fine"])
        self.assertEqual(len(watcher.last_errors), 1)
        self.assertEqual(watcher.last_errors[0]["topic"], "broken")

    def test_source_on_dead_port_is_recorded_with_the_real_fetcher(self):
        """Настоящий сетевой сбой (порт закрыт) — результат обхода, а не исключение."""
        watcher = Watcher(self.state, min_interval=0)
        result = watcher.check("http://127.0.0.1:1/nope")
        self.assertEqual(result["status"], 0)
        self.assertIn("недоступен", result["error"])
        self.assertFalse(result["changed"])


if __name__ == "__main__":
    unittest.main()