"""Тесты адаптеров: plain / json / rss / atom → события сканера; состояние переживает обходы."""
from __future__ import annotations

import json
import os
import tempfile
import unittest
from types import SimpleNamespace

from hubd.adapters import (AdapterError, canonical_json, feed_events, json_events,
                           plain_events, run_adapter, sha256_hex)
from hubd.scan import Watcher

RSS_V1 = b"""<?xml version="1.0"?>
<rss version="2.0"><channel><title>feed</title>
<item><guid>post-1</guid><title>First</title>
<link>http://x.example/1</link><description>hello</description></item>
<item><guid>post-2</guid><title>Second</title><link>http://x.example/2</link></item>
</channel></rss>"""

RSS_V2 = b"""<?xml version="1.0"?>
<rss version="2.0"><channel><title>feed</title>
<item><guid>post-1</guid><title>First</title>
<link>http://x.example/1</link><description>hello</description></item>
<item><guid>post-2</guid><title>Second (edited)</title><link>http://x.example/2</link></item>
<item><guid>post-3</guid><title>Third</title><link>http://x.example/3</link></item>
</channel></rss>"""

ATOM_V1 = b"""<?xml version="1.0"?>
<feed xmlns="http://www.w3.org/2005/Atom"><title>a</title>
<entry><id>tag:x,1</id><title>Atom one</title>
<link rel="alternate" href="http://x.example/a1"/>
<updated>2024-01-01T00:00:00Z</updated><summary>sum</summary></entry>
</feed>"""


class AdapterTestCase(unittest.TestCase):
    def test_plain_event_is_whole_body(self):
        result = {"url": "http://x.example/page", "body": b"payload",
                  "hash": sha256_hex(b"payload")}
        events = plain_events({"topic": "t/page"}, result, {})
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["topic"], "t/page")
        self.assertEqual(events[0]["payload"], b"payload")
        self.assertEqual(events[0]["hash"], result["hash"])
        self.assertEqual(events[0]["content_type"], "application/octet-stream")

    def test_json_extracts_path_and_remembers_digest(self):
        item = {"topic": "app/releases", "adapter": "json", "json_path": "tag_name"}
        state = {}
        body = json.dumps({"tag_name": "v1.0", "noise": "x"}).encode()
        events = json_events(item, {"url": "http://api.example/latest",
                                    "body": body, "hash": sha256_hex(body)}, state)
        self.assertEqual(json.loads(events[0]["payload"]), "v1.0")
        noisy = json.dumps({"tag_name": "v1.0", "noise": "y"}).encode()
        self.assertEqual(json_events(item, {"url": "u", "body": noisy,
                                            "hash": sha256_hex(noisy)}, state),
                         [], "тело шумит, значение то же — событий нет")
        bump = json.dumps({"tag_name": "v1.1"}).encode()
        events = json_events(item, {"url": "u", "body": bump,
                                    "hash": sha256_hex(bump)}, state)
        self.assertEqual(json.loads(events[0]["payload"]), "v1.1")

    def test_json_missing_path_is_adapter_error(self):
        with self.assertRaises(AdapterError):
            json_events({"adapter": "json", "json_path": "no.such"},
                        {"url": "u", "body": b"{}", "hash": "h"}, {})

    def test_json_invalid_body_is_adapter_error(self):
        with self.assertRaises(AdapterError):
            json_events({"adapter": "json"}, {"url": "u", "body": b"<xml>", "hash": "h"}, {})

    def test_feed_parses_rss_and_remembers_seen(self):
        item = {"topic": "blog", "adapter": "rss"}
        state = {}
        events = feed_events(item, {"url": "http://x.example/rss",
                                    "body": RSS_V1, "hash": "h"}, state)
        self.assertEqual([e["topic"] for e in events], ["blog/post-1", "blog/post-2"])
        self.assertEqual(events[0]["url"], "http://x.example/1")
        payload = json.loads(events[0]["payload"])
        self.assertEqual(payload["title"], "First")
        self.assertEqual(feed_events(item, {"url": "http://x.example/rss",
                                            "body": RSS_V1, "hash": "h"}, state), [])

    def test_feed_edited_entry_is_event_again(self):
        item = {"topic": "blog", "adapter": "rss"}
        state = {}
        feed_events(item, {"url": "u", "body": RSS_V1, "hash": "h"}, state)
        events = feed_events(item, {"url": "u", "body": RSS_V2, "hash": "h"}, state)
        self.assertEqual([e["topic"] for e in events], ["blog/post-2", "blog/post-3"])

    def test_feed_per_entry_false_single_topic(self):
        item = {"topic": "blog/all", "adapter": "rss", "per_entry": False}
        events = feed_events(item, {"url": "u", "body": RSS_V1, "hash": "h"}, {})
        self.assertEqual({e["topic"] for e in events}, {"blog/all"})

    def test_feed_max_entries_caps_burst(self):
        item = {"topic": "blog", "adapter": "rss", "max_entries": 1}
        events = feed_events(item, {"url": "u", "body": RSS_V1, "hash": "h"}, {})
        self.assertEqual(len(events), 1)

    def test_atom_namespace(self):
        events = feed_events({"topic": "a", "adapter": "atom"},
                             {"url": "u", "body": ATOM_V1, "hash": "h"}, {})
        self.assertEqual(events[0]["topic"], "a/tag-x-1")
        self.assertEqual(events[0]["url"], "http://x.example/a1")

    def test_garbage_feed_is_adapter_error(self):
        with self.assertRaises(AdapterError):
            feed_events({"adapter": "rss"}, {"url": "u", "body": b"not xml", "hash": "h"}, {})

    def test_unknown_adapter_name(self):
        with self.assertRaises(AdapterError):
            run_adapter({"adapter": "csv"}, {"url": "u", "body": b"", "hash": "h"}, {})

    def test_run_adapter_normalizes_plugin_payload(self):
        plugin = SimpleNamespace(extract=lambda item, result, state: [{"payload": {"a": 1}}])
        events = run_adapter({"plugin": "p", "topic": "t"},
                             {"url": "u", "body": b"", "hash": "h"}, {}, {"p": plugin})
        self.assertEqual(events[0]["payload"], canonical_json({"a": 1}))
        self.assertEqual(events[0]["topic"], "t")
        self.assertEqual(events[0]["hash"], sha256_hex(events[0]["payload"]))

    def test_run_adapter_transform_chain_and_drop(self):
        upper = SimpleNamespace(
            transform=lambda event, item: dict(event, topic=event["topic"].upper()))
        drop = SimpleNamespace(transform=lambda event, item: None)
        item = {"topic": "t", "transform": ["upper"]}
        events = run_adapter(item, {"url": "u", "body": b"x", "hash": "h"}, {},
                             {"upper": upper})
        self.assertEqual(events[0]["topic"], "T")
        item = {"topic": "t", "transform": ["upper", "drop"]}
        self.assertEqual(run_adapter(item, {"url": "u", "body": b"x", "hash": "h"}, {},
                                     {"upper": upper, "drop": drop}), [])

    def test_missing_transform_plugin_is_error(self):
        with self.assertRaises(AdapterError):
            run_adapter({"topic": "t", "transform": "ghost"},
                        {"url": "u", "body": b"x", "hash": "h"}, {}, {})


class AdapterScanIntegration(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.state = os.path.join(self.tmp.name, "scan.json")

    def tearDown(self):
        self.tmp.cleanup()

    def test_feed_scan_emits_only_new_entries(self):
        class Source:
            answers = [(200, {"ETag": '"v1"'}, RSS_V1), (200, {"ETag": '"v2"'}, RSS_V2)]

            def __call__(self, url, headers):
                return self.answers.pop(0)

        watcher = Watcher(self.state, fetcher=Source(), min_interval=0)
        watchlist = [{"url": "http://x.example/rss", "topic": "blog", "adapter": "rss"}]
        self.assertEqual(watcher.scan(watchlist), [],
                         "знакомство наполняет состояние, событий нет")
        events = watcher.scan(watchlist)
        self.assertEqual(sorted(e["topic"] for e in events),
                         ["blog/post-2", "blog/post-3"],
                         "старые записи уже видены; новая и правленная — события")

    def test_json_adapter_with_volatile_source_respects_suppression(self):
        class Source:
            answers = [(200, {}, b'{"tag_name": "v1"}'),
                       (200, {}, b'{"tag_name": "v2"}')]

            def __call__(self, url, headers):
                return self.answers.pop(0)

        watcher = Watcher(self.state, fetcher=Source(), min_interval=0)
        watchlist = [{"url": "http://api.example/latest", "topic": "app",
                      "adapter": "json", "json_path": "tag_name"}]
        watcher.scan(watchlist)
        events = watcher.scan(watchlist)
        self.assertEqual(events, [], "источник без валидаторов подавлен: нужен allow_volatile")
        self.assertEqual(watcher.last_suppressed[0]["url"], "http://api.example/latest")
