"""Тесты сборки списка наблюдения из архива музея (музей только читаем)."""
from __future__ import annotations

import json
import os
import tempfile
import unittest

from hubd.scan import load_watchlist
from hubd.watchlist import from_museum, write

BASE = {
    "slug": "rfc-graveyard",
    "entries": [
        {
            "id": "ipmulticast",
            "name": "IP multicast и MBone",
            "src": [
                {"u": "https://www.rfc-editor.org/rfc/rfc3170.txt", "r": 1},
                {"u": "https://en.wikipedia.org/wiki/IP_multicast", "r": 0},
                {"u": "https://www.rfc-editor.org/rfc/rfc1112.txt", "r": 0},
            ],
        },
        {
            "id": "set",
            "name": "SET",
            "src": [
                {"u": "http://exelana.com/set/spec100/set_bk1.pdf", "r": 1},
                {"u": "https://en.wikipedia.org/wiki/Secure_Electronic_Transaction", "r": 0},
            ],
        },
        {
            "id": "uddi",
            "name": "UDDI",
            "src": [
                {"u": "https://www.oasis-open.org/committees/uddi-spec/doc/spec/v3/uddi.htm", "r": 1},
            ],
        },
    ],
}
EXTRA = {
    "slug": "second-digest",
    "entries": [
        {
            "id": "time868",
            "name": "Time Protocol",
            # тот же RFC, что и в первом датасете: адрес не должен удвоиться
            "src": [{"u": "https://datatracker.ietf.org/doc/rfc3170/", "r": 1}],
        }
    ],
}


class WatchlistTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.museum = os.path.join(self.tmp.name, "lost-tech-museum")
        os.makedirs(os.path.join(self.museum, "data"))
        with open(os.path.join(self.museum, "data", "rfc-graveyard.json"), "w", encoding="utf-8") as fh:
            json.dump(BASE, fh, ensure_ascii=False)
        with open(os.path.join(self.museum, "data", "second-digest.json"), "w", encoding="utf-8") as fh:
            json.dump(EXTRA, fh, ensure_ascii=False)

    def tearDown(self):
        self.tmp.cleanup()

    def build(self, **kwargs):
        return from_museum(self.museum, **kwargs)

    def test_rfc_entries_become_status_and_changes_sources(self):
        sources, _skipped = self.build()
        by_url = {source["url"]: source for source in sources}
        self.assertIn("https://datatracker.ietf.org/api/v1/doc/document/rfc3170/", by_url)
        self.assertIn("https://datatracker.ietf.org/feed/document-changes/rfc3170/", by_url)
        self.assertEqual(by_url["https://datatracker.ietf.org/api/v1/doc/document/rfc3170/"]["topic"],
                         "rfc/rfc3170/status")
        self.assertEqual(by_url["https://datatracker.ietf.org/feed/document-changes/rfc3170/"]["topic"],
                         "rfc/rfc3170/changes")

    def test_provenance_points_at_archive_entries(self):
        sources, _ = self.build()
        status = next(s for s in sources
                      if s["url"] == "https://datatracker.ietf.org/api/v1/doc/document/rfc3170/")
        self.assertEqual(status["museum"], ["ipmulticast", "time868"], "провенанс от обеих записей")
        self.assertIn("ipmulticast", status["note"])

    def test_same_url_is_not_duplicated(self):
        sources, _ = self.build()
        urls = [source["url"] for source in sources]
        self.assertEqual(len(urls), len(set(urls)))

    def test_non_archival_hosts_are_skipped_with_reason(self):
        sources, skipped = self.build()
        urls = {source["url"] for source in sources}
        self.assertNotIn("http://exelana.com/set/spec100/set_bk1.pdf", urls)
        reasons = {row["url"]: row["why"] for row in skipped}
        self.assertIn("http://exelana.com/set/spec100/set_bk1.pdf", reasons)
        self.assertIn("--all-hosts", reasons["http://exelana.com/set/spec100/set_bk1.pdf"])
        self.assertNotIn("https://en.wikipedia.org/wiki/IP_multicast", urls,
                         "источники с r=0 в список не попадают вообще")

    def test_all_hosts_includes_non_archival(self):
        sources, skipped = self.build(include_all_hosts=True)
        urls = {source["url"] for source in sources}
        self.assertIn("http://exelana.com/set/spec100/set_bk1.pdf", urls)
        self.assertEqual(skipped, [])

    def test_archival_non_rfc_source_keeps_its_topic(self):
        sources, _ = self.build()
        oasis = next(s for s in sources if "oasis-open.org" in s["url"])
        self.assertEqual(oasis["topic"], "rfc-graveyard/uddi")
        self.assertEqual(oasis["museum"], ["uddi"])

    def test_slug_filter_limits_datasets(self):
        sources, _ = self.build(slugs=["second-digest"])
        urls = {source["url"] for source in sources}
        self.assertIn("https://datatracker.ietf.org/api/v1/doc/document/rfc3170/", urls)
        self.assertFalse(any("oasis-open.org" in url for url in urls), "датасет не выбран — его записей нет")
        self.assertFalse(any("rfc1112" in url for url in urls))

    def test_museum_pages_and_feeds_are_watched(self):
        sources, _ = self.build()
        topics = {source["topic"] for source in sources}
        self.assertIn("museum/site", topics)
        self.assertIn("museum/digest/rfc-graveyard", topics)
        self.assertIn("ietf/rfc-feed", topics)
        self.assertIn("arxiv/obsolete-protocols", topics)
        arxiv = next(s for s in sources if s["topic"] == "arxiv/obsolete-protocols")
        self.assertTrue(arxiv["allow_volatile"], "у arXiv API нет ETag — это осознанное решение")

    def test_every_source_has_url_topic_note(self):
        sources, _ = self.build()
        for source in sources:
            self.assertTrue(source["url"].startswith("http"))
            self.assertTrue(source["topic"])
            self.assertTrue(source["note"])

    def test_written_file_is_readable_by_scanner(self):
        sources, _ = self.build()
        out = os.path.join(self.tmp.name, "watchlist.json")
        write(out, sources, self.museum)
        loaded = load_watchlist(out)
        self.assertEqual(len(loaded), len(sources))
        self.assertEqual({item["url"] for item in loaded}, {item["url"] for item in sources})
        with open(out, encoding="utf-8") as fh:
            payload = json.load(fh)
        self.assertIn("Сгенерировано", payload["comment"])

    def test_missing_museum_directory_raises(self):
        with self.assertRaises(FileNotFoundError):
            from_museum(os.path.join(self.tmp.name, "нет-такого-музея"))


if __name__ == "__main__":
    unittest.main()
