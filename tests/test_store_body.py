"""Дифф реестра и дочитывание тела по Range.

Два требования от триажа музея (Архивесса):
  1. повтор на большом теле не должен стоить трёх полных копий 13,8 МБ — при обрыве
     остаток дочитывается по `Range: bytes=<прочитано>-`, если сервер отдаёт 206;
     сервер, который Range игнорирует (rfc-editor), присылает тело заново — берём его;
  2. кандидат от реестра должен нести дифф («строка X: Internet Standard -> Historic»),
     а не первые 300 байт XML-шапки, по которым ничего не видно.
"""
from __future__ import annotations

import http.client
import os
import tempfile
import unittest
from unittest import mock

from hubd.scan import Watcher, default_fetcher


class _Response:
    def __init__(self, body: bytes, status: int = 200, headers: dict | None = None):
        self._body = body
        self.status = status
        self.headers = headers or {}

    def read(self) -> bytes:
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False


class RangeResumeTest(unittest.TestCase):
    def test_206_remainder_is_glued_to_partial(self):
        first = http.client.IncompleteRead(b"abc", 6)
        with mock.patch("hubd.scan.urllib.request.urlopen",
                        side_effect=[first, _Response(b"def", status=206)]):
            with mock.patch("hubd.scan.time.sleep"):
                status, _, body = default_fetcher("https://example.test/big", {})
        self.assertEqual(status, 200)
        self.assertEqual(body, b"abcdef", "остаток обязан приклеиться, а не заменить прочитанное")

    def test_server_ignoring_range_gives_full_body_once(self):
        first = http.client.IncompleteRead(b"abc", 6)
        with mock.patch("hubd.scan.urllib.request.urlopen",
                        side_effect=[first, _Response(b"abcdef", status=200)]):
            with mock.patch("hubd.scan.time.sleep"):
                status, _, body = default_fetcher("https://example.test/big", {})
        self.assertEqual(status, 200)
        self.assertEqual(body, b"abcdef", "сервер без Range отдаёт всё тело: склейка запрещена")

    def test_range_header_is_sent_on_retry(self):
        seen = []

        def fake(request, timeout=None):
            seen.append(dict(request.header_items()))
            if len(seen) == 1:
                raise http.client.IncompleteRead(b"abc", 6)
            return _Response(b"def", status=206)

        with mock.patch("hubd.scan.urllib.request.urlopen", side_effect=fake):
            with mock.patch("hubd.scan.time.sleep"):
                default_fetcher("https://example.test/big", {})
        first = {k.lower(): v for k, v in seen[0].items()}
        second = {k.lower(): v for k, v in seen[1].items()}
        self.assertNotIn("range", first, "первый запрос — без Range")
        self.assertEqual(second.get("range"), "bytes=3-", "повтор докачивает с прочитанного")


class StoreBodyDiffTest(unittest.TestCase):
    V1 = b"""<registry>
<record><name>rfc1</name><status>Internet Standard</status></record>
<record><name>rfc2</name><status>Proposed Standard</status></record>
</registry>"""
    V2 = b"""<registry>
<record><name>rfc1</name><status>Internet Standard</status></record>
<record><name>rfc2</name><status>Historic</status></record>
</registry>"""

    def _watcher(self, directory: str, bodies: list[bytes]) -> Watcher:
        queue = list(bodies)

        def fetcher(url, headers):
            body = queue.pop(0) if queue else bodies[-1]
            return 200, {}, body

        return Watcher(os.path.join(directory, "scan.json"), fetcher=fetcher, min_interval=0)

    def test_diff_shows_status_change_instead_of_xml_header(self):
        with tempfile.TemporaryDirectory() as tmp:
            watcher = self._watcher(tmp, [self.V1, self.V2])
            first = watcher.check("https://example.test/registry.xml", store_body=True)
            self.assertTrue(first["first_seen"])
            self.assertEqual(first["diff"], "", "первое знакомство — диффать не с чем")

            second = watcher.check("https://example.test/registry.xml", store_body=True)
            self.assertTrue(second["changed"])
            self.assertIn("Historic", second["diff"])
            self.assertIn("Proposed Standard", second["diff"])
            self.assertIn("-", second["diff"])

    def test_body_is_gzipped_next_to_state(self):
        with tempfile.TemporaryDirectory() as tmp:
            watcher = self._watcher(tmp, [self.V1])
            watcher.check("https://example.test/registry.xml", store_body=True)
            bodies = os.path.join(tmp, "bodies")
            self.assertTrue(os.path.isdir(bodies))
            stored = os.listdir(bodies)
            self.assertEqual(len(stored), 1)
            self.assertTrue(stored[0].endswith(".body.gz"))
            self.assertEqual(watcher.read_body("https://example.test/registry.xml"), self.V1)

    def test_no_body_stored_without_flag(self):
        with tempfile.TemporaryDirectory() as tmp:
            watcher = self._watcher(tmp, [self.V1, self.V2])
            watcher.check("https://example.test/registry.xml")
            second = watcher.check("https://example.test/registry.xml")
            self.assertTrue(second["changed"])
            self.assertEqual(second["diff"], "")
            self.assertFalse(os.path.isdir(os.path.join(tmp, "bodies")))


if __name__ == "__main__":
    unittest.main()
