import os
import tempfile
import unittest

from hubd.scan import Watcher, parse_rfc_index

NS = 'xmlns="https://www.rfc-editor.org/rfc-index"'


def index(rows):
    body = "".join(
        "<rfc-entry><doc-id>%s</doc-id><title>%s</title>"
        "<current-status>%s</current-status>"
        "<publication-status>%s</publication-status>"
        "<stream>%s</stream></rfc-entry>" % row for row in rows)
    return ("<rfc-index %s>%s</rfc-index>" % (NS, body)).encode()


URL = "https://example.test/rfc-index.xml"


class ProjectionCacheTest(unittest.TestCase):
    def _watcher(self, tmp, body, changed=None):
        payload = {"body": body}
        if changed is not None:
            payload["body"] = changed

        def fetcher(url, headers):
            return 200, {}, payload["body"]

        return Watcher(os.path.join(tmp, "scan.json"), fetcher=fetcher, min_interval=0), payload

    def test_cache_keeps_only_cheap_fields(self):
        body = index([("RFC1347", "TCP and UDP", "PROPOSED STANDARD", "PROPOSED STANDARD", "Legacy")])
        with tempfile.TemporaryDirectory() as tmp:
            watcher, _ = self._watcher(tmp, body)
            watcher.write_projection(URL, parse_rfc_index(body))
            cached = watcher.read_projection(URL)
            self.assertEqual(cached["RFC1347"]["current-status"], "PROPOSED STANDARD")
            self.assertNotIn("title", cached["RFC1347"], "title в 7 раз дороже: в кэше его нет")
            self.assertNotIn("doc-id", cached["RFC1347"], "doc-id и есть ключ, дублировать незачем")

    def test_change_detected_from_cache_without_previous_body(self):
        before = index([("RFC1347", "TCP and UDP", "PROPOSED STANDARD", "PROPOSED STANDARD", "Legacy")])
        after = index([("RFC1347", "TCP and UDP", "HISTORIC", "PROPOSED STANDARD", "Legacy")])
        with tempfile.TemporaryDirectory() as tmp:
            watcher, payload = self._watcher(tmp, before)
            watcher.write_projection(URL, parse_rfc_index(before))
            watcher.state[URL] = {"hash": "старый-отпечаток", "checks": 1, "changes": 0}
            payload["body"] = after
            result = watcher.check(URL, store_body=False, projection="rfc-index-status")
            self.assertTrue(result["changed"])
            self.assertIn("-RFC1347  current-status: PROPOSED STANDARD", result["diff"])
            self.assertIn("+RFC1347  current-status: HISTORIC", result["diff"])

    def test_title_only_change_still_reaches_triage(self):
        before = index([("RFC1", "Old Title", "PROPOSED STANDARD", "PROPOSED STANDARD", "Legacy")])
        after = index([("RFC1", "New Title", "PROPOSED STANDARD", "PROPOSED STANDARD", "Legacy")])
        with tempfile.TemporaryDirectory() as tmp:
            watcher, payload = self._watcher(tmp, before)
            watcher.write_body(URL, before)
            watcher.write_projection(URL, parse_rfc_index(before))
            watcher.state[URL] = {"hash": "старый-отпечаток", "checks": 1, "changes": 0}
            payload["body"] = after
            result = watcher.check(URL, store_body=True, projection="rfc-index-status")
            self.assertTrue(result["changed"])
            self.assertIn("New Title", result["diff"],
                          "смену заголовка ловит фолбэк по телу: в кэше title не хранится")


if __name__ == "__main__":
    unittest.main()
