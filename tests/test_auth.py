"""Тесты токенов издателя: хеши, scope тем, 401 на защищённом хабе, открытый хаб по умолчанию."""
from __future__ import annotations

import json
import os
import tempfile
import threading
import unittest
import urllib.error
import urllib.parse
import urllib.request

from hubd.auth import TokenStore, generate, token_from_headers, token_hash
from hubd.hub import Hub, build_server


class TokenTestCase(unittest.TestCase):
    def test_generate_and_hash(self):
        token, digest = generate()
        self.assertEqual(digest, token_hash(token))
        self.assertEqual(len(token), 64)

    def test_load_rejects_plaintext_tokens(self):
        fd, path = tempfile.mkstemp(suffix=".json")
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump({"publish_tokens": ["open-secret"]}, fh)
        try:
            with self.assertRaises(ValueError):
                TokenStore.load(path)
        finally:
            os.unlink(path)

    def test_load_rejects_bad_hash(self):
        fd, path = tempfile.mkstemp(suffix=".json")
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump({"publish_tokens": [{"sha256": "xyz"}]}, fh)
        try:
            with self.assertRaises(ValueError):
                TokenStore.load(path)
        finally:
            os.unlink(path)

    def test_check_scope(self):
        store = TokenStore([{"sha256": token_hash("tok"), "topics": ["rfc/*"]},
                            {"sha256": token_hash("open"), "topics": []}])
        self.assertEqual(store.check("tok", "rfc/1"), (True, ""))
        ok, reason = store.check("tok", "museum/x")
        self.assertFalse(ok)
        self.assertIn("не даёт доступ", reason)
        self.assertEqual(store.check("open", "любая/тема"), (True, ""))
        ok, _ = store.check("nope", "rfc/1")
        self.assertFalse(ok)
        ok, reason = store.check("", "rfc/1")
        self.assertFalse(ok)
        self.assertIn("Bearer", reason)

    def test_token_from_headers(self):
        self.assertEqual(token_from_headers({"Authorization": "Bearer abc"}), "abc")
        self.assertEqual(token_from_headers({"X-Hub-Token": "abc"}), "abc")
        self.assertEqual(token_from_headers({}), "")


class HubAuthHTTPTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.hub = Hub(os.path.join(self.tmp.name, "hub.json"),
                       publish_auth=TokenStore([{"sha256": token_hash("good-token"),
                                                 "topics": ["app/*"]}]))
        self.server = build_server(self.hub, "127.0.0.1", 0)
        self.url = "http://127.0.0.1:%d" % self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def tearDown(self):
        self.server.shutdown()
        self.tmp.cleanup()

    def _publish(self, topic, headers=None):
        request = urllib.request.Request(
            "%s/publish?topic=%s" % (self.url, urllib.parse.quote(topic)),
            data=b"x", method="POST", headers=headers or {})
        try:
            with urllib.request.urlopen(request, timeout=5) as resp:
                return resp.status
        except urllib.error.HTTPError as exc:
            return exc.code

    def test_publish_without_token_is_401(self):
        self.assertEqual(self._publish("app/x"), 401)

    def test_publish_bearer_ok_and_scoped(self):
        self.assertEqual(self._publish("app/x", {"Authorization": "Bearer good-token"}), 200)
        self.assertEqual(self._publish("other/y", {"Authorization": "Bearer good-token"}), 401,
                         "токен со scope app/* не публикует в other/*")
        self.assertEqual(self._publish("app/x", {"X-Hub-Token": "good-token"}), 200)
        self.assertEqual(self._publish("app/x", {"Authorization": "Bearer nope"}), 401)

    def test_open_hub_still_open(self):
        hub = Hub(os.path.join(self.tmp.name, "open.json"))
        server = build_server(hub, "127.0.0.1", 0)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        try:
            request = urllib.request.Request(
                "http://127.0.0.1:%d/publish?topic=anything" % server.server_address[1],
                data=b"x", method="POST")
            with urllib.request.urlopen(request, timeout=5) as resp:
                self.assertEqual(resp.status, 200)
        finally:
            server.shutdown()
