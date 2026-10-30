"""Тесты подписчика: challenge эхом, hub.mode=denied, доставки с проверкой подписи."""
from __future__ import annotations

import json
import os
import tempfile
import threading
import unittest
import urllib.parse
import urllib.request
from http.server import ThreadingHTTPServer

from hubd.hub import sign
from hubd.listen import Inbox, Subscriber, parse_secrets, secret_for, signature_valid


class ListenTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.inbox_path = os.path.join(self.tmp.name, "inbox.jsonl")
        self.inbox = Inbox(self.inbox_path)
        self.subscriber = Subscriber(self.inbox, [("rfc/*", "s3cret")])
        self.server = self.subscriber.build_server("127.0.0.1", 0)
        self.url = "http://127.0.0.1:%d" % self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def tearDown(self):
        self.server.shutdown()
        self.tmp.cleanup()

    def _get(self, query):
        with urllib.request.urlopen(self.url + "/cb?" + urllib.parse.urlencode(query),
                                    timeout=5) as resp:
            return resp.status, resp.read()

    def _post(self, body, headers=None):
        request = urllib.request.Request(self.url + "/cb", data=body, method="POST",
                                         headers=headers or {})
        with urllib.request.urlopen(request, timeout=5) as resp:
            return resp.status

    def test_challenge_echoed_and_logged(self):
        status, body = self._get({"hub.mode": "subscribe", "hub.topic": "rfc/1",
                                  "hub.challenge": "ch4ll3ng3"})
        self.assertEqual((status, body), (200, b"ch4ll3ng3"))
        self.assertEqual(self.inbox.all()[-1]["kind"], "challenge")

    def test_denied_logged_with_reason(self):
        self._get({"hub.mode": "denied", "hub.topic": "rfc/1",
                   "hub.reason": "не по политике"})
        row = self.inbox.all()[-1]
        self.assertEqual(row["kind"], "denied")
        self.assertEqual(row["reason"], "не по политике")

    def test_delivery_signature_checked(self):
        body = b"payload"
        self._post(body, {"X-Hub-Topic": "rfc/1", "X-Hub-Signature": sign("s3cret", body)})
        row = self.inbox.all()[-1]
        self.assertEqual(row["kind"], "delivery")
        self.assertTrue(row["signature_valid"])
        self.assertEqual(row["body"], "payload")

        self._post(body, {"X-Hub-Topic": "rfc/1", "X-Hub-Signature": "sha256=bad"})
        self.assertFalse(self.inbox.all()[-1]["signature_valid"],
                         "неверная подпись не выдаётся за верную")

    def test_delivery_without_secret_saved_unverified(self):
        self._post(b"x", {"X-Hub-Topic": "unrelated/1"})
        row = self.inbox.all()[-1]
        self.assertIsNone(row["signature_valid"])

    def test_inbox_file_written(self):
        self._get({"hub.challenge": "c"})
        with open(self.inbox_path, encoding="utf-8") as fh:
            rows = [json.loads(line) for line in fh]
        self.assertTrue(rows)

    def test_signature_valid_unit(self):
        self.assertTrue(signature_valid("s", b"b", sign("s", b"b")))
        self.assertFalse(signature_valid("s", b"b", "sha256=00"))

    def test_parse_secrets(self):
        self.assertEqual(parse_secrets(["rfc/*=s3cret"]), [("rfc/*", "s3cret")])
        with self.assertRaises(ValueError):
            parse_secrets(["no-equals"])
        path = os.path.join(self.tmp.name, "secrets.json")
        with open(path, "w", encoding="utf-8") as fh:
            json.dump({"secrets": {"museum/*": "m"}}, fh)
        self.assertEqual(parse_secrets(file_path=path), [("museum/*", "m")])

    def test_secret_for_first_match_wins(self):
        secrets = [("rfc/868", "a"), ("rfc/*", "b")]
        self.assertEqual(secret_for(secrets, "rfc/868"), "a")
        self.assertEqual(secret_for(secrets, "rfc/3170"), "b")
        self.assertEqual(secret_for(secrets, "other/1"), "")
