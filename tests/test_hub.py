"""Тесты хаба: рукопожатие подписки, доставка с подписью, аренда, честный отказ.

Всё на loopback (127.0.0.1), без сети наружу и без зависимостей: только stdlib.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import tempfile
import threading
import unittest
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from hubd.hub import Hub, build_server


class Clock:
    """Управляемые часы: тесты двигают время сами, а не ждут."""

    def __init__(self, start: float = 1_000_000.0):
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class CallbackServer:
    """Локальный подписчик: подтверждает challenge и записывает доставки."""

    def __init__(self, echo_challenge: bool = True, fail_deliveries: bool = False):
        self.records: list[tuple] = []
        outer = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, format, *args):
                pass

            def do_GET(self):
                query = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
                outer.records.append(("GET", query))
                challenge = (query.get("hub.challenge") or [""])[0]
                body = (challenge if echo_challenge else "не тот challenge").encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_POST(self):
                length = int(self.headers.get("Content-Length", "0") or 0)
                payload = self.rfile.read(length)
                outer.records.append(("POST", dict(self.headers), payload))
                code = 500 if fail_deliveries else 204
                self.send_response(code)
                self.send_header("Content-Length", "0")
                self.end_headers()

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    @property
    def url(self) -> str:
        return "http://127.0.0.1:%d/cb" % self.server.server_address[1]

    @property
    def posts(self) -> list[tuple]:
        return [r for r in self.records if r[0] == "POST"]

    def stop(self) -> None:
        self.server.shutdown()
        self.server.server_close()


def post_form(url: str, form: dict) -> tuple[int, dict]:
    data = urllib.parse.urlencode(form).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST",
                                 headers={"Content-Type": "application/x-www-form-urlencoded"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("utf-8") or "{}")


class HubTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.clock = Clock()
        self.hub = Hub(os.path.join(self.tmp.name, "hub.json"), clock=self.clock, timeout=5)
        self.server = build_server(self.hub, "127.0.0.1", 0)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.base = "http://127.0.0.1:%d" % self.server.server_address[1]
        self.callbacks: list[CallbackServer] = []

    def tearDown(self):
        for cb in self.callbacks:
            cb.stop()
        self.server.shutdown()
        self.server.server_close()
        self.tmp.cleanup()

    def callback(self, **kwargs) -> CallbackServer:
        cb = CallbackServer(**kwargs)
        self.callbacks.append(cb)
        return cb

    def subscribe(self, cb: CallbackServer, topic="museum/rfc", lease=3600, secret=None):
        form = {"hub.mode": "subscribe", "hub.topic": topic, "hub.callback": cb.url,
                "hub.lease_seconds": lease}
        if secret:
            form["hub.secret"] = secret
        return post_form(self.base + "/subscribe", form)

    def publish(self, topic="museum/rfc", body=b"payload"):
        url = "%s/publish?topic=%s" % (self.base, urllib.parse.quote(topic, safe=""))
        req = urllib.request.Request(url, data=body, method="POST",
                                     headers={"Content-Type": "application/octet-stream"})
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                return resp.status, json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read().decode("utf-8") or "{}")

    # --- подписка ------------------------------------------------------------
    def test_subscribe_handshake_echoes_challenge(self):
        cb = self.callback()
        code, payload = self.subscribe(cb, secret="s3cret")
        self.assertEqual(code, 202, payload)
        self.assertEqual(payload["topic"], "museum/rfc")
        self.assertEqual(len(cb.records), 1)
        self.assertEqual(cb.records[0][0], "GET")
        self.assertIn("hub.challenge", cb.records[0][1])
        subs = self.hub.store.as_list()
        self.assertEqual(len(subs), 1)
        self.assertNotIn("secret", subs[0], "секрет не должен уходить наружу")

    def test_subscribe_rejected_when_challenge_not_confirmed(self):
        cb = self.callback(echo_challenge=False)
        code, payload = self.subscribe(cb)
        self.assertEqual(code, 400)
        self.assertIn("challenge", payload["error"])
        self.assertEqual(self.hub.store.as_list(), [])

    def test_subscribe_rejected_when_callback_unreachable(self):
        code, payload = post_form(self.base + "/subscribe", {
            "hub.mode": "subscribe", "hub.topic": "museum/rfc",
            "hub.callback": "http://127.0.0.1:9/nope", "hub.lease_seconds": 600})
        self.assertEqual(code, 400)
        self.assertTrue(payload["error"])

    def test_lease_is_clamped_and_renewable(self):
        cb = self.callback()
        _, payload = self.subscribe(cb, lease=5)
        self.assertEqual(payload["lease_seconds"], 60, "аренда короче минуты не выдаётся")
        self.subscribe(cb, lease=7200)
        self.assertEqual(payload["lease_seconds"], 60)

    # --- доставка ------------------------------------------------------------
    def test_publish_fans_out_with_signature(self):
        first = self.callback()
        second = self.callback()
        self.subscribe(first, secret="topsecret")
        self.subscribe(second)
        body = b"RFC 3170: deployment is slow"
        code, report = self.publish(body=body)
        self.assertEqual(code, 200, report)
        self.assertEqual(len(report["delivered"]), 2)
        self.assertEqual(len(first.posts), 1)
        self.assertEqual(first.posts[0][2], body)
        signature = first.posts[0][1].get("X-Hub-Signature")
        expected = "sha256=" + hmac.new(b"topsecret", body, hashlib.sha256).hexdigest()
        self.assertEqual(signature, expected)
        self.assertEqual(first.posts[0][1].get("X-Hub-Topic"), "museum/rfc")
        self.assertIsNone(second.posts[0][1].get("X-Hub-Signature"),
                          "без секрета подпись не выдумывается")
        self.assertEqual(self.hub.store.for_topic("museum/rfc")[0]["deliveries"], 1)

    def test_publish_without_subscribers_is_not_an_error(self):
        code, report = self.publish()
        self.assertEqual(code, 200)
        self.assertEqual(report["delivered"], [])
        self.assertIn("нет активных подписчиков", report["note"])

    def test_lease_expiry_drops_subscriber(self):
        cb = self.callback()
        self.subscribe(cb, lease=60)
        self.clock.advance(61)
        code, report = self.publish(body=b"after expiry")
        self.assertEqual(report["delivered"], [])
        self.assertEqual(cb.posts, [], "просроченной аренде ничего не доставляется")
        self.assertEqual(self.hub.store.as_list(), [])

    def test_degraded_subscriber_gets_honest_refusal(self):
        cb = self.callback(fail_deliveries=True)
        self.subscribe(cb)
        for _ in range(3):
            code, report = self.publish(body=b"attempt")
            self.assertEqual(code, 502, "пока контракт не выполнен — честная ошибка доставки")
        self.assertTrue(self.hub.store.as_list()[0]["degraded"])
        code, report = self.publish(body=b"refused")
        self.assertEqual(code, 503)
        self.assertEqual(len(report["refused"]), 1)
        self.assertTrue(report["delivered"] == [] and report["failed"] == [])
        self.assertIn("контракт нарушен", report["refused"][0]["reason"])

    def test_unsubscribe_removes_subscription(self):
        cb = self.callback()
        self.subscribe(cb)
        code, payload = post_form(self.base + "/unsubscribe",
                                  {"hub.mode": "unsubscribe", "hub.topic": "museum/rfc", "hub.callback": cb.url})
        self.assertEqual(code, 202, payload)
        self.assertEqual(self.hub.store.as_list(), [])
        code, payload = post_form(self.base + "/unsubscribe",
                                  {"hub.mode": "unsubscribe", "hub.topic": "museum/rfc", "hub.callback": cb.url})
        self.assertEqual(code, 404)

    def test_healthz_and_subscriptions_endpoints(self):
        cb = self.callback()
        self.subscribe(cb)
        with urllib.request.urlopen(self.base + "/healthz", timeout=5) as resp:
            self.assertTrue(json.loads(resp.read().decode())["ok"])
        with urllib.request.urlopen(self.base + "/subscriptions", timeout=5) as resp:
            data = json.loads(resp.read().decode())
        self.assertEqual(len(data["subscriptions"]), 1)
        self.assertEqual(data["subscriptions"][0]["topic"], "museum/rfc")


if __name__ == "__main__":
    unittest.main()
