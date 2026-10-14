"""Тесты хаба: рукопожатие подписки, доставка с подписью, аренда, очередь повторов, честный отказ.

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
from hubd.store import backoff_seconds


class Clock:
    """Управляемые часы: тесты двигают время сами, а не ждут."""

    def __init__(self, start: float = 1_000_000.0):
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class CallbackServer:
    """Локальный подписчик: подтверждает challenge и записывает доставки.

    fail — отвечать на доставку 500 (переключается на лету: подписчик «чинится»);
    verification_status — каким кодом ответить на проверку намерения (404 = «не хочу»).
    """

    def __init__(self, echo_challenge: bool = True, fail_deliveries: bool = False,
                 verification_status: int = 200):
        self.records: list[tuple] = []
        self.fail = fail_deliveries
        self.echo_challenge = echo_challenge
        self.verification_status = verification_status
        outer = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, format, *args):
                pass

            def _reply(self, code: int, body: bytes = b"") -> None:
                self.send_response(code)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                if body:
                    self.wfile.write(body)

            def do_GET(self):
                query = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
                outer.records.append(("GET", query))
                if outer.verification_status != 200:
                    self._reply(outer.verification_status, b"no")
                    return
                challenge = (query.get("hub.challenge") or [""])[0]
                body = (challenge if outer.echo_challenge else "не тот challenge").encode("utf-8")
                self._reply(200, body)

            def do_POST(self):
                length = int(self.headers.get("Content-Length", "0") or 0)
                payload = self.rfile.read(length)
                outer.records.append(("POST", dict(self.headers), payload))
                self._reply(500 if outer.fail else 204)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    @property
    def url(self) -> str:
        return "http://127.0.0.1:%d/cb" % self.server.server_address[1]

    @property
    def posts(self) -> list[tuple]:
        return [r for r in self.records if r[0] == "POST"]

    @property
    def gets(self) -> list[tuple]:
        return [r for r in self.records if r[0] == "GET"]

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
    MAX_ATTEMPTS = 8
    TOPIC_POLICY = None

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.clock = Clock()
        self.store_path = os.path.join(self.tmp.name, "hub.json")
        self.hub = Hub(self.store_path, clock=self.clock, timeout=5,
                       max_attempts=self.MAX_ATTEMPTS, topic_policy=self.TOPIC_POLICY)
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

    def subscribe(self, cb: CallbackServer, topic="museum/rfc", lease=3600, secret=None,
                  mode="subscribe"):
        form = {"hub.mode": mode, "hub.topic": topic, "hub.callback": cb.url,
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

    def retry(self) -> dict:
        """Проходка по очереди — так же, как её дёргает служба или POST /retry."""
        return self.hub.retry()

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

    def test_subscriber_can_refuse_subscription(self):
        """WebSub §5.3.1: 404 на проверку намерения — подписчик не согласен."""
        cb = self.callback(verification_status=404)
        code, payload = self.subscribe(cb)
        self.assertEqual(code, 400)
        self.assertIn("отказался", payload["error"])
        self.assertEqual(self.hub.store.as_list(), [], "отказ подписчика — подписки нет")

    def test_subscribe_with_mode_denied_is_rejected(self):
        """hub.mode=denied — сообщение хаба подписчику, а не запрос от него."""
        cb = self.callback()
        code, payload = self.subscribe(cb, mode="denied")
        self.assertEqual(code, 400)
        self.assertIn("subscribe", payload["error"])
        self.assertEqual(cb.records, [], "хаб не должен даже проверять намерение по такому запросу")
        self.assertEqual(self.hub.store.as_list(), [])

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
        self.assertEqual(self.hub.store.for_topic("museum/rfc")[0]["deliveries"], 1)

    def test_delivery_without_secret_has_no_signature(self):
        """Подписка без hub.secret: подпись не выдумывается, хаб её не ставит."""
        cb = self.callback()
        self.subscribe(cb)
        body = "доставка без секрета".encode("utf-8")
        code, report = self.publish(body=body)
        self.assertEqual(code, 200, report)
        self.assertEqual(len(cb.posts), 1)
        self.assertIsNone(cb.posts[0][1].get("X-Hub-Signature"))
        self.assertEqual(cb.posts[0][2], body)
        self.assertEqual(report["unconfirmed"], 0)

    def test_partial_delivery_is_not_reported_as_success(self):
        """Одному доставили, другому нет: 200 значил бы «доставлено всем» — это было бы враньё."""
        good = self.callback()
        bad = self.callback(fail_deliveries=True)
        self.subscribe(good)
        self.subscribe(bad)
        code, report = self.publish(body=b"payload")
        self.assertEqual(code, 202, report)
        self.assertFalse(report["confirmed"])
        self.assertEqual(len(report["delivered"]), 1)
        self.assertEqual(len(report["queued"]), 1)
        self.assertEqual(report["unconfirmed"], 1)
        self.assertEqual(len(good.posts), 1)

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

    # --- очередь повторов ----------------------------------------------------
    def test_failed_delivery_is_queued_with_backoff(self):
        cb = self.callback(fail_deliveries=True)
        self.subscribe(cb)
        code, report = self.publish(body=b"attempt")
        self.assertEqual(code, 202, "принято, но не подтверждено — не 200 и не отказ")
        self.assertEqual(len(report["queued"]), 1)
        self.assertEqual(report["queued"][0]["attempts"], 0)
        self.assertEqual(report["queued"][0]["next_in"], int(backoff_seconds(0)))
        self.assertEqual(report["unconfirmed"], 1)
        self.assertEqual(self.hub.store.unconfirmed(), 1)

        report = self.retry()
        self.assertEqual(report["attempted"], 0, "до истечения паузы повтор не делаем")

    def test_retry_delivers_after_backoff_and_clears_queue(self):
        cb = self.callback(fail_deliveries=True)
        self.subscribe(cb)
        _, report = self.publish(body=b"attempt")
        self.clock.advance(report["queued"][0]["next_in"] + 1)
        cb.fail = False                                  # подписчик починился
        outcome = self.retry()
        self.assertEqual(outcome["attempted"], 1)
        self.assertEqual(len(outcome["delivered"]), 1)
        self.assertEqual(outcome["unconfirmed"], 0)
        self.assertEqual(cb.posts[-1][2], b"attempt", "повтор доставляет то же тело")
        self.assertEqual(self.hub.store.unconfirmed(), 0)
        self.assertEqual(self.hub.store.as_list()[0]["failures"], 0)

    def test_retry_waits_longer_after_each_failure(self):
        cb = self.callback(fail_deliveries=True)
        self.subscribe(cb)
        _, report = self.publish(body=b"attempt")
        first_wait = report["queued"][0]["next_in"]
        self.clock.advance(first_wait + 1)
        outcome = self.retry()
        self.assertEqual(len(outcome["still_pending"]), 1)
        self.assertGreater(outcome["still_pending"][0]["next_in"], first_wait,
                           "backoff растёт: каждая следующая пауза длиннее")
        self.assertEqual(outcome["still_pending"][0]["attempts"], 1)

    def test_attempts_exhausted_mark_degraded_and_refuse(self):
        cb = self.callback(fail_deliveries=True)
        self.subscribe(cb)
        _code, report = self.publish(body=b"attempt")
        self.assertEqual(len(report["queued"]), 1)
        wait = report["queued"][0]["next_in"]
        outcome = {}
        for _ in range(self.MAX_ATTEMPTS + 1):
            self.clock.advance(wait + 1)
            outcome = self.retry()
            if outcome["gave_up"]:
                break
            self.assertEqual(len(outcome["still_pending"]), 1, outcome)
            wait = outcome["still_pending"][0]["next_in"]
        self.assertTrue(outcome.get("gave_up"), "после потолка повторов хаб сдаётся")
        self.assertIn("контракт нарушен", outcome["gave_up"][0]["reason"])
        self.assertTrue(self.hub.store.as_list()[0]["degraded"])
        self.assertEqual(self.hub.store.unconfirmed(), 0, "отказавшись, хаб не держит очередь вечно")

        code, report = self.publish(body=b"refused")
        self.assertEqual(code, 503)
        self.assertEqual(len(report["refused"]), 1)
        self.assertEqual(report["delivered"], [])
        self.assertIn("контракт нарушен", report["refused"][0]["reason"])

    def test_oversized_payload_is_not_queued(self):
        cb = self.callback(fail_deliveries=True)
        self.subscribe(cb)
        self.hub.store.pending_max_bytes = 8
        code, report = self.publish(body=b"0123456789")
        self.assertEqual(code, 502, "доставить не смогли и повторить не сможем — об этом честно")
        self.assertEqual(len(report["not_queued"]), 1)
        self.assertIn("больше очереди", report["not_queued"][0]["reason"])
        self.assertEqual(self.hub.store.unconfirmed(), 0)

    def test_pending_queue_survives_restart(self):
        cb = self.callback(fail_deliveries=True)
        self.subscribe(cb)
        self.publish(body=b"attempt")
        again = Hub(self.store_path, clock=self.clock, timeout=5)
        self.assertEqual(again.store.unconfirmed(), 1, "очередь на диске, а не в памяти")
        self.assertEqual(len(again.store.pending_rows()), 1)
        self.assertEqual(again.store.body_of(next(iter(again.store.pending.values()))), b"attempt")

    def test_lease_expiry_clears_the_pending_queue(self):
        cb = self.callback(fail_deliveries=True)
        self.subscribe(cb, lease=60)
        _, report = self.publish(body=b"attempt")
        self.assertEqual(report["unconfirmed"], 1)
        self.clock.advance(61)
        outcome = self.retry()
        self.assertEqual(outcome["unconfirmed"], 0, "истёкшую аренду не повторяем")
        self.assertEqual(outcome["attempted"], 0, "и не стучимся к тому, чья аренда кончилась")
        self.assertEqual(self.hub.store.as_list(), [])
        self.assertEqual(len(cb.posts), 1, "после истечения аренды доставок больше нет")

    def test_pending_for_vanished_subscription_is_dropped(self):
        """Защита от расхождения: запись в очереди есть, подписки под ней уже нет."""
        cb = self.callback()
        self.subscribe(cb)
        self.hub.store.pending["manual"] = {
            "key": "manual", "sub": "нет-такой-подписки", "topic": "museum/rfc",
            "body": "eA==", "content_type": "text/plain", "bytes": 1, "attempts": 0,
            "last_error": "", "created": self.clock.now, "next": self.clock.now - 1,
        }
        outcome = self.retry()
        self.assertEqual(len(outcome["dropped"]), 1)
        self.assertEqual(outcome["unconfirmed"], 0)
        self.assertEqual(cb.posts, [], "несуществующей подписке ничего не доставляем")

    def test_unsubscribe_drops_pending_deliveries(self):
        cb = self.callback(fail_deliveries=True)
        self.subscribe(cb)
        self.publish(body=b"attempt")
        self.assertEqual(self.hub.store.unconfirmed(), 1)
        code, _ = post_form(self.base + "/unsubscribe",
                            {"hub.mode": "unsubscribe", "hub.topic": "museum/rfc",
                             "hub.callback": cb.url})
        self.assertEqual(code, 202)
        self.assertEqual(self.hub.store.unconfirmed(), 0)

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
        cb = self.callback(fail_deliveries=True)
        self.subscribe(cb)
        self.publish(body=b"attempt")
        with urllib.request.urlopen(self.base + "/healthz", timeout=5) as resp:
            health = json.loads(resp.read().decode())
        self.assertTrue(health["ok"])
        self.assertEqual(health["unconfirmed"], 1, "здоровье хаба включает очередь")
        with urllib.request.urlopen(self.base + "/subscriptions", timeout=5) as resp:
            data = json.loads(resp.read().decode())
        self.assertEqual(len(data["subscriptions"]), 1)
        self.assertEqual(data["subscriptions"][0]["topic"], "museum/rfc")
        self.assertEqual(data["subscriptions"][0]["pending"], 1)
        self.assertEqual(data["unconfirmed"], 1)


class HubPolicyTestCase(HubTestCase):
    """Хаб с политикой тем: отказ — это сообщение подписчику, а не тишина."""

    TOPIC_POLICY = staticmethod(lambda topic: (topic.startswith("museum/"),
                                               "хаб обслуживает только темы museum/*"))

    def test_hub_denies_topic_and_notifies_callback(self):
        cb = self.callback()
        code, payload = self.subscribe(cb, topic="private/rfc")
        self.assertEqual(code, 403, payload)
        self.assertTrue(payload["denied"])
        self.assertTrue(payload["callback_notified"])
        self.assertEqual(self.hub.store.as_list(), [], "отказ хаба — подписки нет")
        denied = [query for _kind, query in cb.gets if (query.get("hub.mode") or [""])[0] == "denied"]
        self.assertEqual(len(denied), 1, "WebSub §5.2: подписчику отправляется hub.mode=denied")
        self.assertIn("museum/*", denied[0]["hub.reason"][0])

    def test_allowed_topic_still_subscribes(self):
        cb = self.callback()
        code, payload = self.subscribe(cb, topic="museum/rfc")
        self.assertEqual(code, 202, payload)


if __name__ == "__main__":
    unittest.main()
