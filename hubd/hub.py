"""Хаб подписок: аренда, доставка с подписью, честный отказ.

Правила, ради которых это написано (взяты из находок музея):
  * издатель не знает подписчиков — публикация идёт в тему, не в список адресатов (ipmulticast);
  * подписка — аренда с явным сроком, продление — обязанность подписчика (WebSub/PubSubHubbub);
  * контракт доставки: подписчик, который трижды не подтвердил приём, помечается degraded,
    и хаб больше не делает вид, что доставил — он честно отказывает (rsvp/atmip).

HTTP-API (совместим по духу с PubSubHubbub/WebSub, но без чужих зависимостей):
  POST /subscribe    hub.mode=subscribe, hub.topic, hub.callback, hub.lease_seconds, hub.secret
  POST /unsubscribe  hub.mode=unsubscribe, hub.topic, hub.callback
  POST /publish?topic=...   тело — полезная нагрузка; ответ — отчёт о доставке
  GET  /subscriptions   состояние подписок (без секретов)
  GET  /healthz
"""
from __future__ import annotations

import hashlib
import hmac
import json
import secrets
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .store import Store

SIGNATURE_HEADER = "X-Hub-Signature"
MAX_BODY = 5 * 1024 * 1024


def sign(secret: str, body: bytes) -> str:
    return "sha256=" + hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()


class Hub:
    def __init__(self, store_path: str, clock=time.time, max_failures: int = 3,
                 timeout: float = 5.0, journal_path: str | None = None):
        self.store = Store(store_path, clock=clock)
        self.clock = clock
        self.max_failures = max_failures
        self.timeout = timeout
        self.journal_path = journal_path
        self.journal: list[dict] = []

    # --- подписка ----------------------------------------------------------------
    def verify_intent(self, topic: str, callback: str, mode: str, lease_seconds: int) -> tuple[bool, str]:
        """Проверка намерения: подписчик должен эхом вернуть наш challenge."""
        challenge = secrets.token_hex(16)
        query = urllib.parse.urlencode({
            "hub.mode": mode, "hub.topic": topic,
            "hub.challenge": challenge, "hub.lease_seconds": lease_seconds,
        })
        url = callback + ("&" if "?" in callback else "?") + query
        try:
            with urllib.request.urlopen(url, timeout=self.timeout) as resp:
                body = resp.read().decode("utf-8", "replace").strip()
                if resp.status // 100 != 2:
                    return False, "callback ответил %s" % resp.status
        except (urllib.error.URLError, OSError) as exc:
            return False, "callback недоступен: %s" % exc
        if body != challenge:
            return False, "challenge не подтверждён"
        return True, ""

    def subscribe(self, topic: str, callback: str, lease_seconds: int = 3600,
                  secret: str | None = None) -> tuple[int, dict]:
        if not topic or not callback:
            return 400, {"error": "нужны hub.topic и hub.callback"}
        ok, why = self.verify_intent(topic, callback, "subscribe", lease_seconds)
        if not ok:
            self._log("subscribe", topic, callback, False, why)
            return 400, {"error": why}
        sub = self.store.subscribe(topic, callback, lease_seconds, secret)
        self._log("subscribe", topic, callback, True, "аренда %ss" % sub["lease_seconds"])
        return 202, {"id": sub["id"], "topic": topic, "lease_seconds": sub["lease_seconds"],
                     "expires_in": self.store.expires_in(sub)}

    def unsubscribe(self, topic: str, callback: str) -> tuple[int, dict]:
        if not self.store.unsubscribe(topic, callback):
            return 404, {"error": "подписка не найдена"}
        self._log("unsubscribe", topic, callback, True, "")
        return 202, {"removed": True}

    # --- публикация --------------------------------------------------------------
    def publish(self, topic: str, body: bytes, content_type: str = "application/octet-stream") -> tuple[int, dict]:
        subs = self.store.for_topic(topic)
        report = {"topic": topic, "bytes": len(body), "delivered": [], "failed": [], "refused": []}
        if not subs:
            report["note"] = "нет активных подписчиков: публикация никому не адресована"
            self._log("publish", topic, "-", False, report["note"])
            return 200, report
        for sub in subs:
            if sub.get("degraded"):
                report["refused"].append({"id": sub["id"], "callback": sub["callback"],
                                          "reason": "контракт нарушен: %s" % sub.get("last_error", "нет подтверждений")})
                continue
            ok, error = self._deliver(sub, topic, body, content_type)
            self.store.note_result(sub["id"], ok, error, self.max_failures)
            (report["delivered"] if ok else report["failed"]).append({"id": sub["id"], "callback": sub["callback"]})
        self._log("publish", topic, "-",
                  bool(report["delivered"]),
                  "подписчиков %d, доставлено %d, отказов %d" % (
                      len(subs), len(report["delivered"]), len(report["refused"])))
        if not report["delivered"]:
            return (503 if report["refused"] else 502), report
        return 200, report

    def _deliver(self, sub: dict, topic: str, body: bytes, content_type: str) -> tuple[bool, str]:
        headers = {
            "Content-Type": content_type,
            "Content-Length": str(len(body)),
            "X-Hub-Topic": topic,
            "X-Hub-Delivery": uuid.uuid4().hex,
            "X-Hub-Lease-Remaining": str(max(0, self.store.expires_in(sub))),
        }
        if sub.get("secret"):
            headers[SIGNATURE_HEADER] = sign(sub["secret"], body)
        req = urllib.request.Request(sub["callback"], data=body, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                if resp.status // 100 == 2:
                    return True, ""
                return False, "callback ответил %s" % resp.status
        except urllib.error.HTTPError as exc:
            exc.close()
            return False, "callback ответил %s" % exc.code
        except (urllib.error.URLError, OSError) as exc:
            return False, "callback недоступен: %s" % exc

    # --- журнал ------------------------------------------------------------------
    def _log(self, action: str, topic: str, who: str, ok: bool, note: str) -> None:
        record = {"at": self.clock(), "action": action, "topic": topic, "who": who, "ok": ok, "note": note}
        self.journal.append(record)
        if self.journal_path:
            try:
                with open(self.journal_path, "a", encoding="utf-8") as fh:
                    fh.write(json.dumps(record, ensure_ascii=False) + "\n")
            except OSError:
                pass


class _Handler(BaseHTTPRequestHandler):
    hub: "Hub" = None          # type: ignore[assignment]
    protocol_version = "HTTP/1.1"

    def log_message(self, format, *args):        # тишина по умолчанию: журнал ведёт хаб
        pass

    # --- вспомогательное ---------------------------------------------------------
    def _send(self, code: int, payload: dict) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _form(self) -> dict:
        length = min(int(self.headers.get("Content-Length", "0") or 0), MAX_BODY)
        raw = self.rfile.read(length).decode("utf-8", "replace")
        flat = urllib.parse.parse_qs(raw, keep_blank_values=True)
        return {k: v[0] for k, v in flat.items()}

    def _body(self) -> bytes:
        length = min(int(self.headers.get("Content-Length", "0") or 0), MAX_BODY)
        return self.rfile.read(length)

    # --- маршруты ----------------------------------------------------------------
    def do_GET(self):
        path = urllib.parse.urlparse(self.path).path
        if path == "/healthz":
            self._send(200, {"ok": True, "subscriptions": len(self.hub.store.as_list()),
                             "now": int(self.hub.clock())})
        elif path == "/subscriptions":
            self._send(200, {"subscriptions": self.hub.store.as_list()})
        elif path == "/journal":
            self._send(200, {"journal": self.hub.journal[-50:]})
        else:
            self._send(404, {"error": "нет такого пути"})

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/subscribe":
            form = self._form()
            code, payload = self.hub.subscribe(
                form.get("hub.topic", ""), form.get("hub.callback", ""),
                int(form.get("hub.lease_seconds", 3600) or 3600), form.get("hub.secret") or None)
            self._send(code, payload)
        elif parsed.path == "/unsubscribe":
            form = self._form()
            code, payload = self.hub.unsubscribe(form.get("hub.topic", ""), form.get("hub.callback", ""))
            self._send(code, payload)
        elif parsed.path == "/publish":
            query = urllib.parse.parse_qs(parsed.query)
            topic = (query.get("topic") or [""])[0]
            if not topic:
                self._send(400, {"error": "нужен ?topic="})
                return
            code, payload = self.hub.publish(topic, self._body(),
                                             self.headers.get("Content-Type", "application/octet-stream"))
            self._send(code, payload)
        else:
            self._send(404, {"error": "нет такого пути"})


def build_server(hub: Hub, host: str = "127.0.0.1", port: int = 8765) -> ThreadingHTTPServer:
    handler = type("BoundHandler", (_Handler,), {"hub": hub})
    return ThreadingHTTPServer((host, port), handler)
