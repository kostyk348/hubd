"""Хаб подписок: аренда, доставка с подписью, честный отказ.

Правила, ради которых это написано (взяты из находок музея):
  * издатель не знает подписчиков — публикация идёт в тему, не в список адресатов (ipmulticast);
  * подписка — аренда с явным сроком, продление — обязанность подписчика (WebSub/PubSubHubbub);
  * контракт доставки (rsvp/atmip): неподтверждённая доставка не теряется и не убивает подписку
    с первого раза — она ждёт в очереди и повторяется с растущей паузой. Хаб сдаётся только
    когда исчерпал попытки, и тогда помечает подписку degraded и честно отказывает (503 + причина).

Отношение к WebSub (RFC-подобная дисциплина, без чужих зависимостей):
  * подписка проверяется challenge'ом, который подписчик обязан эхом вернуть;
  * подписчик может отказаться от подписки — ответ 404 на проверку означает «не хочу»;
  * хаб может отказать сам — тогда он обязан сообщить это подписчику: GET с hub.mode=denied,
    hub.topic и hub.reason (WebSub §5.2). Значение hub.mode=denied принадлежит хабу: подписчик,
    который прислал его в POST /subscribe, получает 400.

HTTP-API:
  POST /subscribe    hub.mode=subscribe, hub.topic, hub.callback, hub.lease_seconds, hub.secret
  POST /unsubscribe  hub.mode=unsubscribe, hub.topic, hub.callback
  POST /publish?topic=...   тело — полезная нагрузка; ответ — отчёт о доставке
  POST /retry        одна проходка по очереди неподтверждённых доставок
  GET  /subscriptions   состояние подписок (без секретов)
  GET  /healthz
"""
from __future__ import annotations

import fnmatch
import hashlib
import hmac
import json
import secrets
import ssl
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .auth import token_from_headers
from .store import DEFAULT_MAX_ATTEMPTS, Store

SIGNATURE_HEADER = "X-Hub-Signature"
MAX_BODY = 5 * 1024 * 1024
DENIED_STATUS = (404, 410)          # подписчик отказывается — WebSub §5.3.1


def sign(secret: str, body: bytes) -> str:
    return "sha256=" + hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()


def topic_policy_from_config(config: dict):
    """Политика тем из конфига: {"deny": ["private/*"], "allow": ["museum/*"]}.

    Шаблоны fnmatch. Порядок честный: deny проверяется первым (запрещённое
    запрещено, даже если совпало с allow), затем allow — тема вне списка
    разрешённых получает отказ с причиной. Пустые списки ничего не меняют.
    Возвращает callable(topic) -> (bool, причина) — тот же контракт, что у
    --topic-prefix, только без правки кода.
    """
    deny = [str(pattern) for pattern in config.get("deny") or []]
    allow = [str(pattern) for pattern in config.get("allow") or []]

    def decide(topic: str) -> tuple[bool, str]:
        for pattern in deny:
            if fnmatch.fnmatchcase(topic, pattern):
                return False, "тема %r запрещена политикой (deny %r)" % (topic, pattern)
        if allow and not any(fnmatch.fnmatchcase(topic, pattern) for pattern in allow):
            return False, "хаб обслуживает только темы: %s" % ", ".join(allow)
        return True, ""

    return decide


def load_policy_file(path: str):
    """Прочитать JSON-файл политики и вернуть callable(topic)."""
    with open(path, encoding="utf-8") as fh:
        config = json.load(fh)
    if not isinstance(config, dict):
        raise ValueError('политика должна быть JSON-объектом: {"allow": […], "deny": […]}')
    return topic_policy_from_config(config)


class Hub:
    def __init__(self, store_path: str, clock=time.time, max_attempts: int = DEFAULT_MAX_ATTEMPTS,
                 timeout: float = 5.0, journal_path: str | None = None, topic_policy=None,
                 pending_max_bytes: int | None = None, publish_auth=None):
        self.store = (Store(store_path, clock=clock) if pending_max_bytes is None
                      else Store(store_path, clock=clock, pending_max_bytes=pending_max_bytes))
        self.clock = clock
        self.max_attempts = max_attempts
        self.timeout = timeout
        self.journal_path = journal_path
        self.topic_policy = topic_policy      # callable(topic) -> bool | (bool, причина)
        self.publish_auth = publish_auth      # auth.TokenStore или None (хаб открыт)
        self.journal: list[dict] = []
        # Хаб обслуживается несколькими потоками (HTTP-сервер + проходка по очереди),
        # а хранилище — один JSON-файл: изменения сериализуем, иначе потеряется запись.
        self.lock = threading.RLock()

    # --- подписка ----------------------------------------------------------------
    def verify_intent(self, topic: str, callback: str, mode: str,
                      lease_seconds: int) -> tuple[bool, str, bool]:
        """Проверка намерения. Возвращает (подтверждено, причина, отказ подписчика)."""
        challenge = secrets.token_hex(16)
        query = urllib.parse.urlencode({
            "hub.mode": mode, "hub.topic": topic,
            "hub.challenge": challenge, "hub.lease_seconds": lease_seconds,
        })
        url = callback + ("&" if "?" in callback else "?") + query
        try:
            with urllib.request.urlopen(url, timeout=self.timeout) as resp:
                status = resp.status
                body = resp.read().decode("utf-8", "replace").strip()
        except urllib.error.HTTPError as exc:
            code = exc.code
            exc.close()
            if code in DENIED_STATUS:
                return False, "подписчик отказался от подписки (HTTP %s)" % code, True
            return False, "callback ответил %s" % code, False
        except (urllib.error.URLError, OSError) as exc:
            return False, "callback недоступен: %s" % exc, False
        if status // 100 != 2:
            return False, "callback ответил %s" % status, False
        if body != challenge:
            return False, "challenge не подтверждён", False
        return True, "", False

    def notify_denied(self, topic: str, callback: str, reason: str) -> bool:
        """Сказать подписчику, что хаб отказал: GET hub.mode=denied (WebSub §5.2)."""
        query = urllib.parse.urlencode({"hub.mode": "denied", "hub.topic": topic, "hub.reason": reason})
        url = callback + ("&" if "?" in callback else "?") + query
        try:
            with urllib.request.urlopen(url, timeout=self.timeout) as resp:
                return resp.status // 100 == 2
        except urllib.error.HTTPError as exc:
            exc.close()
            return False
        except (urllib.error.URLError, OSError):
            return False

    def _policy(self, topic: str) -> tuple[bool, str]:
        if self.topic_policy is None:
            return True, ""
        verdict = self.topic_policy(topic)
        if isinstance(verdict, tuple):
            return bool(verdict[0]), str(verdict[1] if len(verdict) > 1 else "")
        return bool(verdict), "тема не разрешена политикой хаба"

    def subscribe(self, topic: str, callback: str, lease_seconds: int = 3600,
                  secret: str | None = None, mode: str = "subscribe") -> tuple[int, dict]:
        if mode != "subscribe":
            return 400, {"error": "hub.mode должен быть \"subscribe\"; \"denied\" хаб отправляет "
                                  "подписчику сам, а не принимает от него"}
        if not topic or not callback:
            return 400, {"error": "нужны hub.topic и hub.callback"}
        allowed, reason = self._policy(topic)
        if not allowed:
            notified = self.notify_denied(topic, callback, reason)
            self._log("deny", topic, callback, False, reason)
            return 403, {"error": "подписка отклонена хабом: %s" % reason,
                         "denied": True, "callback_notified": notified}
        ok, why, _denied = self.verify_intent(topic, callback, "subscribe", lease_seconds)
        if not ok:
            self._log("subscribe", topic, callback, False, why)
            return 400, {"error": why}
        with self.lock:
            sub = self.store.subscribe(topic, callback, lease_seconds, secret)
            expires_in = self.store.expires_in(sub)
        self._log("subscribe", topic, callback, True, "аренда %ss" % sub["lease_seconds"])
        return 202, {"id": sub["id"], "topic": topic, "lease_seconds": sub["lease_seconds"],
                     "expires_in": expires_in}

    def unsubscribe(self, topic: str, callback: str) -> tuple[int, dict]:
        with self.lock:
            removed = self.store.unsubscribe(topic, callback)
        if not removed:
            return 404, {"error": "подписка не найдена"}
        self._log("unsubscribe", topic, callback, True, "")
        return 202, {"removed": True}

    # --- публикация --------------------------------------------------------------
    def publish(self, topic: str, body: bytes, content_type: str = "application/octet-stream") -> tuple[int, dict]:
        with self.lock:
            now = self.clock()
            subs = self.store.for_topic(topic)
        report = {"topic": topic, "bytes": len(body), "delivered": [], "queued": [],
                  "not_queued": [], "refused": [], "unconfirmed": 0, "confirmed": True}
        if not subs:
            report["note"] = "нет активных подписчиков: публикация никому не адресована"
            with self.lock:
                report["unconfirmed"] = self.store.unconfirmed()
            report["confirmed"] = report["unconfirmed"] == 0
            self._log("publish", topic, "-", False, report["note"])
            return 200, report
        for sub in subs:
            if sub.get("degraded"):
                report["refused"].append({
                    "id": sub["id"], "callback": sub["callback"],
                    "reason": sub.get("degraded_reason")
                              or "контракт нарушен: %s" % (sub.get("last_error") or "нет подтверждений")})
                continue
            ok, error = self._deliver(sub, topic, body, content_type)
            with self.lock:
                if ok:
                    self.store.note_result(sub["id"], True)
                    report["delivered"].append({"id": sub["id"], "callback": sub["callback"]})
                    continue
                self.store.note_result(sub["id"], False, error)
                entry = self.store.enqueue(sub["id"], topic, body, content_type, error)
            if entry is None:
                report["not_queued"].append({
                    "id": sub["id"], "callback": sub["callback"], "error": error,
                    "reason": "тело больше очереди (%d байт): повторная доставка невозможна" % len(body)})
            else:
                report["queued"].append({
                    "id": sub["id"], "callback": sub["callback"], "attempts": entry["attempts"],
                    "next_in": max(0, int(entry["next"] - now)), "error": error,
                    "reason": "не подтверждено: ждёт повторной попытки"})
        with self.lock:
            report["unconfirmed"] = self.store.unconfirmed()
        report["confirmed"] = not report["queued"] and not report["not_queued"]
        self._log("publish", topic, "-", bool(report["delivered"]),
                  "подписчиков %d, доставлено %d, в очереди %d, отказов %d, не подтверждено всего %d" % (
                      len(subs), len(report["delivered"]), len(report["queued"]),
                      len(report["refused"]), report["unconfirmed"]))
        return self._publish_status(report), report

    @staticmethod
    def _publish_status(report: dict) -> int:
        """201-подобная честность: 200 означает «доставлено всем подписчикам», а не «принято».

        202 — принято, но что-то ещё не подтверждено (уйдёт в повтор);
        502 — доставить не смогли и повторить не сможем (тело больше очереди);
        503 — все подписчики нарушили контракт доставки.
        """
        if report["queued"]:
            return 202
        if report["not_queued"]:
            return 502
        if report["delivered"]:
            return 200
        if report["refused"]:
            return 503
        return 200

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

    # --- очередь приповторов -----------------------------------------------------
    def retry(self) -> dict:
        """Одна проходка по очереди: то, чья пауза истекла, пытаемся доставить снова."""
        with self.lock:
            self.store.expire()
            due = self.store.due_pending()
        report = {"attempted": 0, "delivered": [], "still_pending": [], "gave_up": [],
                  "dropped": [], "unconfirmed": 0}
        for entry in due:
            with self.lock:
                sub = self.store.subs.get(entry["sub"])
                if sub is None:
                    self.store.drop_pending(entry["key"])
            if sub is None:
                report["dropped"].append({"key": entry["key"], "topic": entry["topic"],
                                          "reason": "подписки больше нет (истекла или снята)"})
                continue
            report["attempted"] += 1
            body = self.store.body_of(entry)
            ok, error = self._deliver(sub, entry["topic"], body, entry["content_type"])
            with self.lock:
                outcome = self.store.note_attempt(entry["key"], ok, error, self.max_attempts)
                now = self.clock()
            row = {"key": entry["key"], "id": entry["sub"], "topic": entry["topic"],
                   "callback": sub["callback"], "attempts": outcome["entry"]["attempts"],
                   "bytes": entry["bytes"]}
            if outcome["state"] == "delivered":
                report["delivered"].append(row)
            elif outcome["state"] == "retry":
                row["next_in"] = max(0, int(outcome["entry"]["next"] - now))
                report["still_pending"].append(row)
            elif outcome["state"] == "gave_up":
                row["reason"] = sub.get("degraded_reason") or "попытки исчерпаны"
                report["gave_up"].append(row)
        with self.lock:
            report["unconfirmed"] = self.store.unconfirmed()
        if report["attempted"] or report["dropped"]:
            self._log("retry", "-", "-", bool(report["delivered"]),
                      "попыток %d, доставлено %d, ждут %d, сдались %d, брошено %d, не подтверждено всего %d" % (
                          report["attempted"], len(report["delivered"]), len(report["still_pending"]),
                          len(report["gave_up"]), len(report["dropped"]), report["unconfirmed"]))
        return report

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
                             "unconfirmed": self.hub.store.unconfirmed(),
                             "now": int(self.hub.clock())})
        elif path == "/subscriptions":
            self._send(200, {"subscriptions": self.hub.store.as_list(),
                             "unconfirmed": self.hub.store.unconfirmed()})
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
                int(form.get("hub.lease_seconds", 3600) or 3600), form.get("hub.secret") or None,
                mode=form.get("hub.mode", ""))
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
            if self.hub.publish_auth is not None:
                token = token_from_headers(self.headers)
                allowed, reason = self.hub.publish_auth.check(token, topic)
                if not allowed:
                    self.hub._log("publish", topic, "-", False, "отказано: %s" % reason)
                    self._send(401, {"error": reason})
                    return
            code, payload = self.hub.publish(topic, self._body(),
                                             self.headers.get("Content-Type", "application/octet-stream"))
            self._send(code, payload)
        elif parsed.path == "/retry":
            self._send(200, self.hub.retry())
        else:
            self._send(404, {"error": "нет такого пути"})


def build_server(hub: Hub, host: str = "127.0.0.1", port: int = 8765,
                 tls_cert: str = "", tls_key: str = "") -> ThreadingHTTPServer:
    """Сервер хаба. С --tls-cert/--tls-key сокет оборачивается в TLS: тот же
    протокол, но доставка и публикация идут по https."""
    handler = type("BoundHandler", (_Handler,), {"hub": hub})
    server = ThreadingHTTPServer((host, port), handler)
    if tls_cert or tls_key:
        if not (tls_cert and tls_key):
            raise ValueError("нужны оба файла: --tls-cert и --tls-key")
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(tls_cert, tls_key)
        server.socket = context.wrap_socket(server.socket, server_side=True)
    return server
