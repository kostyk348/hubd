"""Подписчик: обратная сторона протокола, чтобы hubd был полным WebSub-клиентом.

Хаб и сканер умеют публиковать; этот модуль умеет принимать:

  * GET с ``hub.challenge`` — проверка намерения: challenge обязан вернуться
    эхом (WebSub §5.3), иначе хаб не подтвердит подписку;
  * GET с ``hub.mode=denied`` — хаб сообщил об отказе (WebSub §5.2): пишем
    в журнал с причиной;
  * POST доставки — тело складывается в JSONL-журнал, подпись
    ``X-Hub-Signature: sha256=…`` проверяется по секрету темы.

Секреты тем задаются шаблонами fnmatch (первый подходящий выигрывает):
``--secret "rfc/*=s3cret"`` или файлом ``--secrets-file``:

    {"secrets": {"rfc/*": "s3cret", "museum/*": "другой"}}

Доставка без верной подписи не отбрасывается, а сохраняется с флагом
``signature_valid: false``: строка в журнале честнее тишины — решать вам.
"""
from __future__ import annotations

import fnmatch
import hashlib
import hmac
import json
import os
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

SIGNATURE_HEADER = "X-Hub-Signature"


def signature_valid(secret: str, body: bytes, provided: str) -> bool:
    expected = "sha256=" + hmac.new(secret.encode("utf-8"), body,
                                    hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, provided or "")


class Inbox:
    """Журнал входящих событий: JSONL, дописываем строку — не переписываем файл."""

    def __init__(self, path: str = "", clock=time.time):
        self.path = path
        self.clock = clock
        self.rows: list[dict] = []
        self.lock = threading.Lock()

    def add(self, kind: str, **fields) -> dict:
        row = {"at": self.clock(), "kind": kind}
        row.update(fields)
        with self.lock:
            self.rows.append(row)
            if self.path:
                directory = os.path.dirname(os.path.abspath(self.path))
                os.makedirs(directory, exist_ok=True)
                with open(self.path, "a", encoding="utf-8") as fh:
                    fh.write(json.dumps(row, ensure_ascii=False) + "\n")
                    fh.flush()
                    os.fsync(fh.fileno())
        return row

    def all(self) -> list[dict]:
        with self.lock:
            return list(self.rows)


def parse_secrets(pairs=None, file_path: str = "") -> list[tuple[str, str]]:
    """Секреты тем: [("rfc/*", "s3cret"), …] из флагов и/или файла."""
    out: list[tuple[str, str]] = []
    if file_path:
        with open(file_path, encoding="utf-8") as fh:
            data = json.load(fh)
        secrets = data.get("secrets") or {}
        if not isinstance(secrets, dict):
            raise ValueError("в %s secrets должен быть объектом {шаблон: секрет}" % file_path)
        for pattern, secret in secrets.items():
            out.append((str(pattern), str(secret)))
    for pair in pairs or []:
        if "=" not in pair:
            raise ValueError("секрет задаётся как ТЕМА=СЕКРЕТ, получено %r" % pair)
        topic, secret = pair.split("=", 1)
        topic, secret = topic.strip(), secret
        if not topic:
            raise ValueError("пустой шаблон темы в %r" % pair)
        out.append((topic, secret))
    return out


def secret_for(secrets: list[tuple[str, str]], topic: str) -> str:
    """Первый подходящий шаблон темы; шаблон без * матчится целиком."""
    for pattern, secret in secrets:
        if fnmatch.fnmatchcase(topic, pattern):
            return secret
    return ""


class Subscriber:
    """HTTP-сервер обратного вызова подписчика (challenge, denied, доставки)."""

    def __init__(self, inbox: Inbox, secrets: list[tuple[str, str]],
                 respond: int = 204):
        self.inbox = inbox
        self.secrets = secrets
        self.respond = respond

    # --- маршруты ----------------------------------------------------------------
    def handle_get(self, path: str) -> tuple[int, bytes, dict]:
        query = urllib.parse.parse_qs(urllib.parse.urlparse(path).query)
        topic = (query.get("hub.topic") or [""])[0]
        challenge = (query.get("hub.challenge") or [""])[0]
        mode = (query.get("hub.mode") or [""])[0]
        if challenge:
            self.inbox.add("challenge", topic=topic, mode=mode)
            return 200, challenge.encode("utf-8"), {}
        if mode == "denied":
            reason = (query.get("hub.reason") or [""])[0]
            self.inbox.add("denied", topic=topic, reason=reason)
            return 200, b"", {}
        return 200, b"", {}

    def handle_post(self, headers, body: bytes, path: str) -> tuple[int, dict]:
        topic = headers.get("X-Hub-Topic") or ""
        if not topic:
            query = urllib.parse.parse_qs(urllib.parse.urlparse(path).query)
            topic = (query.get("topic") or [""])[0]
        provided = headers.get(SIGNATURE_HEADER) or ""
        secret = secret_for(self.secrets, topic)
        valid = bool(secret) and signature_valid(secret, body, provided)
        row = self.inbox.add("delivery", topic=topic, bytes=len(body),
                             signature=provided,
                             signature_valid=valid if secret else None,
                             body=body.decode("utf-8", "replace"))
        print("доставка: тема=%r, %d байт, подпись: %s" % (
            topic, len(body),
            "верная" if valid else ("нет секрета — не проверялась" if not secret
                                    else "НЕВЕРНАЯ")))
        return self.respond, row

    def build_server(self, host: str = "127.0.0.1", port: int = 0) -> ThreadingHTTPServer:
        subscriber = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, format, *args):      # журнал ведёт Inbox
                pass

            def do_GET(self):
                code, body, _row = subscriber.handle_get(self.path)
                self.send_response(code)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_POST(self):
                length = int(self.headers.get("Content-Length", "0") or 0)
                body = self.rfile.read(length)
                code, _row = subscriber.handle_post(self.headers, body, self.path)
                self.send_response(code)
                self.send_header("Content-Length", "0")
                self.end_headers()

        return ThreadingHTTPServer((host, port), Handler)
