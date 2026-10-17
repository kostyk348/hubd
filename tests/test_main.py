"""Тесты CLI на недоступном хабе: сбой публикации — сообщение и код 0, а не traceback.

Проверяем ровно то, что уронило ночной обход музея: `hubd` поднимают вручную, и в момент
сканирования его может не быть. «Хаб не отвечает» — штатное состояние, а не падение обхода.
"""
from __future__ import annotations

import hashlib
import io
import json
import os
import socket
import tempfile
import threading
import unittest
from contextlib import redirect_stderr, redirect_stdout
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from hubd.__main__ import _get_json, _post_form, _post_json, _post_publish, main
from hubd.hub import Hub, build_server


def dead_url() -> str:
    """Адрес, на котором заведомо никто не слушает: соединение отклонено сразу."""
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return "http://127.0.0.1:%d" % port


class GarbageServer:
    """Порт открыт, но говорит не по HTTP (как sshd): соединение есть, хаба нет."""

    def __init__(self):
        self.sock = socket.socket()
        self.sock.bind(("127.0.0.1", 0))
        self.sock.listen(5)
        self.port = self.sock.getsockname()[1]
        self.stop = threading.Event()
        threading.Thread(target=self._serve, daemon=True).start()

    def _serve(self):
        while not self.stop.is_set():
            try:
                conn, _ = self.sock.accept()
            except OSError:
                return
            with conn:
                try:
                    conn.recv(1024)
                    conn.sendall(b"SSH-2.0-OpenSSH_9.6\r\n")
                except OSError:
                    pass

    def close(self):
        self.stop.set()
        self.sock.close()


class SourceServer:
    """Живой источник: отдаёт тело с ETag и умеет 304."""

    def __init__(self, body: bytes, etag: str):
        self.body = body
        self.etag = etag
        source = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, format, *args):
                pass

            def do_GET(self):
                if self.headers.get("If-None-Match") == source.etag:
                    self.send_response(304)
                    self.send_header("ETag", source.etag)
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                self.send_response(200)
                self.send_header("ETag", source.etag)
                self.send_header("Content-Length", str(len(source.body)))
                self.end_headers()
                self.wfile.write(source.body)

            def do_HEAD(self):
                self.send_response(405)
                self.send_header("Content-Length", "0")
                self.end_headers()

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.url = "http://127.0.0.1:%d/feed" % self.server.server_address[1]

    def close(self):
        self.server.shutdown()


def run_cli(*argv) -> tuple[int, str, str]:
    """Вызов CLI как из терминала: (код, stdout, stderr)."""
    out, err = io.StringIO(), io.StringIO()
    with redirect_stdout(out), redirect_stderr(err):
        code = main(list(argv))
    return code, out.getvalue(), err.getvalue()


class HubDownCallsTestCase(unittest.TestCase):
    """Низкий уровень: каждая обёртка над urllib возвращает ошибку, а не бросает её."""

    def test_post_publish_reports_refused_connection(self):
        code, report = _post_publish(dead_url(), "demo/topic", b"x")
        self.assertEqual(code, 0)
        self.assertIn("хаб недоступен", report["error"])
        self.assertIn("соединение отклонено", report["error"])

    def test_post_form_reports_refused_connection(self):
        code, report = _post_form(dead_url() + "/subscribe", {"hub.mode": "subscribe"})
        self.assertEqual(code, 0)
        self.assertIn("хаб недоступен", report["error"])

    def test_get_json_reports_refused_connection(self):
        data = _get_json(dead_url() + "/subscriptions")
        self.assertIn("хаб недоступен", data["error"])

    def test_post_json_reports_refused_connection(self):
        data = _post_json(dead_url() + "/retry")
        self.assertIn("хаб недоступен", data["error"])

    def test_port_that_speaks_something_else_is_not_a_hub(self):
        garbage = GarbageServer()
        try:
            code, report = _post_publish("http://127.0.0.1:%d" % garbage.port, "demo", b"x")
        finally:
            garbage.close()
        self.assertEqual(code, 0)
        self.assertIn("хаб недоступен", report["error"])
        self.assertIn("не HTTP-хаб", report["error"])


class CommandsWithoutHubTestCase(unittest.TestCase):
    """Команды при выключенном хабе печатают причину и выходят с кодом 0."""

    def test_retry_with_dead_hub_exits_zero(self):
        code, out, err = run_cli("retry", "--hub", dead_url())
        self.assertEqual(code, 0)
        self.assertIn("хаб недоступен", out)
        self.assertIn("повторите после запуска хаба", out)
        self.assertNotIn("Traceback", err)

    def test_subs_with_dead_hub_exits_zero(self):
        code, out, err = run_cli("subs", "--hub", dead_url())
        self.assertEqual(code, 0)
        self.assertIn("хаб недоступен", out)
        self.assertNotIn("Traceback", err)

    def test_publish_with_dead_hub_says_nothing_was_published(self):
        code, out, err = run_cli("publish", "--hub", dead_url(), "--topic", "museum/rfc", "--text", "x")
        self.assertEqual(code, 1, "ручная публикация, которая не дошла, — это провал команды")
        self.assertIn("ничего не опубликовано", out)
        self.assertIn("хаб недоступен", out)
        self.assertNotIn("Traceback", err)


class ScanPublishTestCase(unittest.TestCase):
    """scan: сбор кандидатов без хаба, публикация — только по --publish."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = self.tmp.name
        self.source = SourceServer(b"version 1", '"v1"')
        self.watchlist = os.path.join(self.dir, "watchlist.json")
        with open(self.watchlist, "w", encoding="utf-8") as fh:
            json.dump([{"url": self.source.url, "topic": "museum/feed"}], fh)
        self.state = os.path.join(self.dir, "scan.json")
        self.queue = os.path.join(self.dir, "candidates.jsonl")
        self._seed_previous_version()

    def tearDown(self):
        self.source.close()
        self.tmp.cleanup()

    def _seed_previous_version(self):
        """Состояние «источник уже знаком, содержимое было другим» — чтобы обход увидел изменение."""
        with open(self.state, "w", encoding="utf-8") as fh:
            json.dump({"saved": 0, "sources": {self.source.url: {
                "hash": hashlib.sha256(b"version 0").hexdigest(),
                "checks": 1, "changes": 0, "status": 200, "etag": '"v0"',
            }}}, fh)

    def _scan_args(self):
        return ["scan", "--watchlist", self.watchlist, "--state", self.state,
                "--queue", self.queue, "--interval", "0", "--once"]

    def _queue_rows(self):
        with open(self.queue, encoding="utf-8") as fh:
            return [json.loads(line) for line in fh if line.strip()]

    def test_scan_without_publish_never_touches_the_hub(self):
        recorder = RecordingHub()
        try:
            code, out, err = run_cli(*self._scan_args(), "--hub", recorder.url)
        finally:
            recorder.close()
        self.assertEqual(code, 0)
        self.assertEqual(recorder.paths, [], "без --publish сканер не ходит в хаб вовсе")
        self.assertIn("публикация в хаб: выключена", out)
        self.assertEqual(len(self._queue_rows()), 1, "кандидат собран без хаба")
        self.assertNotIn("Traceback", err)

    def test_scan_with_publish_against_dead_hub_still_finishes(self):
        code, out, err = run_cli(*self._scan_args(), "--publish", "--hub", dead_url())
        self.assertEqual(code, 0, "обход состоялся даже без хаба")
        self.assertIn("публикация не удалась: хаб недоступен", out)
        self.assertIn("недоступен 1", out)
        self.assertEqual(len(self._queue_rows()), 1, "кандидат записан до публикации")
        self.assertNotIn("Traceback", err)

    def test_scan_with_publish_against_live_hub_delivers(self):
        hub = Hub(os.path.join(self.dir, "hub.json"))
        server = build_server(hub, "127.0.0.1", 0)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        try:
            code, out, err = run_cli(*self._scan_args(), "--publish",
                                     "--hub", "http://127.0.0.1:%d" % server.server_address[1])
        finally:
            server.shutdown()
            server.server_close()
        self.assertEqual(code, 0)
        self.assertIn("публикация 200", out)
        self.assertIn("хаб принял 1", out)
        self.assertNotIn("Traceback", err)


class RecordingHub:
    """Пустой HTTP-сервер, который запоминает пути запросов."""

    def __init__(self):
        recorder = self
        self.paths: list[str] = []

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, format, *args):
                pass

            def do_POST(self):
                recorder.paths.append(self.path)
                self.send_response(200)
                self.send_header("Content-Length", "2")
                self.end_headers()
                self.wfile.write(b"{}")

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.url = "http://127.0.0.1:%d" % self.server.server_address[1]

    def close(self):
        self.server.shutdown()


if __name__ == "__main__":
    unittest.main()
