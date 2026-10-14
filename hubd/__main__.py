"""CLI: hubd serve | scan | subs | publish | demo | check.

    python -m hubd serve   --store data/hub.json --port 8765
    python -m hubd scan    --watchlist watchlist.json --store data/hub.json --hub http://127.0.0.1:8765
    python -m hubd subs    --hub http://127.0.0.1:8765
    python -m hubd publish --hub http://127.0.0.1:8765 --topic rfc/3170 --text "обновилось"
    python -m hubd demo                      # хаб + подписчик + публикация, всё на loopback
    python -m hubd check   --watchlist watchlist.json --state data/scan.json   # без публикации
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .hub import Hub, build_server
from .scan import Watcher, load_watchlist


def _post_form(url: str, form: dict) -> tuple[int, dict]:
    data = urllib.parse.urlencode(form).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST",
                                 headers={"Content-Type": "application/x-www-form-urlencoded"})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8") or "{}"
        exc.close()
        return exc.code, json.loads(raw)


def _post_publish(hub_url: str, topic: str, body: bytes) -> tuple[int, dict]:
    url = "%s/publish?topic=%s" % (hub_url.rstrip("/"), urllib.parse.quote(topic))
    req = urllib.request.Request(url, data=body, method="POST",
                                 headers={"Content-Type": "application/octet-stream"})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8") or "{}"
        exc.close()
        return exc.code, json.loads(raw)


def _get_json(url: str) -> dict:
    with urllib.request.urlopen(url, timeout=15) as resp:
        return json.loads(resp.read().decode("utf-8"))


def cmd_serve(args) -> int:
    hub = Hub(args.store, journal_path=args.journal)
    server = build_server(hub, args.host, args.port)
    host, port = server.server_address[:2]
    print("хаб слушает http://%s:%d  (store: %s)" % (host, port, args.store))
    print("подписок: %d" % len(hub.store.as_list()))
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nостановлено")
    return 0


def cmd_scan(args) -> int:
    watcher = Watcher(args.state, min_interval=args.interval)
    watchlist = load_watchlist(args.watchlist)
    rounds = 1 if args.once else 0
    while True:
        events = watcher.scan(watchlist)
        if events:
            for event in events:
                code, report = _post_publish(args.hub, event["topic"], event["payload"])
                print("изменился %s → публикация %s: %s" % (event["url"], code,
                                                           json.dumps(report, ensure_ascii=False)[:200]))
        else:
            print("изменений нет (%d источников)" % len(watchlist))
        rounds += 1
        if args.once:
            return 0
        time.sleep(args.every)


def cmd_check(args) -> int:
    watcher = Watcher(args.state, min_interval=args.interval)
    for item in load_watchlist(args.watchlist):
        result = watcher.check(item["url"])
        print("%-4s %-8s %s" % (result["status"], "изменён" if result["changed"] else "без изменений",
                                result["url"]))
    return 0


def cmd_subs(args) -> int:
    data = _get_json(args.hub.rstrip("/") + "/subscriptions")
    subs = data.get("subscriptions", [])
    if not subs:
        print("подписок нет")
        return 0
    for sub in subs:
        print("%-16s %-28s %s  аренда %ss, доставок %d, провалов %d%s" % (
            sub["id"], sub["topic"][:28], sub["callback"], sub["expires_in"],
            sub["deliveries"], sub["failures"], "  DEGRADED" if sub.get("degraded") else ""))
    return 0


def cmd_publish(args) -> int:
    body = args.text.encode("utf-8") if args.text else sys.stdin.buffer.read()
    code, report = _post_publish(args.hub, args.topic, body)
    print("HTTP %s %s" % (code, json.dumps(report, ensure_ascii=False, indent=1)))
    return 0 if code == 200 else 1


def cmd_demo(args) -> int:
    """Живая демонстрация: хаб + подписчик + две публикации. Всё на 127.0.0.1."""
    received: list[bytes] = []

    class Callback(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, format, *args):
            pass

        def do_GET(self):
            query = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            challenge = (query.get("hub.challenge") or [""])[0]
            body = challenge.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self):
            length = int(self.headers.get("Content-Length", "0") or 0)
            payload = self.rfile.read(length)
            received.append(payload)
            print("  ← доставка %d байт, подпись: %s" % (
                len(payload), self.headers.get("X-Hub-Signature", "нет")))
            self.send_response(204)
            self.send_header("Content-Length", "0")
            self.end_headers()

    callback = ThreadingHTTPServer(("127.0.0.1", 0), Callback)
    threading.Thread(target=callback.serve_forever, daemon=True).start()
    callback_url = "http://127.0.0.1:%d/cb" % callback.server_address[1]

    hub = Hub(os.path.join(args.workdir, "demo-hub.json"), journal_path=os.path.join(args.workdir, "demo-hub.log"))
    server = build_server(hub, "127.0.0.1", 0)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    hub_url = "http://127.0.0.1:%d" % server.server_address[1]
    print("хаб: %s, подписчик: %s" % (hub_url, callback_url))

    code, payload = _post_form(hub_url + "/subscribe", {
        "hub.mode": "subscribe", "hub.topic": "museum/rfc",
        "hub.callback": callback_url, "hub.lease_seconds": 3600, "hub.secret": "s3cret"})
    print("подписка: HTTP %s %s" % (code, json.dumps(payload, ensure_ascii=False)))

    for text in (b"RFC 3170: multicast deployment", b"RFC 868: status check"):
        code, report = _post_publish(hub_url, "museum/rfc", text)
        print("публикация «%s» → HTTP %s, доставлено %d" % (text.decode(), code, len(report["delivered"])))

    ok = len(received) == 2
    print("\nитог: доставок %d из 2 — %s" % (len(received), "работает" if ok else "НЕ работает"))
    callback.shutdown()
    server.shutdown()
    return 0 if ok else 1


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="hubd", description="хаб подписок + сканер источников")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("serve", help="запустить хаб")
    p.add_argument("--store", default="data/hub.json")
    p.add_argument("--journal", default="data/journal.jsonl")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8765)
    p.set_defaults(func=cmd_serve)

    p = sub.add_parser("scan", help="следить за источниками и публиковать изменения")
    p.add_argument("--watchlist", required=True)
    p.add_argument("--state", default="data/scan.json")
    p.add_argument("--hub", default="http://127.0.0.1:8765")
    p.add_argument("--interval", type=float, default=1.0, help="пауза между запросами к одному хосту")
    p.add_argument("--every", type=float, default=300.0, help="пауза между обходами")
    p.add_argument("--once", action="store_true")
    p.set_defaults(func=cmd_scan)

    p = sub.add_parser("check", help="обойти источники без публикации")
    p.add_argument("--watchlist", required=True)
    p.add_argument("--state", default="data/scan.json")
    p.add_argument("--interval", type=float, default=1.0)
    p.set_defaults(func=cmd_check)

    p = sub.add_parser("subs", help="показать подписки")
    p.add_argument("--hub", default="http://127.0.0.1:8765")
    p.set_defaults(func=cmd_subs)

    p = sub.add_parser("publish", help="опубликовать в тему")
    p.add_argument("--hub", default="http://127.0.0.1:8765")
    p.add_argument("--topic", required=True)
    p.add_argument("--text")
    p.set_defaults(func=cmd_publish)

    p = sub.add_parser("demo", help="живая демонстрация на loopback")
    p.add_argument("--workdir", default=".")
    p.set_defaults(func=cmd_demo)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
