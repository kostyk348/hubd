"""CLI: hubd serve | scan | check | subs | publish | retry | candidates | watchlist | demo.

    python -m hubd serve    --store data/hub.json --port 8765
    python -m hubd scan     --watchlist watchlist.json --state data/scan.json \
                            --queue data/candidates.jsonl
    python -m hubd scan     --watchlist watchlist.json --queue data/candidates.jsonl \
                            --publish --hub http://127.0.0.1:8765     # доставка — явный флаг
    python -m hubd check    --watchlist watchlist.json --state data/scan.json
    python -m hubd subs     --hub http://127.0.0.1:8765
    python -m hubd publish  --hub http://127.0.0.1:8765 --topic rfc/rfc3170/status --text "понижен"
    python -m hubd retry    --hub http://127.0.0.1:8765
    python -m hubd candidates --file data/candidates.jsonl --status new
    python -m hubd candidates --file data/candidates.jsonl --reject <id> --reason "наша же публикация"
    python -m hubd watchlist  --museum ../lost-tech-museum --out watchlist.json
    python -m hubd demo                        # хаб + подписчики + очередь повторов, всё на loopback
"""
from __future__ import annotations

import argparse
import http.client
import json
import os
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .candidates import Candidates
from .hub import Hub, build_server
from .scan import Watcher, load_watchlist
from .watchlist import from_museum, write as write_watchlist


def _network_cause(exc: BaseException) -> str:
    """Причина сетевого сбоя по-человечески: хаб может быть не запущен — это штатное состояние."""
    cause = getattr(exc, "reason", exc)          # URLError несёт настоящую причину в .reason
    if not isinstance(cause, BaseException):
        return str(cause)
    if isinstance(cause, http.client.RemoteDisconnected):
        return "порт отвечает, но это не HTTP-хаб (соединение закрыто без ответа)"
    if isinstance(cause, http.client.BadStatusLine):
        return "порт отвечает, но это не HTTP-хаб (ответ не по HTTP)"
    if isinstance(cause, http.client.IncompleteRead):
        return "ответ оборвался на середине"
    if isinstance(cause, ConnectionRefusedError):
        return "соединение отклонено"
    if isinstance(cause, TimeoutError):
        return "нет ответа за отведённое время"
    if isinstance(cause, ConnectionResetError):
        return "соединение сброшено"
    return "%s: %s" % (type(cause).__name__, cause)


class HubUnavailable(Exception):
    """Хаб не ответил по сети. Причина — строкой, чтобы команда напечатала её, а не traceback."""


def _http_call(req: urllib.request.Request, timeout: float) -> tuple[int, bytes]:
    """Один HTTP-вызов: (код, тело). Сетевой сбой — HubUnavailable, а не падение процесса."""
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as exc:        # хаб ответил кодом ошибки — это данные, не сбой
        try:
            body = exc.read()
        except (http.client.HTTPException, OSError) as read_exc:
            raise HubUnavailable("хаб недоступен: %s" % _network_cause(read_exc)) from read_exc
        finally:
            exc.close()
        return exc.code, body
    except (urllib.error.URLError, http.client.HTTPException, OSError, ValueError) as exc:
        raise HubUnavailable("хаб недоступен: %s" % _network_cause(exc)) from exc


def _json_payload(code: int, body: bytes) -> dict:
    try:
        data = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        return {"error": "хаб ответил не JSON (код %d, %s): %r" % (
            code, type(exc).__name__, body[:120])}
    if isinstance(data, dict):
        return data
    return {"body": data}


def _call_json(req: urllib.request.Request, timeout: float) -> tuple[int, dict]:
    try:
        code, body = _http_call(req, timeout)
    except HubUnavailable as exc:
        return 0, {"error": str(exc)}
    return code, _json_payload(code, body)


def _post_form(url: str, form: dict) -> tuple[int, dict]:
    data = urllib.parse.urlencode(form).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST",
                                 headers={"Content-Type": "application/x-www-form-urlencoded"})
    return _call_json(req, 15)


def _post_publish(hub_url: str, topic: str, body: bytes) -> tuple[int, dict]:
    url = "%s/publish?topic=%s" % (hub_url.rstrip("/"), urllib.parse.quote(topic))
    req = urllib.request.Request(url, data=body, method="POST",
                                 headers={"Content-Type": "application/octet-stream"})
    return _call_json(req, 15)


def _get_json(url: str) -> dict:
    try:
        code, body = _http_call(urllib.request.Request(url, method="GET"), 15)
    except HubUnavailable as exc:
        return {"error": str(exc)}
    return _json_payload(code, body)


def _post_json(url: str) -> dict:
    req = urllib.request.Request(url, data=b"", method="POST",
                                 headers={"Content-Type": "application/octet-stream"})
    try:
        code, body = _http_call(req, 60)
    except HubUnavailable as exc:
        return {"error": str(exc)}
    return _json_payload(code, body)


def _report_line(report: dict) -> str:
    return ("попыток %d, доставлено %d, ждут повтора %d, сдались %d, брошено %d, "
            "не подтверждено всего %d" % (
                report.get("attempted", 0), len(report.get("delivered", [])),
                len(report.get("still_pending", [])), len(report.get("gave_up", [])),
                len(report.get("dropped", [])), report.get("unconfirmed", 0)))


# --- хаб --------------------------------------------------------------------------
def _retry_loop(hub: Hub, interval: float, stop: threading.Event) -> None:
    """Проходка по очереди неподтверждённых доставок, пока живёт хаб."""
    while not stop.wait(interval):
        try:
            report = hub.retry()
        except Exception as exc:                          # noqa: BLE001 — цикл не должен падать
            print("повтор: сбой проходки: %s" % exc)
            continue
        if report["attempted"] or report["dropped"]:
            print("повтор: %s" % _report_line(report))


def cmd_serve(args) -> int:
    policy = None
    if args.topic_prefix:
        prefix = args.topic_prefix

        def decide(topic: str) -> tuple[bool, str]:
            if topic.startswith(prefix):
                return True, ""
            return False, "хаб обслуживает только темы с префиксом %r" % prefix

        policy = decide
    hub = Hub(args.store, journal_path=args.journal, topic_policy=policy)
    server = build_server(hub, args.host, args.port)
    host, port = server.server_address[:2]
    stop = threading.Event()
    if args.retry_interval > 0:
        threading.Thread(target=_retry_loop, args=(hub, args.retry_interval, stop),
                         daemon=True).start()
    print("хаб слушает http://%s:%d  (store: %s)" % (host, port, args.store))
    print("подписок: %d, не подтверждено доставок: %d, проходка по очереди: %s" % (
        len(hub.store.as_list()), hub.store.unconfirmed(),
        "каждые %gs" % args.retry_interval if args.retry_interval > 0 else "выключена"))
    if args.topic_prefix:
        print("политика тем: только с префиксом %r (остальным — hub.mode=denied)" % args.topic_prefix)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nостановлено")
    finally:
        stop.set()
    return 0


# --- сканер -----------------------------------------------------------------------
def cmd_scan(args) -> int:
    watcher = Watcher(args.state, min_interval=args.interval)
    watchlist = load_watchlist(args.watchlist)
    queue = Candidates(args.queue) if args.queue else None
    publish = bool(args.publish and args.hub.strip())
    if args.publish and not publish:
        print("публикация выключена: --hub пуст")
    if queue is not None:
        counts = queue.counts()
        print("очередь кандидатов: %s (новых %d, принято %d, отклонено %d)" % (
            args.queue, counts.get("new", 0), counts.get("accepted", 0), counts.get("rejected", 0)))
    print("публикация в хаб: %s" % (
        "включена, %s" % args.hub if publish else
        "выключена (собираем кандидатов; --publish, когда хаб поднят)"))
    while True:
        events = watcher.scan(watchlist)
        if events:
            fresh = dupes = 0
            accepted = unreachable = 0
            for event in events:
                if queue is not None:
                    if queue.add(topic=event["topic"], url=event["url"],
                                 digest=event["hash"], body=event["payload"]) is None:
                        dupes += 1
                    else:
                        fresh += 1
                if not publish:
                    print("изменился %s → кандидат в очереди, публикация выключена" % event["url"])
                    continue
                code, report = _post_publish(args.hub, event["topic"], event["payload"])
                if code == 0:
                    unreachable += 1
                    print("изменился %s → публикация не удалась: %s" % (
                        event["url"], report.get("error", "хаб недоступен")))
                    continue
                accepted += 1
                print("изменился %s → публикация %s: %s" % (
                    event["url"], code, json.dumps(report, ensure_ascii=False)[:200]))
            if queue is not None:
                print("кандидатов новых: %d, уже были в очереди: %d (всего %d)" % (
                    fresh, dupes, len(queue.all())))
            if publish:
                print("публикаций: хаб принял %d, недоступен %d" % (accepted, unreachable))
        else:
            print("изменений нет (%d источников)" % len(watchlist))
        for error in watcher.last_errors:
            print("источник недоступен: %s — %s" % (error["url"], error["error"]))
        for suppressed in watcher.last_suppressed:
            print("событие подавлено: %s — %s" % (suppressed["url"], suppressed["reason"]))
        if args.once:
            return 0
        time.sleep(args.every)


def cmd_check(args) -> int:
    watcher = Watcher(args.state, min_interval=args.interval)
    failures = []
    for item in load_watchlist(args.watchlist):
        result = watcher.check(item["url"])
        if result.get("error"):
            failures.append((item["url"], result["error"]))
        print("%-4s %-13s %s" % (result["status"], "изменён" if result["changed"] else "без изменений",
                                 item["url"]))
    if failures:
        print("\nнедоступных источников: %d" % len(failures))
        for url, error in failures:
            print("  %s — %s" % (url, error))
    return 1 if (failures and args.fail_on_error) else 0


# --- подписки ---------------------------------------------------------------------
def cmd_subs(args) -> int:
    data = _get_json(args.hub.rstrip("/") + "/subscriptions")
    if data.get("error"):
        print("%s — повторите после запуска хаба" % data["error"])
        return 0
    subs = data.get("subscriptions", [])
    if not subs:
        print("подписок нет")
        return 0
    for sub in subs:
        print("%-16s %-28s %s  аренда %ss, доставок %d, провалов %d, в очереди %d%s" % (
            sub["id"], sub["topic"][:28], sub["callback"], sub["expires_in"],
            sub["deliveries"], sub["failures"], sub.get("pending", 0),
            "  DEGRADED: %s" % sub["degraded_reason"] if sub.get("degraded") else ""))
    print("\nне подтверждено доставок: %d" % data.get("unconfirmed", 0))
    return 0


def cmd_publish(args) -> int:
    body = args.text.encode("utf-8") if args.text else sys.stdin.buffer.read()
    code, report = _post_publish(args.hub, args.topic, body)
    if code == 0:
        print("%s — ничего не опубликовано, повторите после запуска хаба"
              % report.get("error", "хаб недоступен"))
        return 1
    print("HTTP %s %s" % (code, json.dumps(report, ensure_ascii=False, indent=1)))
    if code == 202:
        print("принято, но не подтверждено: хаб повторит доставку по очереди "
              "(не подтверждено всего %d)" % report.get("unconfirmed", 0))
    elif code == 502:
        print("доставить не смогли и повторить не сможем: %s" % report.get("not_queued"))
    elif code == 503:
        print("подписчики нарушили контракт доставки: %s" % report.get("refused"))
    return 0 if code in (200, 202) else 1


def cmd_retry(args) -> int:
    report = _post_json(args.hub.rstrip("/") + "/retry")
    if report.get("error"):
        print("%s — повторите после запуска хаба" % report["error"])
        return 0
    for row in report["still_pending"]:
        print("ждёт повтора: %s %s, попыток %d, через %ss, последняя ошибка: %s" % (
            row["id"], row["topic"], row["attempts"], row["next_in"], row.get("reason", "")))
    for row in report["gave_up"]:
        print("отказ: %s %s — %s" % (row["id"], row["topic"], row["reason"]))
    for row in report["dropped"]:
        print("брошено: %s %s — %s" % (row["key"], row["topic"], row["reason"]))
    print(_report_line(report))
    return 1 if report["gave_up"] else 0


# --- очередь кандидатов музея -----------------------------------------------------
DECISION_WORDS = {"accepted": "принят", "rejected": "отклонён"}


def _print_decision(path: str, key: str, status: str, result: dict | None) -> int:
    """Печатает исход решения триажа. Код 0 — решение в силе, 1 — кандидата нет."""
    word = DECISION_WORDS[status]
    if result is None:
        print("кандидат %s не найден в очереди %s" % (key, path))
        return 1
    reason = result["reason"]
    if not result["changed"]:
        print("кандидат %s уже %s — файл не менялся%s" % (
            key, word, ", причина: «%s»" % reason if reason else ""))
        return 0
    if result["repeat"]:
        print("кандидат %s уже %s, причина обновлена: «%s»" % (key, word, reason))
        return 0
    if reason and not result.get("carried"):
        print("%s кандидат %s (был %s), причина: «%s»" % (
            word, key, result["previous"], reason))
    elif reason:
        print("%s кандидат %s (был %s), причина не задана — прежняя запись строки сохранена: "
              "«%s»" % (word, key, result["previous"], reason))
    else:
        print("%s кандидат %s (был %s), без причины" % (word, key, result["previous"]))
    return 0


def cmd_candidates(args) -> int:
    queue = Candidates(args.file)
    if args.reason and not (args.accept or args.reject):
        print("ошибка: --reason без решения — причина объясняет --accept или --reject")
        return 2
    code = 0
    for key, status in ((args.accept, "accepted"), (args.reject, "rejected")):
        if not key:
            continue
        code = max(code, _print_decision(args.file, key, status,
                                         queue.decide(key, status, args.reason)))
    counts = queue.counts()
    print("очередь %s: всего %d — новых %d, принято %d, отклонено %d" % (
        args.file, len(queue.all()), counts.get("new", 0), counts.get("accepted", 0),
        counts.get("rejected", 0)))
    rows = queue.by_status(args.status) if args.status else queue.all()
    for row in rows[-args.limit:]:
        print("  %s  %-9s %-30s %s" % (row["key"], row.get("status"), row["topic"], row["url"]))
        if row.get("note"):
            print("      причина: %s" % row["note"].replace("\n", " ")[:200])
        if row.get("sample") and args.show_sample:
            print("      %s" % row["sample"].replace("\n", " ")[:160])
    return code


def cmd_watchlist(args) -> int:
    try:
        sources, skipped = from_museum(args.museum, slugs=args.slug or None,
                                       include_all_hosts=args.all_hosts)
    except FileNotFoundError as exc:
        print("ошибка: %s" % exc)
        return 1
    write_watchlist(args.out, sources, args.museum)
    urls = {source["url"] for source in sources}
    print("источников: %d (уникальных адресов: %d) → %s" % (len(sources), len(urls), args.out))
    topics = {}
    for source in sources:
        head = source["topic"].split("/")[0]
        topics[head] = topics.get(head, 0) + 1
    print("по темам: %s" % ", ".join("%s=%d" % item for item in sorted(topics.items())))
    volatile = [s for s in sources if s.get("allow_volatile")]
    if volatile:
        print("без валидаторов (allow_volatile, события не подавляются): %d" % len(volatile))
    if skipped:
        print("пропущено источников: %d — хосты не из архивных (нужен --all-hosts)" % len(skipped))
        for row in skipped[:5]:
            print("  %s (%s)" % (row["url"], row["host"]))
    return 0


# --- живая демонстрация -----------------------------------------------------------
def cmd_demo(args) -> int:
    """Живая демонстрация: хаб + подписчики + очередь повторов. Всё на 127.0.0.1."""
    workdir = args.workdir or tempfile.mkdtemp(prefix="hubd-demo-")
    os.makedirs(workdir, exist_ok=True)
    received: list[tuple[str, bytes]] = []
    controls: dict[str, dict] = {}
    checks: list[tuple[str, bool]] = []

    def check(label: str, ok: bool) -> None:
        checks.append((label, ok))
        print("  %s %s" % ("✓" if ok else "✗", label))

    def make_callback(name: str, failing: bool):
        controls[name] = {"fail": failing}
        control = controls[name]

        class Callback(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, format, *args):
                pass

            def do_GET(self):
                query = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
                mode = (query.get("hub.mode") or [""])[0]
                if mode == "denied":
                    print("  ← %s: хаб отказал (hub.mode=denied): %s" % (
                        name, (query.get("hub.reason") or [""])[0]))
                body = ((query.get("hub.challenge") or [""])[0]).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_POST(self):
                length = int(self.headers.get("Content-Length", "0") or 0)
                payload = self.rfile.read(length)
                code = 500 if control["fail"] else 204
                if code == 204:
                    received.append((name, payload))
                print("  ← %s: доставка %d байт, подпись: %s, ответ %d" % (
                    name, len(payload), self.headers.get("X-Hub-Signature", "нет"), code))
                self.send_response(code)
                self.send_header("Content-Length", "0")
                self.end_headers()

        server = ThreadingHTTPServer(("127.0.0.1", 0), Callback)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        return server, "http://127.0.0.1:%d/cb" % server.server_address[1]

    good, good_url = make_callback("приёмник", failing=False)
    flaky, flaky_url = make_callback("ломающийся", failing=True)

    def deny_private(topic: str) -> tuple[bool, str]:
        return (topic.startswith("museum/"), "хаб обслуживает только темы museum/*")

    hub = Hub(os.path.join(workdir, "demo-hub.json"),
              journal_path=os.path.join(workdir, "demo-hub.log"), topic_policy=deny_private)
    server = build_server(hub, "127.0.0.1", 0)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    hub_url = "http://127.0.0.1:%d" % server.server_address[1]
    print("рабочая папка: %s" % workdir)
    print("хаб: %s, подписчики: %s и %s" % (hub_url, good_url, flaky_url))

    print("\n1. политика хаба: подписка на частную тему")
    code, payload = _post_form(hub_url + "/subscribe", {
        "hub.mode": "subscribe", "hub.topic": "private/rfc", "hub.callback": good_url,
        "hub.lease_seconds": 3600})
    print("подписка на private/rfc: HTTP %s %s" % (code, json.dumps(payload, ensure_ascii=False)))
    check("подписка вне политики отклонена (403)", code == 403)
    check("подписчик уведомлён через hub.mode=denied", bool(payload.get("callback_notified")))
    check("подписка не сохранена", hub.store.as_list() == [])

    print("\n2. подписка с секретом (проверка намерения challenge'ом)")
    code, payload = _post_form(hub_url + "/subscribe", {
        "hub.mode": "subscribe", "hub.topic": "museum/rfc", "hub.callback": good_url,
        "hub.lease_seconds": 3600, "hub.secret": "s3cret"})
    print("подписка: HTTP %s %s" % (code, json.dumps(payload, ensure_ascii=False)))
    check("подписка принята (202)", code == 202)

    print("\n3. публикация в тему — доставка с подписью")
    code, report = _post_publish(hub_url, "museum/rfc", b"RFC 3170: multicast deployment")
    print("публикация → HTTP %s, доставлено %d" % (code, len(report["delivered"])))
    check("доставлено подписчику с HMAC-подписью", code == 200 and len(received) == 1)

    print("\n4. ломающийся подписчик: доставка не подтверждена → очередь")
    code, payload = _post_form(hub_url + "/subscribe", {
        "hub.mode": "subscribe", "hub.topic": "museum/rfc", "hub.callback": flaky_url,
        "hub.lease_seconds": 3600, "hub.secret": "s3cret"})
    check("вторая подписка принята", code == 202)
    code, report = _post_publish(hub_url, "museum/rfc", b"RFC 868: status check")
    print("публикация → HTTP %s, доставлено %d, в очереди %d" % (
        code, len(report["delivered"]), len(report["queued"])))
    check("неполная доставка не выдаётся за успех (202)", code == 202)
    check("неподтверждённая доставка в очереди", report["unconfirmed"] == 1)
    wait = (report["queued"][0]["next_in"] + 1) if report["queued"] else 0

    print("\n5. подписчик починился; через %ss пауза истекла — проходка по очереди" % wait)
    controls["ломающийся"]["fail"] = False
    time.sleep(wait)
    report = _post_json(hub_url + "/retry")
    print("повтор: %s" % _report_line(report))
    check("повторная доставка подтверждена", len(report["delivered"]) == 1)
    check("очередь пуста", report["unconfirmed"] == 0)
    check("всего подтверждённых доставок: 3", len(received) == 3)

    good.shutdown()
    flaky.shutdown()
    server.shutdown()
    failed = [label for label, ok in checks if not ok]
    print("\nитог: проверок %d, провалено %d — %s" % (
        len(checks), len(failed), "работает" if not failed else "НЕ работает: %s" % "; ".join(failed)))
    return 0 if not failed else 1


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="hubd", description="хаб подписок + сканер источников")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("serve", help="запустить хаб")
    p.add_argument("--store", default="data/hub.json")
    p.add_argument("--journal", default="data/journal.jsonl")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8765)
    p.add_argument("--topic-prefix", default="",
                   help="обслуживать только темы с этим префиксом (остальным хаб отказывает)")
    p.add_argument("--retry-interval", type=float, default=30.0,
                   help="как часто проходить очередь неподтверждённых доставок (0 — выключить)")
    p.set_defaults(func=cmd_serve)

    p = sub.add_parser("scan", help="следить за источниками и складывать изменения в очередь кандидатов")
    p.add_argument("--watchlist", required=True)
    p.add_argument("--state", default="data/scan.json")
    p.add_argument("--queue", default="", help="файл очереди кандидатов (JSONL), например data/candidates.jsonl")
    p.add_argument("--hub", default="http://127.0.0.1:8765",
                   help="адрес хаба; используется только вместе с --publish")
    p.add_argument("--publish", action="store_true",
                   help="публиковать изменения в хаб (по умолчанию выключено: обход только собирает "
                        "кандидатов и завершается успешно, даже если хаб не запущен)")
    p.add_argument("--interval", type=float, default=1.0, help="пауза между запросами к одному хосту")
    p.add_argument("--every", type=float, default=300.0, help="пауза между обходами")
    p.add_argument("--once", action="store_true")
    p.set_defaults(func=cmd_scan)

    p = sub.add_parser("check", help="обойти источники без публикации")
    p.add_argument("--watchlist", required=True)
    p.add_argument("--state", default="data/scan.json")
    p.add_argument("--interval", type=float, default=1.0)
    p.add_argument("--fail-on-error", action="store_true",
                   help="вернуть 1, если хоть один источник недоступен (для cron)")
    p.set_defaults(func=cmd_check)

    p = sub.add_parser("subs", help="показать подписки")
    p.add_argument("--hub", default="http://127.0.0.1:8765")
    p.set_defaults(func=cmd_subs)

    p = sub.add_parser("publish", help="опубликовать в тему")
    p.add_argument("--hub", default="http://127.0.0.1:8765")
    p.add_argument("--topic", required=True)
    p.add_argument("--text")
    p.set_defaults(func=cmd_publish)

    p = sub.add_parser("retry", help="одна проходка по очереди неподтверждённых доставок")
    p.add_argument("--hub", default="http://127.0.0.1:8765")
    p.set_defaults(func=cmd_retry)

    p = sub.add_parser("candidates", help="очередь кандидатов музея (что сканер увидел)")
    p.add_argument("--file", default="data/candidates.jsonl")
    p.add_argument("--status", choices=["new", "accepted", "rejected"], default="")
    p.add_argument("--limit", type=int, default=20)
    p.add_argument("--show-sample", action="store_true")
    p.add_argument("--accept", default="", help="id кандидата, который триаж принял")
    p.add_argument("--reject", default="", help="id кандидата, который триаж отклонил")
    p.add_argument("--reason", default="",
                   help="причина решения: ложится в строку очереди рядом со статусом")
    p.set_defaults(func=cmd_candidates)

    p = sub.add_parser("watchlist", help="собрать список наблюдения из архива музея")
    p.add_argument("--museum", required=True, help="путь к репозиторию lost-tech-museum (только чтение)")
    p.add_argument("--out", default="watchlist.json")
    p.add_argument("--slug", action="append", help="взять только эти датасеты (по умолчанию все)")
    p.add_argument("--all-hosts", action="store_true",
                   help="включить и не архивные хосты (новости, вики)")
    p.set_defaults(func=cmd_watchlist)

    p = sub.add_parser("demo", help="живая демонстрация на loopback")
    p.add_argument("--workdir", default="")
    p.set_defaults(func=cmd_demo)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
