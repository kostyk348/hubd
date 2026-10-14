"""Сканер источников: conditional GET, дедупликация, вежливость 1 запрос/сек на хост, публикация в хаб.

Зачем: даже у WebSub-идеи на входе почти всегда поллинг фида. Здесь поллинг спрятан в одном месте,
сделан вежливо (ETag/If-Modified-Since, пауза между запросами к одному хосту) и превращён в события
для хаба: изменилось — публикуем, не изменилось — молчим (молчание дешевле трафика).
"""
from __future__ import annotations

import hashlib
import json
import os
import tempfile
import time
import urllib.error
import urllib.request
from urllib.parse import urlparse


def default_fetcher(url: str, headers: dict[str, str]) -> tuple[int, dict[str, str], bytes]:
    """Возвращает (код, заголовки, тело). 304 — если источник не менялся."""
    req = urllib.request.Request(url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, dict(resp.headers), resp.read()
    except urllib.error.HTTPError as exc:
        return exc.code, dict(exc.headers or {}), exc.read() if exc.fp else b""


class Watcher:
    """Следит за источниками и говорит, что изменилось."""

    def __init__(self, state_path: str, fetcher=default_fetcher, clock=time.time,
                 min_interval: float = 1.0, user_agent: str = "hubd/0.1 (+museum)"):
        self.state_path = state_path
        self.fetcher = fetcher
        self.clock = clock
        self.min_interval = min_interval
        self.user_agent = user_agent
        self.state: dict[str, dict] = {}
        self.last_suppressed: list[dict] = []
        self.last_errors: list[dict] = []
        self._last_host_hit: dict[str, float] = {}
        self.load()

    # --- состояние ---------------------------------------------------------------
    def load(self) -> None:
        if os.path.exists(self.state_path):
            with open(self.state_path, encoding="utf-8") as fh:
                self.state = json.load(fh).get("sources", {})

    def save(self) -> None:
        directory = os.path.dirname(os.path.abspath(self.state_path))
        os.makedirs(directory, exist_ok=True)
        payload = {"saved": self.clock(), "sources": self.state}
        fd, tmp = tempfile.mkstemp(dir=directory)
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=1)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, self.state_path)

    def wait_for_host(self, url: str) -> float:
        """Вежливость: не чаще min_interval секунд к одному хосту."""
        host = urlparse(url).netloc
        now = self.clock()
        previous = self._last_host_hit.get(host)
        waited = 0.0
        if previous is not None:
            pause = self.min_interval - (now - previous)
            if pause > 0:
                waited = pause
                time.sleep(pause)
        self._last_host_hit[host] = self.clock()
        return waited

    def check(self, url: str) -> dict:
        """Один источник: условный GET, сверка с отпечатком содержимого.

        Сбой источника (сеть, 5xx, 404) — это результат обхода, а не повод уронить весь скан:
        он записывается в состояние и возвращается вызывающему как ошибка источника.
        """
        entry = self.state.setdefault(url, {"hash": None, "checks": 0, "changes": 0})
        headers = {"User-Agent": self.user_agent}
        if entry.get("etag"):
            headers["If-None-Match"] = entry["etag"]
        if entry.get("last_modified"):
            headers["If-Modified-Since"] = entry["last_modified"]

        waited = self.wait_for_host(url)
        try:
            status, resp_headers, body = self.fetcher(url, headers)
        except Exception as exc:                      # noqa: BLE001 — источник не роняет обход
            status, resp_headers, body = 0, {}, b""
            network_error = "%s: %s" % (type(exc).__name__, exc)
        else:
            network_error = ""
        entry["checks"] += 1
        entry["checked"] = self.clock()
        entry["waited"] = round(waited, 3)

        if status == 304:
            entry["status"] = 304
            entry["error"] = ""
            self.save()
            return {"url": url, "changed": False, "status": 304, "reason": "not modified"}

        if status != 200:
            error = "источник недоступен (%s)" % network_error if status == 0 else "HTTP %s" % status
            entry["status"] = status
            entry["error"] = error
            self.save()
            return {"url": url, "changed": False, "status": status, "error": error}

        digest = hashlib.sha256(body).hexdigest()
        first_time = entry.get("hash") is None
        changed = (not first_time) and entry.get("hash") != digest
        validators = bool(resp_headers.get("ETag") or resp_headers.get("Last-Modified")
                          or resp_headers.get("etag") or resp_headers.get("last-modified"))
        entry.update({
            "status": 200,
            "hash": digest,
            "bytes": len(body),
            "validators": validators,
            "etag": resp_headers.get("ETag", resp_headers.get("etag", "")),
            "last_modified": resp_headers.get("Last-Modified", resp_headers.get("last-modified", "")),
            "error": "",
        })
        if changed:
            entry["changes"] += 1
        self.save()
        return {
            "url": url,
            "changed": changed,
            "status": 200,
            "bytes": len(body),
            "hash": digest,
            "first_seen": first_time,
            "validators": validators,
            "body": body,
        }

    def scan(self, watchlist: list[dict]) -> list[dict]:
        """Обходит список наблюдения. Возвращает только то, что стоит публиковать.

        Источник без валидаторов (ни ETag, ни Last-Modified) считается «болтливым»: его страница
        меняется на каждый запрос (счётчики, даты), и событие «изменилось» ничего не значит.
        С такого источника события подавляются, пока в списке наблюдения не сказано
        `"allow_volatile": true` — это осознанное решение, а не молчаливая потеря события.
        """
        events = []
        self.last_suppressed = []
        self.last_errors = []
        for item in watchlist:
            url = item["url"]
            result = self.check(url)
            if result.get("error"):
                self.last_errors.append({"url": url, "topic": item.get("topic", url),
                                         "status": result["status"], "error": result["error"]})
            if not result["changed"] or result.get("first_seen"):
                continue                      # первый раз — это знакомство, а не новость
            if not result.get("validators") and not item.get("allow_volatile"):
                self.last_suppressed.append({
                    "url": url, "topic": item.get("topic", url),
                    "reason": "источник без ETag/Last-Modified: события подавлены (нужен allow_volatile)",
                })
                continue
            events.append({
                "topic": item.get("topic", url),
                "url": url,
                "payload": result["body"],
                "hash": result["hash"],
                "status": result["status"],
            })
        return events


def load_watchlist(path: str) -> list[dict]:
    with open(path, encoding="utf-8") as fh:
        data = json.load(fh)
    if isinstance(data, list):
        return data
    return data.get("sources", [])
