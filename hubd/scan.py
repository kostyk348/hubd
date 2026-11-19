"""Сканер источников: conditional GET, дедупликация, вежливость 1 запрос/сек на хост, публикация в хаб.

Зачем: даже у WebSub-идеи на входе почти всегда поллинг фида. Здесь поллинг спрятан в одном месте,
сделан вежливо (ETag/If-Modified-Since, пауза между запросами к одному хосту) и превращён в события
для хаба: изменилось — публикуем, не изменилось — молчим (молчание дешевле трафика).
"""
from __future__ import annotations

import difflib
import gzip
import hashlib
import http.client
import json
import os
import tempfile
import time
import urllib.error
import urllib.request
from urllib.parse import urlparse

from .adapters import AdapterError, run_adapter


def default_fetcher(url: str, headers: dict[str, str], attempts: int = 3) -> tuple[int, dict[str, str], bytes]:
    """Возвращает (код, заголовки, тело). 304 — если источник не менялся.

    Транзиентные обрывы повторяются: на этой машине TLS-соединения к крупным архивам
    рвутся на середине тела (`IncompleteRead` на 13 590 773 байтах у rfc-index.xml,
    13,8 МБ). Первая поправка была «три повтора с нуля», но Архивесса верно указала цену:
    на 13,7 МБ это до 41 МБ трафика за один неудачный обход. Поэтому при обрыве остаток
    дочитывается по `Range: bytes=<прочитано>-` — сервер, который Range поддерживает,
    отдаёт 206, и склейка бесплатна; сервер, который его игнорирует (rfc-editor отдаёт
    200 с полным телом — проверено), просто присылает тело заново, и мы берём его целиком.
    HTTP-ошибки (403/404/429) не повторяются: это ответ сервера, а не сеть.
    """
    last_error: Exception | None = None
    partial = b""
    for attempt in range(attempts):
        request_headers = dict(headers)
        if partial:
            request_headers["Range"] = "bytes=%d-" % len(partial)
        req = urllib.request.Request(url, headers=request_headers, method="GET")
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                status = resp.status
                try:
                    body = resp.read()
                except http.client.IncompleteRead as exc:
                    # Что именно прочитано — знает только ответ. Если сервер отдал 206,
                    # куски склеиваются; если он Range игнорирует (200, rfc-editor так
                    # и делает), начинать надо заново, иначе префикс продублируется
                    # и в реестр попадёт мусорный отпечаток.
                    partial = (partial + (exc.partial or b"")) if status == 206 else (exc.partial or b"")
                    raise
                if partial and status == 206:
                    return 200, dict(resp.headers), partial + body
                return status, dict(resp.headers), body
        except urllib.error.HTTPError as exc:
            return exc.code, dict(exc.headers or {}), exc.read() if exc.fp else b""
        except http.client.IncompleteRead as exc:
            partial += exc.partial or b""
            last_error = exc
            if attempt + 1 < attempts:
                time.sleep(1.0)
        except (urllib.error.URLError, ConnectionError, TimeoutError, OSError) as exc:
            last_error = exc
            if attempt + 1 < attempts:
                time.sleep(1.5 * (attempt + 1))
    raise last_error if last_error else RuntimeError("fetch failed without an error")


class Watcher:
    """Следит за источниками и говорит, что изменилось."""

    #: сколько строк/символов диффа показывать триажу (реестр на 13,8 МБ иначе утопит очередь)
    DIFF_LINES = 80
    DIFF_CHARS = 6000

    def __init__(self, state_path: str, fetcher=default_fetcher, clock=time.time,
                 min_interval: float = 1.0, user_agent: str = "hubd/1.0 (+universal)"):
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

    # --- тела реестров (store_body) ----------------------------------------------
    def _body_path(self, url: str) -> str:
        name = hashlib.sha1(url.encode("utf-8")).hexdigest()[:16] + ".body.gz"
        return os.path.join(os.path.dirname(os.path.abspath(self.state_path)), "bodies", name)

    def read_body(self, url: str) -> bytes | None:
        """Прошлое тело источника, если его просили хранить."""
        path = self._body_path(url)
        if not os.path.exists(path):
            return None
        try:
            with gzip.open(path, "rb") as fh:
                return fh.read()
        except (OSError, EOFError):
            return None                     # битый архив — считаем, что прошлого тела нет

    def write_body(self, url: str, body: bytes) -> None:
        path = self._body_path(url)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = path + ".tmp"
        with gzip.open(tmp, "wb") as fh:
            fh.write(body)
        os.replace(tmp, path)               # атомарно: половина тела не должна попасть в базу

    def diff(self, previous: bytes, current: bytes) -> str:
        """Дифф прошлого и нового тела — то, что триаж увидит вместо первых 300 байт.

        Каждый байт реестра превращать в строки незачем: диффим текстовые строки,
        а результат режем по строкам и символам — очередь кандидатов не должна
        распухать от 13,8-мегабайтного индекса (Архивесса права: без диффа кандидат
        говорит «файл изменился, разбирайся сам»).
        """
        try:
            old_lines = previous.decode("utf-8", "replace").splitlines(keepends=True)
            new_lines = current.decode("utf-8", "replace").splitlines(keepends=True)
        except Exception:                                    # noqa: BLE001 — дифф не критичен
            return ""
        chunks = list(difflib.unified_diff(old_lines, new_lines, fromfile="было", tofile="стало",
                                           n=1, lineterm=""))
        text = "\n".join(chunks[: self.DIFF_LINES])
        return text[: self.DIFF_CHARS]

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

    def check(self, url: str, store_body: bool = False) -> dict:
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
        if store_body and self.read_body(url) is None:
            # Базовая съёмка: без неё первое же изменение реестра придёт кандидатом
            # без диффа — прошлого тела просто нет. Один раз берём полное тело,
            # сняв условные заголовки; отпечаток при этом не меняется и событие
            # «изменилось» не возникнет зря.
            headers.pop("If-None-Match", None)
            headers.pop("If-Modified-Since", None)

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
        diff_text = ""
        if store_body:
            # Прошлое тело нужно ДО перезаписи: иначе диффать будет не с чем и триаж
            # получит «файл изменился» вместо «строка X: Internet Standard -> Historic».
            previous = self.read_body(url)
            if changed and previous is not None:
                diff_text = self.diff(previous, body)
            self.write_body(url, body)
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
            "diff": diff_text,
        }

    def scan(self, watchlist: list[dict], plugins: dict | None = None) -> list[dict]:
        """Обходит список наблюдения. Возвращает только то, что стоит публиковать.

        Источник без валидаторов (ни ETag, ни Last-Modified) считается «болтливым»: его страница
        меняется на каждый запрос (счётчики, даты), и событие «изменилось» ничего не значит.
        С такого источника события подавляются, пока в списке наблюдения не сказано
        `"allow_volatile": true` — это осознанное решение, а не молчаливая потеря события.

        Что именно изменение значит, решает адаптер источника (adapters.py): plain
        публикует тело целиком, лента — новые записи, json — значение по пути.
        plugins — загруженные плагины (plugins.py). Ошибка адаптера или плагина на
        конкретном источнике — видимая ошибка этого источника, а не падение обхода.
        """
        events = []
        self.last_suppressed = []
        self.last_errors = []
        self._adapter_dirty = False
        for item in watchlist:
            url = item["url"]
            result = self.check(url, store_body=bool(item.get("store_body")))
            if result.get("error"):
                self.last_errors.append({"url": url, "topic": item.get("topic", url),
                                         "status": result["status"], "error": result["error"]})
            entry = self.state.get(url) or {}
            if result.get("first_seen"):
                # Знакомство: состояние адаптера наполняем (seen/digest), событий не издаём —
                # иначе первая же правка ленты разродилась бы всем её текущим содержимым.
                try:
                    run_adapter(item, result, entry, plugins)
                    self._adapter_dirty = True
                except AdapterError as exc:
                    self.last_errors.append({"url": url, "topic": item.get("topic", url),
                                             "status": result.get("status"),
                                             "error": "адаптер: %s" % exc})
                continue                      # первый раз — это знакомство, а не новость
            if not result["changed"]:
                continue
            if not result.get("validators") and not item.get("allow_volatile"):
                self.last_suppressed.append({
                    "url": url, "topic": item.get("topic", url),
                    "reason": "источник без ETag/Last-Modified: события подавлены (нужен allow_volatile)",
                })
                continue
            try:
                produced = run_adapter(item, result, entry, plugins)
                for event in produced:
                    # «registry»-источники несут дифф вместо слепого образца тела
                    event.setdefault("diff", result.get("diff", ""))
                events.extend(produced)
                self._adapter_dirty = True
            except AdapterError as exc:
                self.last_errors.append({"url": url, "topic": item.get("topic", url),
                                         "status": result.get("status"),
                                         "error": "адаптер: %s" % exc})
                continue
        if events or self._adapter_dirty:
            self.save()                       # адаптеры обновили своё состояние (seen/digest)
        return events


def load_watchlist(path: str) -> list[dict]:
    with open(path, encoding="utf-8") as fh:
        data = json.load(fh)
    if isinstance(data, list):
        return data
    return data.get("sources", [])
