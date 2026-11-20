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


def parse_rfc_index(raw: bytes) -> dict[str, dict[str, str]]:
    """doc-id -> {current-status, publication-status, stream, title}. Пусто, если XML не разобрался."""
    import xml.etree.ElementTree as ET
    table: dict[str, dict[str, str]] = {}
    if not raw:
        return table
    try:
        root = ET.fromstring(raw)
    except ET.ParseError:
        return table
    wanted = {"current-status", "publication-status", "stream", "title"}
    for entry in root.iter():
        if not entry.tag.endswith("rfc-entry"):
            continue
        row: dict[str, str] = {}
        for child in entry:
            tag = child.tag.rsplit("}", 1)[-1]
            if tag == "doc-id":
                row["doc-id"] = (child.text or "").strip()
            elif tag in wanted:
                row[tag] = " ".join((child.text or "").split())
        doc = row.get("doc-id", "")
        if doc:
            table[doc] = row
    return table


def _projection_line(marker: str, doc: str, row: dict[str, str]) -> str:
    parts = ["%s%s" % (marker, doc)]
    if row.get("current-status"):
        parts.append("current-status: %s" % row["current-status"])
    if row.get("publication-status"):
        parts.append("publication-status: %s" % row["publication-status"])
    if row.get("stream"):
        parts.append("(stream: %s)" % row["stream"])
    return "  ".join(parts)


def project_from_tables(before: dict[str, dict], after: dict[str, dict],
                        limit_lines: int = 80, limit_chars: int = 6000) -> str:
    """Дифф двух проекций `doc-id -> поля`.

    Проекция весит 29 КБ против 2,3 МБ gz-тела и 13,8 МБ сырья (замер триажа),
    поэтому её можно хранить и диффить на каждом обходе: одна смена — одна-две
    строки, в бюджет 80 строк влезает ~40 смен против ~8 у сырого диффа.
    Заголовок честно говорит, сколько смен показано, а сколько отрезано.
    """
    if not before or not after:
        return ""
    fields = ("current-status", "publication-status", "stream", "title")
    changed: list[tuple[str, dict, dict]] = []
    for doc in sorted(after):
        if doc not in before:
            changed.append((doc, {}, after[doc]))
        elif any(f in before[doc] and f in after[doc] and before[doc][f] != after[doc][f]
                 for f in fields):
            changed.append((doc, before[doc], after[doc]))
    for doc in sorted(before):
        if doc not in after:
            changed.append((doc, before[doc], {}))
    total = len(changed)
    if total == 0:
        return ""
    lines: list[str] = []
    shown = 0
    for doc, old, new in changed:
        if old and new:
            pair = [_projection_line("-", doc, old), _projection_line("+", doc, new)]
            if "title" in old and "title" in new and old["title"] != new["title"]:
                # Заголовок печатаем только когда он изменился: иначе строка про
                # статус выглядела бы неизменившейся, а смена была бы в title.
                pair.append("   \u00ab%s\u00bb -> \u00ab%s\u00bb" % (
                    old.get("title", ""), new.get("title", "")))
        elif new:
            pair = ["%s  новая запись" % _projection_line("+", doc, new)]
        else:
            pair = ["%s  запись исчезла" % _projection_line("-", doc, old)]
        if len(lines) + len(pair) + 1 > limit_lines:
            break
        lines.extend(pair)
        shown += 1
    tail = "" if shown == total else ", обрезано"
    header = "изменено записей: %d (показано %d%s)" % (total, shown, tail)
    return "\n".join([header] + [line.rstrip() for line in lines])[:limit_chars]


def project_rfc_index_status(previous: bytes, current: bytes,
                             limit_lines: int = 80, limit_chars: int = 6000) -> str:
    """Совместимость: дифф по двум телам — когда кэша проекции ещё нет."""
    return project_from_tables(parse_rfc_index(previous), parse_rfc_index(current),
                               limit_lines, limit_chars)


class Watcher:
    """Следит за источниками и говорит, что изменилось."""

    #: сколько подряд идущих неудач терпим, прежде чем поставить источник на паузу
    QUARANTINE_AFTER = 3

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
        self.last_skipped: list[dict] = []
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

    def _projection_path(self, url: str) -> str:
        name = hashlib.sha1(url.encode("utf-8")).hexdigest()[:16] + ".projection.json.gz"
        return os.path.join(os.path.dirname(os.path.abspath(self.state_path)), "bodies", name)

    def read_projection(self, url: str) -> dict:
        """Кэш проекции (29 КБ вместо 2,3 МБ gz-тела): производное от тела, не канон."""
        path = self._projection_path(url)
        if not os.path.exists(path):
            return {}
        try:
            with gzip.open(path, "rt", encoding="utf-8") as fh:
                data = json.load(fh)
            return data if isinstance(data, dict) else {}
        except (OSError, EOFError, ValueError):
            return {}

    def write_projection(self, url: str, table: dict) -> None:
        if not table:
            return
        path = self._projection_path(url)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = path + ".tmp"
        lean = {doc: {k: v for k, v in row.items() if k not in ("title", "doc-id")}
                for doc, row in table.items()}
        with gzip.open(tmp, "wt", encoding="utf-8") as fh:
            json.dump(lean, fh, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        os.replace(tmp, path)

    def diff(self, previous: bytes, current: bytes) -> str:
        """Дифф прошлого и нового тела — то, что триаж увидит вместо первых 300 байт.

        Каждый байт реестра превращать в строки незачем: диффим текстовые строки,
        а результат режем по строкам и символам — очередь кандидатов не должна
        распухать от 13,8-мегабайтного индекса (Архивесса права: без диффа кандидат
        говорит «файл изменился, разбирайся сам»).
        """
        try:
            old_lines = previous.decode("utf-8", "replace").splitlines()
            new_lines = current.decode("utf-8", "replace").splitlines()
        except Exception:                                    # noqa: BLE001 — дифф не критичен
            return ""
        chunks = list(difflib.unified_diff(old_lines, new_lines, fromfile="было", tofile="стало",
                                           n=1, lineterm=""))
        text = "\n".join(chunks[: self.DIFF_LINES])
        return text[: self.DIFF_CHARS]

    def missing_fingerprints(self, watchlist: list[dict]) -> list[dict]:
        """Источники, у которых так и нет отпечатка: молчащий источник — тоже дефект.

        Триаж видит только кандидатов, поэтому «в списке есть, а изменений не приходит»
        иначе выглядит как «всё спокойно». Здесь видно, когда источник последний раз
        реально читался — «добавлен час назад» и «сломан месяц назад» это разные вещи.
        """
        missing = []
        for item in watchlist:
            url = item["url"]
            entry = self.state.get(url) or {}
            if entry.get("hash") is None:
                last_ok = entry.get("last_ok")
                missing.append({
                    "url": url,
                    "topic": item.get("topic", url),
                    "last_ok": (time.strftime("%d.%m %H:%M", time.localtime(last_ok))
                                if last_ok else "никогда"),
                    "error": entry.get("error", ""),
                    "checks": int(entry.get("checks") or 0),
                })
        return missing

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

    def check(self, url: str, store_body: bool = False, projection: str = "") -> dict:
        """Один источник: условный GET, сверка с отпечатком содержимого.

        Сбой источника (сеть, 5xx, 404) — это результат обхода, а не повод уронить весь скан:
        он записывается в состояние и возвращается вызывающему как ошибка источника.
        """
        entry = self.state.setdefault(url, {"hash": None, "checks": 0, "changes": 0})
        if projection and not self.read_projection(url):
            # Базовая проекция из уже сохранённого тела: на сети это не стоит ничего,
            # а первый же сдвиг статуса будет диффиться по кэшу, а не по двум телам.
            stored = self.read_body(url)
            if stored:
                self.write_projection(url, parse_rfc_index(stored)
                                      if projection == "rfc-index-status" else {})
        now = self.clock()
        quarantined_until = entry.get("quarantine_until") or 0
        if quarantined_until > now:
            # Сломанный источник не должен стоить трафика на каждом обходе: 13,7 МБ
            # трижды за проход — это то, на что указала Архивесса. Пауза растёт
            # с числом подряд идущих неудач (до суток), успех её снимает.
            return {"url": url, "changed": False, "status": 0, "skipped": True,
                    "reason": "карантин до %s (%d неудач подряд)" % (
                        time.strftime("%d.%m %H:%M", time.localtime(quarantined_until)),
                        int(entry.get("fails") or 0))}
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
            entry["fails"] = 0
            entry["quarantine_until"] = 0
            entry["last_ok"] = self.clock()
            self.save()
            return {"url": url, "changed": False, "status": 304, "reason": "not modified"}

        if status != 200:
            error = "источник недоступен (%s)" % network_error if status == 0 else "HTTP %s" % status
            fails = int(entry.get("fails") or 0) + 1
            entry["fails"] = fails
            if fails >= self.QUARANTINE_AFTER:
                pause = min(3600 * (2 ** (fails - self.QUARANTINE_AFTER)), 24 * 3600)
                entry["quarantine_until"] = self.clock() + pause
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
            "fails": 0,
            "quarantine_until": 0,
            "last_ok": self.clock(),
            "hash": digest,
            "bytes": len(body),
            "validators": validators,
            "etag": resp_headers.get("ETag", resp_headers.get("etag", "")),
            "last_modified": resp_headers.get("Last-Modified", resp_headers.get("last-modified", "")),
            "error": "",
        })
        diff_text = ""
        # Прошлое тело нужно ДО перезаписи: иначе диффать будет не с чем и триаж
        # получит «файл изменился» вместо «строка X: Internet Standard -> Historic».
        previous = self.read_body(url) if store_body else None
        cache_missing = bool(projection) and not self.read_projection(url)
        if projection and (changed or cache_missing):
            # Кэш проекции живёт отдельно от тела: диффить 33 КБ дешевле, чем заново
            # разбирать 13,8 МБ прошлого тела (замер: 0,63 с против 1,74 с). Тело
            # остаётся каноном — первоисточником для цитаты, проекция производна.
            # cache_missing — базовая съёмка проекции: иначе первый же сдвиг статуса
            # стоил бы лишнего разбора прошлого тела (не ошибка, но вдвое дороже).
            before = self.read_projection(url)
            after = parse_rfc_index(body) if projection == "rfc-index-status" else {}
            diff_text = project_from_tables(before, after, self.DIFF_LINES, self.DIFF_CHARS)
            if not diff_text and previous is not None:
                diff_text = project_rfc_index_status(previous, body,
                                                     self.DIFF_LINES, self.DIFF_CHARS)
            if not diff_text and previous is not None:
                diff_text = self.diff(previous, body)   # фолбэк: CSV и прочее сырьё
            if after and (changed or cache_missing):
                self.write_projection(url, after)
        elif changed and previous is not None:
            diff_text = self.diff(previous, body)
        if store_body:
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
        self.last_skipped = []
        self._adapter_dirty = False
        for item in watchlist:
            url = item["url"]
            result = self.check(url, store_body=bool(item.get("store_body")),
                                projection=item.get("projection", ""))
            if result.get("skipped"):
                self.last_skipped.append({"url": url, "topic": item.get("topic", url),
                                          "reason": result.get("reason", "")})
                continue
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
