"""Адаптеры источников: «адрес изменился» → «какие события это значит».

Сканер (scan.py) отвечает на вопрос «изменился ли адрес?» — по байтовому
отпечатку ответа. Адаптер отвечает на следующий вопрос: что это изменение
значит и на какие события его разложить. Для plain-источника событие одно —
само тело ответа; для ленты RSS/Atom событие — каждая новая запись; для
JSON-API — новое значение по указанному пути.

Адаптер выбирается полем `adapter` в списке наблюдения:

    {"url": "https://api.example.com/repos/x/y/releases/latest",
     "topic": "app/y/releases",
     "adapter": "json", "json_path": "tag_name"}

Доступные адаптеры:
  * plain — тело ответа целиком (по умолчанию; прежнее поведение hubd);
  * json  — JSON-API: берём значение по пути `json_path` (например `tag_name`
    или `server.version`), событие — только если это значение изменилось;
  * rss / atom / feed — лента: событие на каждую новую запись (id + updated),
    `max_entries` ограничивает всплеск (по умолчанию 20), `per_entry: false`
    публикует ленту одной темой.

Без поля `adapter` работает plain. Первый обход любого адаптера — знакомство:
событий нет, состояние запоминается (это правило сканера, адаптеры его не меняют).
Состояние адаптера лежит в состоянии обхода источника и переживает перезапуск.
"""
from __future__ import annotations

import hashlib
import json
import re
import xml.etree.ElementTree as ET

ADAPTER_NAMES = ("plain", "json", "rss", "atom", "feed")
MAX_SUMMARY_CHARS = 2000
SEEN_LIMIT = 500                  # сколько отпечатков записей помнить для ленты


class AdapterError(Exception):
    """Ошибка адаптера: видимая строка в отчёте обхода, а не падение скана."""


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def canonical_json(value) -> bytes:
    """Канонический JSON: детерминированный отпечаток и читаемый payload."""
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":")).encode("utf-8")


def _slug(text: str, fallback: str = "entry") -> str:
    text = (text or "").strip().lower()
    text = re.sub(r"[^a-z0-9а-яё]+", "-", text, flags=re.UNICODE).strip("-")
    return text[:60] or fallback


def _json_path(value, path: str):
    """Значение по пути `server.version` или `releases.0.tag_name` (число — индекс)."""
    current = value
    for part in path.split("."):
        if isinstance(current, dict):
            if part not in current:
                raise AdapterError("в JSON нет пути %r: нет ключа %r" % (path, part))
            current = current[part]
        elif isinstance(current, list):
            try:
                index = int(part)
            except ValueError:
                raise AdapterError("в пути %r элемент %r — не индекс списка"
                                   % (path, part)) from None
            if not 0 <= index < len(current):
                raise AdapterError("индекс %r вне границ списка в пути %r" % (part, path))
            current = current[index]
        else:
            raise AdapterError("в JSON нет пути %r: упёрлись в скаляр" % path)
    return current


def _event(topic: str, url: str, payload: bytes, digest: str,
           content_type: str) -> dict:
    return {"topic": topic, "url": url, "payload": payload, "hash": digest,
            "content_type": content_type}


def _normalize(item: dict, event) -> dict:
    """Привести событие (от адаптера или плагина) к контракту сканера."""
    if not isinstance(event, dict):
        raise AdapterError("событие должно быть словарём, получено %s"
                           % type(event).__name__)
    payload = event.get("payload", b"")
    if isinstance(payload, str):
        payload = payload.encode("utf-8")
    elif isinstance(payload, (dict, list)):
        payload = canonical_json(payload)
    elif not isinstance(payload, bytes):
        payload = str(payload).encode("utf-8")
    topic = str(event.get("topic") or item.get("topic") or event.get("url")
                or item.get("url") or "").strip()
    url = str(event.get("url") or item.get("url") or "").strip()
    if not topic:
        raise AdapterError("у события пустая тема: задайте topic в списке наблюдения")
    return {"topic": topic, "url": url, "payload": payload,
            "hash": str(event.get("hash") or sha256_hex(payload)),
            "content_type": str(event.get("content_type") or "application/octet-stream")}


# --- plain -----------------------------------------------------------------------
def plain_events(item: dict, result: dict, state: dict) -> list[dict]:
    """Тело ответа целиком — так работал hubd до появления адаптеров."""
    return [_event(item.get("topic") or result["url"], result["url"],
                   result["body"], result["hash"], "application/octet-stream")]


# --- json ------------------------------------------------------------------------
def json_events(item: dict, result: dict, state: dict) -> list[dict]:
    """JSON-API: событие — новое значение по пути, а не новое тело.

    API часто добавляют в ответ счётчики и даты: тело меняется при каждом
    запросе, а наблюдаемое значение — нет. Отпечаток считается по значению:
    значение не изменилось — событий нет, как бы ни шумело тело.
    """
    try:
        data = json.loads(result["body"].decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise AdapterError("тело не JSON: %s" % exc) from None
    path = (item.get("json_path") or "").strip()
    value = _json_path(data, path) if path else data
    digest = sha256_hex(canonical_json(value))
    if state.get("digest") == digest:
        return []
    state["digest"] = digest
    return [_event(item.get("topic") or result["url"], result["url"],
                   canonical_json(value), digest, "application/json")]


# --- rss / atom ------------------------------------------------------------------
def _child_text(node: ET.Element, *names: str) -> str:
    for name in names:
        text = node.findtext("{*}%s" % name)
        if text and text.strip():
            return text.strip()
    return ""


def _entry_link(node: ET.Element) -> str:
    """Ссылка записи: <link href> у Atom, текст <link> у RSS."""
    fallback = ""
    for link_node in node.findall("{*}link"):
        href = (link_node.get("href") or "").strip()
        text = (link_node.text or "").strip()
        rel = (link_node.get("rel") or "").strip().lower() or "alternate"
        if rel == "alternate" and href:
            return href
        if not fallback and (href or text):
            fallback = href or text
    return fallback


def _feed_entries(body: bytes) -> list[dict]:
    try:
        root = ET.fromstring(body)
    except ET.ParseError as exc:
        raise AdapterError("тело не XML-лента: %s" % exc) from None
    tag = root.tag.rsplit("}", 1)[-1].lower()
    if tag == "rss" or tag == "rdf":
        nodes = root.findall(".//{*}item")
    elif tag == "feed":
        nodes = root.findall(".//{*}entry")
    else:
        raise AdapterError("корень <%s> — это не RSS (<rss>) и не Atom (<feed>)" % tag)
    entries = []
    for node in nodes:
        title = _child_text(node, "title")
        entry_id = _child_text(node, "guid") or _child_text(node, "id")
        updated = _child_text(node, "updated", "published", "pubDate", "date")
        summary = _child_text(node, "summary", "description", "content")
        link = _entry_link(node)
        entry_id = entry_id or link or title
        entries.append({
            "id": entry_id, "title": title, "link": link, "updated": updated,
            "summary": summary[:MAX_SUMMARY_CHARS],
        })
    return entries


def feed_events(item: dict, result: dict, state: dict) -> list[dict]:
    """Лента: событие на каждую новую или изменившуюся запись.

    Отпечаток записи считается по (id, title, link, updated): правка записи
    в ленте — тоже событие, а не только её появление. Уже виденные отпечатки
    лежат в состоянии обхода (последние %d), поэтому повторный обход ленты,
    вернувшейся к прежнему набору записей, дублей не создаёт.
    """ % SEEN_LIMIT
    entries = _feed_entries(result["body"])
    seen = set(state.get("seen") or [])
    fresh = []
    for entry in entries:
        digest = sha256_hex(canonical_json({
            "id": entry["id"], "title": entry["title"],
            "link": entry["link"], "updated": entry["updated"],
        }))
        if digest in seen:
            continue
        entry["digest"] = digest
        fresh.append(entry)
    state["seen"] = (list(seen) + [e["digest"] for e in fresh])[-SEEN_LIMIT:]
    if not fresh:
        return []
    limit = max(1, int(item.get("max_entries", 20)))
    fresh = fresh[:limit]
    base_topic = item.get("topic") or result["url"]
    per_entry = item.get("per_entry", True)
    events = []
    for entry in fresh:
        slug = _slug(entry["id"] or entry["title"])
        topic = ("%s/%s" % (base_topic, slug)) if per_entry else base_topic
        url = entry["link"] or "%s#%s" % (result["url"], slug)
        payload = canonical_json({key: entry[key]
                                  for key in ("id", "title", "link", "updated", "summary")})
        events.append(_event(topic, url, payload, entry["digest"], "application/json"))
    return events


BUILTIN = {
    "plain": plain_events,
    "json": json_events,
    "rss": feed_events,
    "atom": feed_events,
    "feed": feed_events,
}


def _transform_names(item: dict) -> list[str]:
    names = item.get("transform") or []
    if isinstance(names, str):
        return [names]
    return [str(name) for name in names]


def run_adapter(item: dict, result: dict, state: dict,
                plugins: dict | None = None) -> list[dict]:
    """Источник → события. plugins — загруженные плагины (см. plugins.py).

    Порядок: плагин с extract() (если в списке наблюдения указан "plugin") —
    иначе встроенный адаптер по полю "adapter"; затем transform-плагины из поля
    "transform" (по цепочке, каждое событие). Итог нормализуется под контракт
    сканера: topic, url, payload: bytes, hash, content_type.
    """
    plugin_name = str(item.get("plugin") or "").strip()
    if plugin_name:
        plugin = (plugins or {}).get(plugin_name)
        if plugin is None or not hasattr(plugin, "extract"):
            raise AdapterError("плагин %r не найден или без extract()" % plugin_name)
        try:
            events = plugin.extract(item, result, state)
        except AdapterError:
            raise
        except Exception as exc:                       # ошибка плагина — не падение скана
            raise AdapterError("плагин %r упал: %s: %s"
                               % (plugin_name, type(exc).__name__, exc)) from None
        if events is None:
            events = []
        if isinstance(events, dict):
            events = [events]
        normalized = [_normalize(item, event) for event in events]
    else:
        name = str(item.get("adapter") or "plain").strip().lower()
        handler = BUILTIN.get(name)
        if handler is None:
            raise AdapterError("неизвестный адаптер %r (можно: %s)"
                               % (name, ", ".join(ADAPTER_NAMES)))
        normalized = [_normalize(item, event) for event in handler(item, result, state)]

    for transform_name in _transform_names(item):
        plugin = (plugins or {}).get(transform_name)
        if plugin is None or not hasattr(plugin, "transform"):
            raise AdapterError("transform-плагин %r не найден или без transform()"
                               % transform_name)
        passed = []
        for event in normalized:
            try:
                outcome = plugin.transform(event, item)
            except AdapterError:
                raise
            except Exception as exc:
                raise AdapterError("transform-плагин %r упал: %s: %s"
                                   % (transform_name, type(exc).__name__, exc)) from None
            if outcome is not None:
                passed.append(_normalize(item, outcome))
        normalized = passed
    return normalized
