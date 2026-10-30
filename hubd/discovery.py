"""Обнаружение хаба (WebSub §4): адрес сам говорит, куда подписываться.

Два места, где страница или лента объявляет свой хаб:

  * заголовок ответа: ``Link: <https://hub.example/>; rel="hub"``;
  * разметка: ``<link rel="hub" href="...">`` в HTML,
    ``<atom:link rel="hub" href="...">`` в RSS/Atom.

``hubd discover --url …`` печатает найденное; с флагом ``--subscribe`` сразу
подписывает ваш callback у обнаруженного хаба. Отсутствие объявления —
честный ответ «хаб не объявлен», а не молчаливая отсылка к дефолту.
"""
from __future__ import annotations

import re
import urllib.request

# Link-заголовок: <https://hub.example/>; rel="hub", возможно несколько через запятую
_LINK_RE = re.compile(r"<([^>]*)>\s*;([^,]*)", re.S)
_REL_RE = re.compile(r"""rel\s*=\s*["']?([a-z0-9\- ]+)["']?""", re.IGNORECASE)
# Разметка: <link …>, <atom:link …> — любые префиксы пространств имён
_TAG_RE = re.compile(r"<(?:[a-z0-9]+:)?link\b[^>]*>", re.IGNORECASE)
_ATTR_RE = re.compile(r"""([a-zA-Z0-9\-:]+)\s*=\s*["']([^"']*)["']""")

USER_AGENT = "hubd/1.0 (+discovery)"


def from_headers(headers) -> tuple[list[str], list[str]]:
    """(хабы, self) из Link-заголовков. headers — объект с get_all или dict."""
    values: list[str] = []
    get_all = getattr(headers, "get_all", None)
    if callable(get_all):
        values = [value for value in (get_all("Link") or []) if value]
    elif isinstance(headers, dict):
        for key, value in headers.items():
            if key.lower() == "link" and value:
                values.append(value)
    hubs: list[str] = []
    selfs: list[str] = []
    for value in values:
        for match in _LINK_RE.finditer(value):
            target = match.group(1).strip()
            params = match.group(2) or ""
            rel = _REL_RE.search(params)
            rel_value = (rel.group(1).lower() if rel else "")
            for piece in rel_value.split():
                if piece == "hub" and target and target not in hubs:
                    hubs.append(target)
                elif piece == "self" and target and target not in selfs:
                    selfs.append(target)
    return hubs, selfs


def from_body(body: bytes) -> tuple[list[str], list[str]]:
    """(хабы, self) из разметки: <link rel="hub" href>, atom:link."""
    text = body.decode("utf-8", "replace")
    hubs: list[str] = []
    selfs: list[str] = []
    for tag in _TAG_RE.finditer(text):
        attrs = {key.lower(): value
                 for key, value in _ATTR_RE.findall(tag.group(0))}
        rel = attrs.get("rel", "")
        href = attrs.get("href", "")
        for piece in rel.lower().split():
            if piece == "hub" and href and href not in hubs:
                hubs.append(href)
            elif piece == "self" and href and href not in selfs:
                selfs.append(href)
    return hubs, selfs


def discover(url: str, timeout: float = 30.0) -> dict:
    """Забрать адрес и собрать объявленные hub/self.

    Возвращает {"url", "hubs": [...], "self": "...", "content_type"}.
    Сетевые ошибки не глотаются: пусть вызываетющий печатает причину.
    """
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=timeout) as resp:
        body = resp.read(1024 * 1024)              # объявления живут в шапке документа
        header_hubs, header_selfs = from_headers(resp.headers)
        content_type = resp.headers.get("Content-Type", "")
    body_hubs, body_selfs = from_body(body)
    hubs = header_hubs + [hub for hub in body_hubs if hub not in header_hubs]
    selfs = header_selfs + [item for item in body_selfs if item not in header_selfs]
    return {"url": url, "hubs": hubs,
            "self": selfs[0] if selfs else "", "content_type": content_type}
