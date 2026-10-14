"""Список наблюдения из архива музея: какие адреса следить и почему.

Читает `data/*.json` музея (только чтение) и превращает первоисточники записей (`r == 1`)
в источники для сканера. Для RFC, на которые ссылается архив, следятся два адреса:

  * статус документа в datatracker (`/api/v1/doc/document/rfcNNNN/`) — может смениться,
    например Internet Standard → Historic;
  * лента изменений документа (`/feed/document-changes/rfcNNNN/`) — видно, что именно произошло.

Сам текст RFC (`/rfc/rfcNNNN.txt`) не следим: он неизменяем, а его исчезновение — это проверка
живости цитаты, а не событие обновления. Индекс всех RFC тоже не следим: 13.7 МБ на обход
ради того, что дешевле видно в ленте дататрекера.

Провенанс: у каждого источника в поле `museum` — id записей архива, из-за которых он попал в список.
"""
from __future__ import annotations

import glob
import json
import os
import re
from urllib.parse import urlparse

# Хосты первоисточников: патентные/стандартизационные архивы, а не новости и вики.
ARCHIVAL_HOSTS = (
    "rfc-editor.org",
    "datatracker.ietf.org",
    "w3.org",
    "oasis-open.org",
    "pubsubhubbub.github.io",
)

RFC_LINK = re.compile(r"(?:rfc-editor\.org/rfc/|datatracker\.ietf\.org/doc/)rfc[-_]?(\d+)", re.IGNORECASE)

STATUS_URL = "https://datatracker.ietf.org/api/v1/doc/document/%s/"
CHANGES_URL = "https://datatracker.ietf.org/feed/document-changes/%s/"

SITE_SOURCES = (
    {
        "url": "https://kostyk348.github.io/lost-tech-museum/",
        "topic": "museum/site",
        "note": "главная музея: новая публикация архива меняет ETag страницы",
    },
    {
        "url": "https://kostyk348.github.io/lost-tech-museum/digests/rfc-graveyard/",
        "topic": "museum/digest/rfc-graveyard",
        "note": "дайджест, из которого вырос hubd (ipmulticast, pubsubhubbub, rsvp, atmip)",
    },
    {
        "url": "https://datatracker.ietf.org/feed/rfc/",
        "topic": "ietf/rfc-feed",
        "note": "лента новых RFC в дататрекере: дешевле, чем индекс RFC целиком (13.7 МБ)",
    },
    {
        "url": ("https://export.arxiv.org/api/query?"
                "search_query=abs:%22obsolete%22+AND+abs:%22protocol%22&max_results=20"),
        "topic": "arxiv/obsolete-protocols",
        "allow_volatile": True,
        "note": "arXiv: свежие статьи про устаревшие протоколы; у API нет ETag — поэтому allow_volatile",
    },
)


def _host_allowed(host: str, include_all: bool) -> bool:
    if include_all:
        return True
    host = host.lower()
    return any(host == allowed or host.endswith("." + allowed) for allowed in ARCHIVAL_HOSTS)


def _add(sources: dict, url: str, topic: str, note: str, museum_id: str,
         allow_volatile: bool = False) -> None:
    """Один адрес — один источник: разные записи архива просто добавляют себе в провенанс."""
    source = sources.get(url)
    if source is None:
        fresh: dict = {"url": url, "topic": topic, "note": note}
        if allow_volatile:
            fresh["allow_volatile"] = True
        fresh["museum"] = []
        sources[url] = fresh
        source = fresh
    if museum_id and museum_id not in source["museum"]:
        source["museum"].append(museum_id)


def from_museum(museum_dir: str, slugs: list[str] | None = None,
                include_all_hosts: bool = False) -> tuple[list[dict], list[dict]]:
    """Возвращает (источники, пропущенные). Музей только читаем: ни один файл не меняется."""
    data_dir = os.path.join(museum_dir, "data")
    if not os.path.isdir(data_dir):
        raise FileNotFoundError("не нашёл каталог данных музея: %s" % data_dir)
    sources: dict[str, dict] = {}
    skipped: list[dict] = []
    for path in sorted(glob.glob(os.path.join(data_dir, "*.json"))):
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
        if not isinstance(data, dict):
            continue
        slug = data.get("slug") or os.path.splitext(os.path.basename(path))[0]
        if slugs and slug not in slugs:
            continue
        for entry in data.get("entries") or []:
            if not isinstance(entry, dict):
                continue
            entry_id = str(entry.get("id") or "")
            name = entry.get("name") or entry_id
            for src in entry.get("src") or []:
                if not isinstance(src, dict) or src.get("r") != 1:
                    continue
                url = (src.get("u") or "").strip()
                if not url:
                    continue
                host = urlparse(url).netloc.lower()
                match = RFC_LINK.search(url)
                rfc = ("rfc%s" % match.group(1)).lower() if match else ""
                if rfc:
                    _add(sources, STATUS_URL % rfc, "rfc/%s/status" % rfc,
                         "статус %s в дататрекере (%s: %s)" % (rfc.upper(), entry_id, name), entry_id)
                    _add(sources, CHANGES_URL % rfc, "rfc/%s/changes" % rfc,
                         "лента изменений %s (%s: %s)" % (rfc.upper(), entry_id, name), entry_id)
                elif _host_allowed(host, include_all_hosts):
                    _add(sources, url, "%s/%s" % (slug, entry_id),
                         "первоисточник записи «%s»" % name, entry_id)
                else:
                    skipped.append({"url": url, "museum": entry_id, "host": host,
                                    "why": "хост не архивный (новости/вики): нужен --all-hosts"})
    for item in SITE_SOURCES:
        _add(sources, item["url"], item["topic"], item["note"], "",
             allow_volatile=bool(item.get("allow_volatile")))
    return sorted(sources.values(), key=lambda s: (s["topic"], s["url"])), skipped


def write(path: str, sources: list[dict], museum_dir: str = "") -> None:
    payload = {
        "comment": ("Сгенерировано: python -m hubd watchlist --museum %s --out %s"
                    % (museum_dir or "<музей>", os.path.basename(path))),
        "sources": sources,
    }
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=1)
        fh.write("\n")
