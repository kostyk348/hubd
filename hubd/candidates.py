"""Очередь кандидатов: то, что сканер увидел, но хранитель ещё не разобрал.

Зачем отдельный файл, а не сразу хаб: сканер отвечает на вопрос «источник изменился?»,
а разбирать изменение (это находка? цитата? дубль?) должен человек или хранитель архива.
Очередь — это граница между ними: сканер только кладёт кандидатов, статус ставит триаж.

Дедупликация по содержимому: кандидат — это (тема, адрес, отпечаток). Источник, который
вернулся к прежней версии, и повторный обход с теми же изменениями новых строк не создают.

Статус ставит триаж, и вместе со статусом в ту же строку ложится причина решения
(поле `note`): «отклонён: наша же публикация» — это то, что нужно помнить через месяц,
а не только «rejected».
"""
from __future__ import annotations

import hashlib
import json
import os
import tempfile
import time

STATUSES = ("new", "accepted", "rejected")
SAMPLE_CHARS = 300


def candidate_key(topic: str, url: str, digest: str) -> str:
    return hashlib.sha1(("%s\n%s\n%s" % (topic, url, digest)).encode("utf-8")).hexdigest()[:16]


def _sample(body: bytes) -> str:
    """Короткая выдержка для триажа: только если содержимое похоже на текст."""
    if b"\x00" in body[:512]:
        return ""
    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError:
        return ""
    return text[:SAMPLE_CHARS].strip()


class Candidates:
    """Журнал кандидатов в формате JSONL: дописываем строку — не переписываем файл."""

    def __init__(self, path: str, clock=time.time):
        self.path = path
        self.clock = clock
        self.rows: list[dict] = self.load()

    def load(self) -> list[dict]:
        if not os.path.exists(self.path):
            return []
        rows = []
        with open(self.path, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    continue             # битую строку не выдаём за кандидата
        return rows

    def has(self, key: str) -> bool:
        return self.find(key) is not None

    def find(self, key: str) -> dict | None:
        """Строка очереди по ключу кандидата; None — такого кандидата нет."""
        for row in self.rows:
            if row["key"] == key:
                return row
        return None

    def add(self, topic: str, url: str, digest: str, body: bytes = b"",
            status: str = "new", note: str = "") -> dict | None:
        """Кладёт кандидата. None — если такой кандидат уже в очереди."""
        key = candidate_key(topic, url, digest)
        if self.has(key):
            return None
        row = {
            "key": key, "at": self.clock(), "topic": topic, "url": url,
            "digest": digest, "bytes": len(body), "status": status, "note": note,
            "sample": _sample(body),
        }
        directory = os.path.dirname(os.path.abspath(self.path))
        os.makedirs(directory, exist_ok=True)
        with open(self.path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")
            fh.flush()
            os.fsync(fh.fileno())
        self.rows.append(row)
        return row

    def all(self) -> list[dict]:
        return list(self.rows)

    def by_status(self, status: str) -> list[dict]:
        return [row for row in self.rows if row.get("status") == status]

    def counts(self) -> dict[str, int]:
        counts = {status: 0 for status in STATUSES}
        for row in self.rows:
            counts[row.get("status", "new")] = counts.get(row.get("status", "new"), 0) + 1
        return counts

    def set_status(self, key: str, status: str) -> bool:
        """Статус ставит триаж; файл переписываем атомарно, чтобы не потерять строки."""
        return bool((self.decide(key, status) or {}).get("changed"))

    def decide(self, key: str, status: str, reason: str = "") -> dict | None:
        """Решение триажа: статус и причина одной строкой очереди.

        None — кандидата с таким ключом в очереди нет. Иначе словарь:
        key, status, previous (статус до решения), reason (что лежит в строке),
        changed (менялся ли файл), repeat (решение по этому кандидату уже было).

        Причина живёт в поле `note` — рядом со статусом, который она объясняет
        (именно так она и читается в отчёте `hubd candidates`). Пустая причина —
        не ошибка: решение принимается без неё, и строка при этом не обзаводится
        пустым полем. Повторный вызов с той же причиной файл не трогает.
        """
        if status not in STATUSES:
            raise ValueError("неизвестный статус: %r (можно: %s)" % (status, ", ".join(STATUSES)))
        reason = (reason or "").strip()
        row = self.find(key)
        if row is None:
            return None
        previous = row.get("status", "new")
        stored = row.get("note", "")
        if previous == status:
            if not reason or reason == stored:
                return {"key": key, "status": status, "previous": previous, "reason": stored,
                        "changed": False, "repeat": True}
            row["note"] = reason                 # решение то же, уточнилась только причина
            row["decided"] = self.clock()
            self._rewrite()
            return {"key": key, "status": status, "previous": previous, "reason": reason,
                    "changed": True, "repeat": True}
        row["status"] = status
        row["decided"] = self.clock()
        if reason:
            row["note"] = reason
        self._rewrite()
        return {"key": key, "status": status, "previous": previous, "reason": row.get("note", ""),
                "changed": True, "repeat": False, "carried": bool(stored and not reason)}

    def _rewrite(self) -> None:
        directory = os.path.dirname(os.path.abspath(self.path))
        os.makedirs(directory, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=directory)
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            for row in self.rows:
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, self.path)
