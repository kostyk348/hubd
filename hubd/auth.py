"""Токены издателя: публикация — привилегия, а не следствие доступности порта.

По умолчанию хаб открыт: так работает локальная рутина, и это честно
задокументировано. Если запустить ``hubd serve --publish-tokens tokens.json``,
POST /publish требует валидный токен: заголовок
``Authorization: Bearer <token>`` или ``X-Hub-Token: <token>``.

В файле лежат ТОЛЬКО sha256-хеши:

    {"publish_tokens": [
        {"sha256": "0f1e…64 шестнадцатеричных…", "topics": ["rfc/*"]},
        {"sha256": "…"}                                  # без topics — любые темы
    ]}

Токен и его хеш печатает ``hubd token``. Файл с открытым (нехешированным)
токеном хаб отказывается читать: секрет в открытом виде — ошибка конфигурации,
а не допустимая вольность. Тему можно ограничить шаблонами fnmatch: токен
с "topics": ["rfc/*"] публикует только в rfc/*, иначе — 401 с причиной.
"""
from __future__ import annotations

import fnmatch
import hashlib
import json
import secrets as _secrets


def token_hash(token: str) -> str:
    """Хеш токена в файле конфигурации: sha256 от строки токена."""
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def generate() -> tuple[str, str]:
    """(токен, его sha256): токен показывается один раз, хранится только хеш."""
    token = _secrets.token_hex(32)
    return token, token_hash(token)


def token_from_headers(headers) -> str:
    """Токен из заголовков: Authorization: Bearer … или X-Hub-Token: …"""
    authorization = headers.get("Authorization") or ""
    if authorization.lower().startswith("bearer "):
        return authorization[7:].strip()
    return (headers.get("X-Hub-Token") or "").strip()


class TokenStore:
    """Загруженные хеши токенов издателя. Проверка — по sha256, файл хешей."""

    def __init__(self, entries: list[dict]):
        self.entries = entries

    @classmethod
    def load(cls, path: str) -> "TokenStore":
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
        raw = data.get("publish_tokens")
        if raw is None:
            raise ValueError("в %s нет ключа publish_tokens" % path)
        if not isinstance(raw, list):
            raise ValueError("publish_tokens должен быть списком")
        entries: list[dict] = []
        for item in raw:
            if isinstance(item, str):
                raise ValueError(
                    "в %s токен лежит открытым текстом — так нельзя: положите "
                    "sha256-хеш (его печатает hubd token)" % path)
            if not isinstance(item, dict):
                raise ValueError("запись токена должна быть объектом")
            digest = str(item.get("sha256") or "").strip().lower()
            if len(digest) != 64 or any(ch not in "0123456789abcdef" for ch in digest):
                raise ValueError("поле sha256 не похоже на хеш: %r" % digest[:20])
            topics = item.get("topics") or []
            if not isinstance(topics, list):
                raise ValueError("topics должен быть списком шаблонов")
            entries.append({"sha256": digest, "topics": [str(t) for t in topics]})
        return cls(entries)

    def check(self, token: str, topic: str) -> tuple[bool, str]:
        """(разрешено, причина). Пустой токен — отказ с подсказкой, не тишина."""
        if not token:
            return False, ("публикация защищена токеном: передайте его в "
                           "Authorization: Bearer <token> или X-Hub-Token")
        digest = token_hash(token)
        for entry in self.entries:
            if entry["sha256"] == digest:
                patterns = entry["topics"]
                if not patterns or any(fnmatch.fnmatchcase(topic, pattern)
                                       for pattern in patterns):
                    return True, ""
                return False, "токен не даёт доступ к теме %r" % topic
        return False, "неизвестный токен"
