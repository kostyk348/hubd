"""Хранилище подписок: JSON-файл, атомарная запись, аренда (lease) с истечением.

Подписка — это аренда, а не вечная запись: часы хаба решают, жива ли она.
Так идея multicast («состав группы динамический») становится проверяемой:
подписчик обязан продлить аренду, иначе его выкинут.
"""
from __future__ import annotations

import hashlib
import json
import os
import tempfile
import time

DEFAULT_LEASE = 3600
MAX_LEASE = 24 * 3600


def sub_id(topic: str, callback: str) -> str:
    return hashlib.sha1(("%s\n%s" % (topic, callback)).encode("utf-8")).hexdigest()[:16]


class Store:
    def __init__(self, path: str, clock=time.time):
        self.path = path
        self.clock = clock
        self.subs: dict[str, dict] = {}
        self.load()

    # --- персистентность ---------------------------------------------------------
    def load(self) -> None:
        if os.path.exists(self.path):
            with open(self.path, encoding="utf-8") as fh:
                self.subs = json.load(fh).get("subscriptions", {})

    def save(self) -> None:
        directory = os.path.dirname(os.path.abspath(self.path))
        os.makedirs(directory, exist_ok=True)
        payload = {"saved": self.clock(), "subscriptions": self.subs}
        fd, tmp = tempfile.mkstemp(dir=directory)
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=1)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, self.path)

    # --- подписки ----------------------------------------------------------------
    def subscribe(self, topic: str, callback: str, lease_seconds: int = DEFAULT_LEASE,
                  secret: str | None = None) -> dict:
        now = self.clock()
        lease_seconds = max(60, min(int(lease_seconds or DEFAULT_LEASE), MAX_LEASE))
        sid = sub_id(topic, callback)
        sub = self.subs.get(sid) or {
            "id": sid, "topic": topic, "callback": callback, "created": now,
            "deliveries": 0, "failures": 0, "degraded": False,
        }
        sub.update({
            "secret": secret, "lease_seconds": lease_seconds,
            "expires_at": now + lease_seconds, "renewed": now,
            "degraded": False if sub.get("expires_at", 0) < now else sub.get("degraded", False),
        })
        self.subs[sid] = sub
        self.save()
        return sub

    def unsubscribe(self, topic: str, callback: str) -> bool:
        sid = sub_id(topic, callback)
        removed = self.subs.pop(sid, None) is not None
        if removed:
            self.save()
        return removed

    def expires_in(self, sub: dict) -> int:
        return int(sub.get("expires_at", 0) - self.clock())

    def for_topic(self, topic: str) -> list[dict]:
        self.expire()
        return [s for s in self.subs.values() if s["topic"] == topic]

    def expire(self) -> list[str]:
        now = self.clock()
        dead = [sid for sid, s in self.subs.items() if s.get("expires_at", 0) <= now]
        for sid in dead:
            self.subs.pop(sid, None)
        if dead:
            self.save()
        return dead

    def note_result(self, sid: str, ok: bool, error: str = "", max_failures: int = 3) -> dict | None:
        """Исход доставки. Три провала подряд — подписка помечена degraded (контракт не выполняется)."""
        sub = self.subs.get(sid)
        if not sub:
            return None
        if ok:
            sub["deliveries"] += 1
            sub["failures"] = 0
            sub["last_error"] = ""
        else:
            sub["failures"] += 1
            sub["last_error"] = error
            if sub["failures"] >= max_failures:
                sub["degraded"] = True
        sub["last_result"] = self.clock()
        self.save()
        return sub

    def as_list(self) -> list[dict]:
        self.expire()
        out = []
        for sub in sorted(self.subs.values(), key=lambda s: s["topic"]):
            item = dict(sub)
            item["expires_in"] = self.expires_in(sub)
            item.pop("secret", None)          # секрет наружу не отдаём
            out.append(item)
        return out
