"""Хранилище подписок и очереди неподтверждённых доставок.

Подписка — это аренда, а не вечная запись: часы хаба решают, жива ли она.
Так идея multicast («состав группы динамический») становится проверяемой:
подписчик обязан продлить аренду, иначе его выкинут.

Очередь — это контракт доставки (rsvp/atmip): доставка, которую подписчик не подтвердил,
не исчезает и не убивает подписку с первого раза. Она ждёт своей паузы, повторяется
с растущим backoff'ом и сдаётся только когда хаб исчерпал попытки — тогда подписка
помечается degraded и хаб честно отказывает (503 + причина), а не делает вид, что доставил.
"""
from __future__ import annotations

import base64
import hashlib
import json
import os
import tempfile
import time

DEFAULT_LEASE = 3600
MAX_LEASE = 24 * 3600

BACKOFF_BASE = 5.0          # секунд до первой повторной попытки
BACKOFF_MAX = 3600.0        # потолок паузы: дальше ждать смысла нет
DEFAULT_MAX_ATTEMPTS = 8    # попыток на сообщение, включая первую
PENDING_MAX_BYTES = 256 * 1024   # тело больше этого в очередь не кладём (честно скажем об этом)


def sub_id(topic: str, callback: str) -> str:
    return hashlib.sha1(("%s\n%s" % (topic, callback)).encode("utf-8")).hexdigest()[:16]


def delivery_key(sid: str, body: bytes) -> str:
    """Ключ доставки: одна подписка + одно содержимое = одна запись в очереди."""
    digest = hashlib.sha256(body).hexdigest()
    return hashlib.sha1(("%s\n%s" % (sid, digest)).encode("utf-8")).hexdigest()[:16]


def backoff_seconds(attempts: int, base: float = BACKOFF_BASE, cap: float = BACKOFF_MAX) -> float:
    """Пауза перед попыткой номер attempts+1: 5с, 10с, 20с, … не больше часа."""
    return float(min(base * (2 ** max(0, int(attempts))), cap))


class Store:
    def __init__(self, path: str, clock=time.time, pending_max_bytes: int = PENDING_MAX_BYTES):
        self.path = path
        self.clock = clock
        self.pending_max_bytes = pending_max_bytes
        self.subs: dict[str, dict] = {}
        self.pending: dict[str, dict] = {}
        self.load()

    # --- персистентность ---------------------------------------------------------
    def load(self) -> None:
        if os.path.exists(self.path):
            with open(self.path, encoding="utf-8") as fh:
                data = json.load(fh)
            self.subs = data.get("subscriptions") or {}
            self.pending = data.get("pending") or {}

    def save(self) -> None:
        directory = os.path.dirname(os.path.abspath(self.path))
        os.makedirs(directory, exist_ok=True)
        payload = {"saved": self.clock(), "subscriptions": self.subs, "pending": self.pending}
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
        if not sub["degraded"]:
            sub["degraded_reason"] = ""
        self.subs[sid] = sub
        self.save()
        return sub

    def unsubscribe(self, topic: str, callback: str) -> bool:
        sid = sub_id(topic, callback)
        removed = self.subs.pop(sid, None) is not None
        if removed:
            self.drop_pending_for(sid)
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
            self.drop_pending_for(sid)
        if dead:
            self.save()
        return dead

    def note_result(self, sid: str, ok: bool, error: str = "") -> dict | None:
        """Одна попытка доставки: успех сбрасывает счётчик провалов, провал его растит."""
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
        sub["last_result"] = self.clock()
        self.save()
        return sub

    # --- очередь неподтверждённых доставок ---------------------------------------
    def enqueue(self, sid: str, topic: str, body: bytes, content_type: str,
                error: str = "") -> dict | None:
        """Положить доставку в очередь на повтор. None — если тело для очереди великовато."""
        if len(body) > self.pending_max_bytes:
            return None
        key = delivery_key(sid, body)
        existing = self.pending.get(key)
        if existing is not None:
            return existing                      # та же доставка уже ждёт: дублей в очереди нет
        now = self.clock()
        entry = {
            "key": key, "sub": sid, "topic": topic,
            "body": base64.b64encode(body).decode("ascii"),
            "content_type": content_type, "bytes": len(body),
            "attempts": 0, "last_error": error,
            "created": now, "next": now + backoff_seconds(0),
        }
        self.pending[key] = entry
        self.save()
        return entry

    def body_of(self, entry: dict) -> bytes:
        return base64.b64decode(entry["body"].encode("ascii"))

    def drop_pending(self, key: str) -> bool:
        dropped = self.pending.pop(key, None) is not None
        if dropped:
            self.save()
        return dropped

    def drop_pending_for(self, sid: str) -> int:
        keys = [k for k, e in self.pending.items() if e["sub"] == sid]
        for k in keys:
            self.pending.pop(k, None)
        return len(keys)

    def due_pending(self) -> list[dict]:
        """Записи, чья пауза истекла и которые пора повторить."""
        now = self.clock()
        return sorted((e for e in self.pending.values() if e["next"] <= now),
                      key=lambda e: e["next"])

    def note_attempt(self, key: str, ok: bool, error: str = "",
                     max_attempts: int = DEFAULT_MAX_ATTEMPTS) -> dict:
        """Исход повторной попытки: доставлено / ещё подождём / попытки исчерпаны."""
        entry = self.pending.get(key)
        if entry is None:
            return {"state": "gone", "entry": None, "sub": None}
        sub = self.subs.get(entry["sub"])
        if ok:
            self.pending.pop(key, None)
            self.note_result(entry["sub"], True)
            state = "delivered"
        else:
            entry["attempts"] += 1
            entry["last_error"] = error
            self.note_result(entry["sub"], False, error)
            if entry["attempts"] >= max_attempts:
                self.pending.pop(key, None)
                state = "gave_up"
                if sub is not None:
                    sub["degraded"] = True
                    sub["degraded_reason"] = "контракт нарушен: %d попыток, последняя ошибка: %s" % (
                        entry["attempts"], error or "нет подтверждения")
            else:
                entry["next"] = self.clock() + backoff_seconds(entry["attempts"])
                state = "retry"
        self.save()
        return {"state": state, "entry": entry, "sub": sub}

    def unconfirmed(self) -> int:
        """Сколько доставок хаб взял на себя и ещё не подтвердил."""
        return len(self.pending)

    def pending_rows(self) -> list[dict]:
        now = self.clock()
        rows = []
        for entry in sorted(self.pending.values(), key=lambda e: e["next"]):
            rows.append({
                "key": entry["key"], "sub": entry["sub"], "topic": entry["topic"],
                "attempts": entry["attempts"], "bytes": entry["bytes"],
                "next_in": int(entry["next"] - now), "last_error": entry["last_error"],
            })
        return rows

    def as_list(self) -> list[dict]:
        self.expire()
        out = []
        for sub in sorted(self.subs.values(), key=lambda s: s["topic"]):
            item = dict(sub)
            item["expires_in"] = self.expires_in(sub)
            item["pending"] = sum(1 for e in self.pending.values() if e["sub"] == sub["id"])
            item.pop("secret", None)          # секрет наружу не отдаём
            out.append(item)
        return out
