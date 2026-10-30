# hubd

### Stop polling. Subscribe once.

**hubd is a pub/sub hub, a source watcher and a WebSub client in one dependency-free package.
Publishers post to a topic without knowing subscribers, subscriptions are leases, and an
unconfirmed delivery is a state — retried with backoff or honestly refused, never silently lost.**

[**English**](README.md) · [Русский](README.ru.md)

[![tests](https://github.com/kostyk348/hubd/actions/workflows/tests.yml/badge.svg)](https://github.com/kostyk348/hubd/actions/workflows/tests.yml)
[![Release](https://img.shields.io/github/v/release/kostyk348/hubd?sort=semver&color=blue)](https://github.com/kostyk348/hubd/releases)
[![License](https://img.shields.io/badge/license-Apache--2.0-green)](https://github.com/kostyk348/hubd/blob/master/LICENSE)
![Python](https://img.shields.io/badge/python-3.11%2B-informational)

---

## What is hubd?

One Python package (3.11+, **standard library only** — `pip install` is not needed and not
welcome) that covers both sides of the change-detection story — for scripts, bots, agents,
homelab cron jobs, anything that needs to react to changes:

* 📡 **Hub** — subscribe / unsubscribe / publish over HTTP with WebSub discipline:
  challenge-verified intent, `hub.mode=denied`, `X-Hub-Signature: sha256=…`
* ⏳ **Delivery contract** — an unconfirmed delivery is not silence: a persistent retry queue
  with exponential backoff; when attempts run out the subscription is marked `degraded` and the
  hub answers `503` **with a reason**
* 🔍 **Watcher** — conditional GET (`ETag` / `If-Modified-Since`), sha256 dedup, polite
  1 req/s per host; "noisy" sources without validators are visibly suppressed, not dropped
* 🔌 **Adapters** — a changed body is not yet an event: `plain` publishes the raw body,
  `json` publishes the value at `json_path` (dedup **by value**, not by a noisy body),
  `rss`/`atom` publish per entry
* 🧩 **Plugins** — drop `.py` files into a directory: your own adapters (`extract`) and event
  filters (`transform`) without touching the core
* 🛰 **WebSub client side** — `discover` reads the hub a page advertises (`Link` header,
  `atom:link`), `listen` runs a callback server (challenge echo, signed deliveries, JSONL
  inbox), `subscribe` performs the handshake
* 🔐 **Optional hardening** — publisher tokens (sha256-hashed in the config, topic-scoped),
  topic policies (`allow`/`deny`), HTTPS with two flags
* 🧪 **Proven, not promised** — 153 tests, an 11-check live demo on loopback, honest exit
  codes for cron

## Where it comes from

hubd grew out of the [RFC-graveyard digest](https://kostyk348.github.io/lost-tech-museum/digests/rfc-graveyard/)
of the Museum of Lost Ideas — not "inspired by", but record by record:

| archive record | what it became |
| --- | --- |
| `ipmulticast` | an address of a group instead of a list of recipients: publish to a **topic**, know no subscribers |
| `pubsubhubbub` | pub/sub over HTTP: `hub.topic` / `hub.callback` / `hub.secret`, intent verified by a challenge |
| `rsvp`, `atmip` | a delivery contract: a subscription is a lease, unconfirmed delivery is retried and in the limit **honestly refused** |
| `time868` | a prototypical watch object: "RFC 868 — formally a standard, de facto a museum piece" |

The discipline is compatible in spirit with [WebSub (W3C)](https://www.w3.org/TR/websub/):
challenge, 404 as a subscriber's refusal, `hub.mode=denied` as a message **from the hub**,
`X-Hub-Signature: sha256=…` per subscription secret. The museum remains one example watchlist
(`python -m hubd watchlist --museum …`) — not a requirement.

## Quick start

```
# 1. tests — zero dependencies, stdlib only
python3 -m unittest discover -s tests -t .          # Ran 153 tests ... OK

# 2. the whole demonstration: hub, two subscribers, refusal policy, retry queue
python3 -m hubd demo                                 # итог: проверок 11, провалено 0

# 3. run a hub (add --tls-cert/--tls-key for https, --publish-tokens for auth)
python3 -m hubd serve --store data/hub.json --port 8765

# 4. watch sources; collecting and delivering are separate steps
python3 -m hubd scan --watchlist watchlist.json --queue data/candidates.jsonl --once
python3 -m hubd scan --watchlist watchlist.json --queue data/candidates.jsonl \
                    --publish --hub http://127.0.0.1:8765 --every 300
```

A watchlist is just JSON — any list of sources with topics:

```
{"sources": [
  {"url": "https://blog.example/feed.xml",   "topic": "blog",        "adapter": "rss"},
  {"url": "https://api.example.com/.../latest", "topic": "app/releases",
   "adapter": "json", "json_path": "tag_name"},
  {"url": "https://page.example.com/status", "topic": "page/status"}
]}
```

## Screenshots (real output, loopback)

| The 11-check demo | Tokens, policy, subscriber |
| --- | --- |
| ![hubd demo](assets/promo/demo_hub.png) | ![hubd universal](assets/promo/demo_universal.png) |

## The HTTP API of the hub

| method | path | what it does |
| --- | --- | --- |
| `POST` | `/subscribe` | `hub.mode=subscribe`, `hub.topic`, `hub.callback`, `hub.lease_seconds`, `hub.secret`. Answers **202** only after the intent is verified (challenge the subscriber must echo) |
| `POST` | `/unsubscribe` | `hub.mode=unsubscribe`, `hub.topic`, `hub.callback` → 202; the subscription's queue goes with it |
| `POST` | `/publish?topic=…` | body is the payload. Published **to a topic**: the publisher never lists subscribers |
| `POST` | `/retry` | one pass over the queue of unconfirmed deliveries (`serve --retry-interval` runs it in a loop) |
| `GET` | `/subscriptions` | subscriptions without secrets: lease, deliveries, failures, `pending`, total `unconfirmed` |
| `GET` | `/journal` | last 50 journal records (who got what) |
| `GET` | `/healthz` | `ok`, number of subscriptions, `unconfirmed` |

Publishing answers honestly, not "success in general":

| code | meaning |
| --- | --- |
| `200` | delivered to **all** active subscribers |
| `202` | accepted, but something is unconfirmed — it will be retried (`unconfirmed` in the body) |
| `502` | could not deliver and cannot retry (body larger than the queue) |
| `503` | every subscriber broke the delivery contract: `refused` with reasons |

## The delivery contract: "unconfirmed" is a state, not silence

* an unconfirmed delivery enters the queue with a **5 s** delay, doubling up to 1 hour
  (`backoff_seconds`);
* up to 8 attempts per message (`max_attempts`);
* the queue lives on disk (same JSON as subscriptions) and survives restarts;
* a body over 256 KB is **not** queued: instead of a silent loss — `502` with a reason;
* when attempts run out the subscription is marked `degraded`, and the hub honestly refuses
  (`503`) instead of pretending it delivered;
* an expired lease or removed subscription drops its queue: nobody to knock on;
* how many deliveries the hub has taken on and not yet confirmed is visible in `/healthz`,
  `/subscriptions`, `hubd subs` and the `hubd retry` report.

## The watcher and adapters

The watcher answers "did the URL change?" (conditional GET, sha256 dedup, politeness, honest
error lines for cron with `check --fail-on-error`). The adapter answers "what does it mean":

| adapter | field | event |
| --- | --- | --- |
| `plain` | *(default)* | the raw body — exactly the pre-1.0 behavior |
| `json` | `json_path: "tag_name"` | the value at the path; dedup by value — an API that adds counters to every response produces **no** phantom events |
| `rss` / `atom` | `max_entries` (20), `per_entry` | one event per new or edited entry; the first pass seeds state and emits nothing |

The first pass of any adapter is an introduction, not news. A source without `ETag` /
`Last-Modified` is considered noisy: events are suppressed until the watchlist says
`"allow_volatile": true` — a visible decision, not a silent loss.

## Plugins

```
# plugins/drop_bots.py
def transform(event, item):
    return event if "bot" not in event["topic"] else None   # None = drop, visibly
```

```
python3 -m hubd scan --watchlist watchlist.json --plugins plugins --once
```

`extract(item, result, state)` replaces the adapter for sources with `"plugin": "name"`;
`transform(event, item)` rewrites or drops events after any adapter. A crashing plugin is a
visible line, not a broken scan. Plugins are trusted code: they run with hubd's rights.

## The client side (WebSub)

```
python3 -m hubd listen --port 9001 --secret "rfc/*=s3cret"      # callback server, JSONL inbox
python3 -m hubd discover --url https://example.org/feed         # find the advertised hub
python3 -m hubd discover --url https://example.org/feed --subscribe \
                         --callback http://127.0.0.1:9001/cb    # …and subscribe to it
python3 -m hubd token                                           # publisher token + its sha256
```

`listen` echoes the challenge, logs `hub.mode=denied` with its reason, and stores every
delivery in a JSONL journal — an invalid signature is saved with `signature_valid: false`,
because a journal line is more honest than silence.

## Security

* **publisher tokens** — `serve --publish-tokens tokens.json`: the file holds sha256 hashes
  only (a plaintext token in the config is a read error), tokens can be scoped with fnmatch
  patterns (`"topics": ["rfc/*"]`), wrong token → `401` with a reason;
* **topic policies** — `serve --policy policy.json` with `{"allow": [...], "deny": [...]}`;
  `deny` wins over `allow`; `--topic-prefix` remains as sugar;
* **TLS** — `serve --tls-cert cert.pem --tls-key key.pem`: same protocol, https on the wire;
* by default the hub is open — that is the documented local-routine mode, not a hidden one.

## What is verified by running

| check | output |
| --- | --- |
| `python3 -m unittest discover -s tests -t .` | `Ran 153 tests … OK` |
| `python3 -m hubd demo` | `итог: проверок 11, провалено 0 — работает` |
| TLS test | raises a real https server with a self-signed certificate |
| token test | `401` without a token, scoped `401` on a foreign topic, `200` with the right one |
| adapter tests | feeds emit only new entries; a value-stable noisy API emits nothing |

## Limitations (honest ones)

See [LIMITATIONS.md](LIMITATIONS.md). Notably: in-process delivery without publisher
authentication by default, a single-file JSON store, plugins are trusted code, discovery is
regex-based for HTML pages, and the hub is one process on one store.

## Provenance and neighbours

* museum (read-only): [lost-tech-museum](https://github.com/kostyk348/lost-tech-museum),
  the [rfc-graveyard digest](https://kostyk348.github.io/lost-tech-museum/digests/rfc-graveyard/);
* archive records this grew from: `ipmulticast`, `pubsubhubbub`, `rsvp`, `atmip`, `time868`;
* pairs well with [MIME-OS / EMLBox](https://github.com/kostyk348/mime-os) — the events hubd
  delivers are containers mime-os can store; and with
  [EAFAR](https://github.com/kostyk348/eafar) when a sparse world needs to hear about changes.

## Contributing

PRs welcome — see [CONTRIBUTING.md](CONTRIBUTING.md). Changes are tracked in
[CHANGELOG.md](CHANGELOG.md).

## License

Apache-2.0 — see [LICENSE](LICENSE).

**hubd** — publish to a topic, not to a list.
