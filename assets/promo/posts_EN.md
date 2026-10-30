# Launch posts for hubd (EN)

## Hacker News (Show HN)

**Show HN: hubd — a pub/sub hub, source watcher and WebSub client in pure Python stdlib**

hubd is ~2k lines of Python 3.11+ standard library with zero dependencies that turns
"poll everything with cron" into "subscribe once":

- Publishing is honest: `200` = every subscriber confirmed, `202` = something is queued
  (exponential backoff 5s→1h, 8 attempts, survives restarts), `502` = body too large to
  queue, `503` = subscribers broke the delivery contract. No silent losses anywhere.
- The watcher does conditional GET (ETag/If-Modified-Since), sha256 dedup and 1 req/s
  politeness per host. Adapters decide what a changed body *means*: RSS/Atom produce
  per-entry events, JSON APIs produce value-changes (dedup by value, not by a noisy body
  full of counters).
- Both protocol sides ship: the hub (challenge-verified subscriptions, leases,
  `hub.mode=denied`, `X-Hub-Signature: sha256=…`) and a WebSub client (`discover` reads
  the hub a page advertises; `listen` runs a callback server with a JSONL inbox).
- Optional hardening: sha256-hashed publisher tokens with topic scopes, allow/deny topic
  policies, TLS with two flags.

Same seed of ideas as the Museum of Lost Ideas "RFC graveyard" digest: ipmulticast,
pubsubhubbub, rsvp/atmip. 153 tests + `python3 -m hubd demo` (11 checks on loopback).

https://github.com/kostyk348/hubd

## Reddit r/Python (текст поста)

I built a tiny pub/sub hub you can run anywhere Python 3.11 runs — because it uses only
the standard library, `pip install` is not needed at all. The interesting part is the
delivery contract: an unconfirmed delivery is a *state*, not silence — it sits in a
persistent queue with backoff, and when attempts run out the hub marks the subscription
`degraded` and answers 503 with a reason. The watcher + adapters turn RSS/Atom/JSON-API
changes into events; there's also a WebSub client side (discovery + callback server).
Happy to answer questions about the retry queue design.

## Post for X/Twitter

hubd 1.0: a pub/sub hub + source watcher in pure Python stdlib.
Topics instead of address lists, leases instead of eternal subs,
backoff retries instead of silent losses. WebSub both sides, 153 tests.
https://github.com/kostyk348/hubd
