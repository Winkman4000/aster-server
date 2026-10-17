"""
Helpers for Aster notebooks: reading the web, asking a model, Telegram, and state that
lasts between runs. Every notebook starts with `from aster import *`.

Models, Telegram and state go through aster-runner (the Cloudflare Worker) with its
token, so notebooks hold no model, Telegram or database keys of their own. The kernel
sets ASTER_RUNNER_URL, ASTER_RUNNER_TOKEN and ASTER_WORKFLOW.

Only the standard library is used, so a notebook runs on any Python 3.10+.
"""

import html as _html
import json as _json
import os as _os
import re as _re
import urllib.error as _urlerror
import urllib.parse as _urlparse
import urllib.request as _urlrequest
from concurrent.futures import ThreadPoolExecutor as _Pool
from datetime import datetime as _datetime, timezone as _timezone

__all__ = [
    "read_feed", "read_links", "read_page", "fetch", "fetch_all",
    "ask_ai", "send_telegram",
    "remember", "recall", "only_new",
    "log", "now", "slot", "local_time",
]

USER_AGENT = "aster/0.1 (personal daily news digest)"
READER_URL = "https://r.jina.ai/"   # for pages that refuse a direct request
_BLOCKED = {401, 403, 429, 503}


# On a server with IPv6 only (ASTER_IPV6_ONLY=1), a site with no IPv6 address should
# fail at once (and go through aster-runner), not wait out an IPv4 connection that
# can't succeed.
if _os.environ.get("ASTER_IPV6_ONLY"):
    import socket as _socket

    _getaddrinfo = _socket.getaddrinfo

    def _ipv6_only(host, *args, **kwargs):
        found = _getaddrinfo(host, *args, **kwargs)
        v6 = [a for a in found if a[0] == _socket.AF_INET6]
        if found and not v6:
            raise _socket.gaierror(f"{host} has no IPv6 address")
        return v6 or found

    _socket.getaddrinfo = _ipv6_only


# --- the web -----------------------------------------------------------------------

def fetch(url, *, reader=True, timeout=20):
    """The page or feed at `url` as text. If the site refuses the request, it's fetched
    once more through a reader service that returns the page's HTML. If it can't be
    reached at all from here (the server has IPv6 only, and a few sites have no IPv6
    address), aster-runner fetches it instead."""
    try:
        return _get(url, {"User-Agent": USER_AGENT}, timeout)
    except _urlerror.HTTPError as err:
        if not (reader and err.code in _BLOCKED):
            raise
        return _get(READER_URL + url, {"X-Return-Format": "html"}, timeout + 10)
    except (_urlerror.URLError, OSError):
        return _get_via_runner(url, timeout)


def fetch_all(fn, things, workers=8):
    """`fn(thing)` for each thing at the same time; a failure becomes {"error": ...}
    in that thing's place instead of stopping the rest."""
    def safe(thing):
        try:
            return fn(thing)
        except Exception as err:  # noqa: BLE001 - reported per item
            log(f"{thing}: {err}")
            return {"error": str(err), "input": thing}
    with _Pool(max_workers=workers) as pool:
        return list(pool.map(safe, things))


def read_feed(url, source=None, limit=15):
    """The newest posts of an RSS or Atom feed: [{url, title, summary, published, source}]."""
    xml = fetch(url)
    posts = []
    for _, body in _re.findall(r"<(item|entry)\b[^>]*>([\s\S]*?)</\1>", xml):
        link = _tag(body, "link") or (_re.search(r'<link\b[^>]*href="([^"]+)"', body) or [None, ""])[1]
        if not link.startswith("http"):
            continue
        posts.append({
            "url": link,
            "title": _tag(body, "title") or link,
            "summary": _tag(body, "description") or _tag(body, "summary"),
            "published": _tag(body, "pubDate") or _tag(body, "published") or _tag(body, "updated"),
            "source": source or _urlparse.urlparse(url).hostname,
        })
        if len(posts) >= limit:
            break
    return posts


def read_links(url, match, source=None, limit=15):
    """Posts listed on a page that has no feed: links whose full URL matches the regex
    `match`, with their link text as the title."""
    page = fetch(url)
    pattern = _re.compile(match)
    found = {}
    for href, inner in _re.findall(r'<a\b[^>]*?\shref="([^"]+)"[^>]*>([\s\S]*?)</a>', page):
        link = _urlparse.urljoin(url, href).split("#")[0].split("?")[0]
        if not pattern.search(link):
            continue
        text = _clean(inner)
        if len(text) > len(found.get(link, "")) or link not in found:
            found[link] = text
        if len(found) > limit:
            break
    return [{"url": u, "title": t or u, "summary": "", "published": "",
             "source": source or _urlparse.urlparse(url).hostname}
            for u, t in list(found.items())[:limit]]


def read_page(url, max_chars=10000):
    """An article's text: its paragraphs read directly, or through the reader service
    for pages that block bots or render with scripts. Empty if neither works."""
    try:
        paragraphs = [_clean(p) for p in _re.findall(r"<p\b[^>]*>([\s\S]*?)</p>", fetch(url, reader=False))]
        text = "\n".join(p for p in paragraphs if len(p) >= 40)
        if len(text) >= 300:
            return text[:max_chars]
    except Exception:  # noqa: BLE001 - fall through to the reader
        pass
    try:
        text = _get(READER_URL + url, {"X-Return-Format": "text"}, 30)
    except Exception:  # noqa: BLE001
        return ""
    lines = [line.strip() for line in text.splitlines()]
    return "\n".join(line for line in lines if len(line) >= 60)[:max_chars]


# --- models and Telegram -----------------------------------------------------------

def ask_ai(prompt, system=None, lane="news", max_tokens=500):
    """A model's answer to `prompt`, from chat-worker's model lanes (Claude Haiku first,
    with their fallbacks and daily caps)."""
    messages = ([{"role": "system", "content": system}] if system else []) + [
        {"role": "user", "content": prompt}]
    reply = _runner("/api/svc/ai", {"messages": messages, "lane": lane, "maxTokens": max_tokens})
    return (reply.get("reply") or "").strip()


def send_telegram(markdown, dry_run=False):
    """Sends `markdown` with chat-worker's Telegram bot. With dry_run, only shows it."""
    if dry_run:
        log("dry run, not sent:\n" + markdown)
        return {"dry_run": True}
    return _runner("/api/svc/telegram", {"markdown": markdown})


# --- state that lasts between runs -------------------------------------------------
#
# Stored per workflow, unless the name starts with "shared:" (every workflow sees it).

def remember(name, items, key="url"):
    """Saves each item under item[key], replacing what was there. Returns the items."""
    entries = [{"key": str(i[key]), "value": i} for i in items if i.get(key)]
    for start in range(0, len(entries), 200):
        _state("put", name, entries=entries[start:start + 200])
    return items


def recall(name, limit=50, order="newest", where=None):
    """Items saved with remember(). `where` filters on one field: {"edition": None}
    (not set yet) or {"source": "OpenAI"}."""
    args = {"limit": limit, "order": order}
    if where:
        (field, value), = where.items()
        args["where"] = {"field": field, "op": "is", "value": "null"} if value is None else \
            {"field": field, "op": "eq", "value": value}
    return [r["value"] | {"$key": r["key"]} for r in _state("list", name, **args)["records"]]


def only_new(items, name="seen", key="url", first_run_per=None):
    """The items whose item[key] hasn't been seen before; remembers them as seen.

    The first time a group shows up (everything, or per item[first_run_per], e.g.
    "source") its items are remembered but not passed on, so a new source doesn't
    flood what comes next with its back catalogue."""
    unique = {}
    for item in items:
        if item.get(key):
            unique.setdefault(str(item[key]), item)
    groups = {str(i.get(first_run_per, "")) if first_run_per else "" for i in unique.values()}
    markers = {g: f"$group:{g}" for g in groups}
    known = set(_state("has", name, keys=list(unique) + list(markers.values()))["keys"])
    fresh = {k: i for k, i in unique.items() if k not in known}
    first = {g for g, m in markers.items() if m not in known}
    if not fresh and not first:
        return []
    stamp = now()
    _state("put", name, keepExisting=True, entries=(
        [{"key": k, "value": {"seenAt": stamp}} for k in fresh] +
        [{"key": markers[g], "value": {"firstRunAt": stamp}} for g in first]))
    group = (lambda i: str(i.get(first_run_per, ""))) if first_run_per else (lambda i: "")
    for g in first:
        held = sum(1 for i in fresh.values() if group(i) == g)
        log(f"first run{' for ' + g if g else ''}: remembered {held} existing items, passed none on")
    return [i for i in fresh.values() if group(i) not in first]


# --- odds and ends -----------------------------------------------------------------

def log(message):
    """A line in the run's log (printing does the same)."""
    print(message, flush=True)


def now():
    """The time as an ISO string (UTC)."""
    return _datetime.now(_timezone.utc).isoformat(timespec="seconds")


def slot():
    """When this run was due (ISO time): the scheduled slot on the server, or now when
    you run the notebook yourself. Use it to label what a run produces."""
    return _os.environ.get("ASTER_SLOT") or now()


def local_time(iso=None, tz="America/New_York"):
    """`iso` (default: now) as a datetime in `tz`. Falls back to this machine's own
    timezone where Python has no timezone data (Windows without the tzdata package)."""
    when = _datetime.fromisoformat(iso) if iso else _datetime.now(_timezone.utc)
    try:
        from zoneinfo import ZoneInfo
        zone = ZoneInfo(tz)
    except Exception:  # noqa: BLE001
        zone = None
    if when.tzinfo is None:
        # A scheduled slot ("2026-10-17T07:00") is already local time in the schedule's zone.
        return when.replace(tzinfo=zone) if zone else when.astimezone()
    return when.astimezone(zone) if zone else when.astimezone()


def _get(url, headers, timeout):
    request = _urlrequest.Request(url, headers=headers)
    with _urlrequest.urlopen(request, timeout=timeout) as response:
        charset = response.headers.get_content_charset() or "utf-8"
        return response.read(3_000_000).decode(charset, errors="replace")


def _get_via_runner(url, timeout):
    base, token = _runner_auth()
    request = _urlrequest.Request(
        base + "/api/svc/fetch", data=_json.dumps({"url": url}).encode(), method="POST",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json",
                 "User-Agent": USER_AGENT})
    with _urlrequest.urlopen(request, timeout=timeout + 15) as response:
        status = int(response.headers.get("X-Upstream-Status", "200"))
        charset = response.headers.get_content_charset() or "utf-8"
        text = response.read(3_000_000).decode(charset, errors="replace")
    if status >= 400:
        raise RuntimeError(f"{url} returned {status} (fetched through aster-runner)")
    return text


def _runner_auth():
    url = _os.environ.get("ASTER_RUNNER_URL", "").rstrip("/")
    token = _os.environ.get("ASTER_RUNNER_TOKEN", "")
    if not url or not token:
        raise RuntimeError("not connected to aster-runner (ASTER_RUNNER_URL / ASTER_RUNNER_TOKEN unset)")
    return url, token


def _runner(path, body):
    url = _os.environ.get("ASTER_RUNNER_URL", "").rstrip("/")
    token = _os.environ.get("ASTER_RUNNER_TOKEN", "")
    if not url or not token:
        raise RuntimeError("not connected to aster-runner (ASTER_RUNNER_URL / ASTER_RUNNER_TOKEN unset)")
    request = _urlrequest.Request(
        url + path, data=_json.dumps(body).encode(), method="POST",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json",
                 "User-Agent": USER_AGENT})
    try:
        with _urlrequest.urlopen(request, timeout=90) as response:
            return _json.loads(response.read())
    except _urlerror.HTTPError as err:
        detail = err.read().decode(errors="replace")[:300]
        raise RuntimeError(f"aster-runner {path}: HTTP {err.code} {detail}") from None


def _state(op, name, **args):
    workflow = _os.environ.get("ASTER_WORKFLOW", "scratch")
    return _runner("/api/svc/state", {"workflow": workflow, "op": op, "namespace": name, **args})


def _tag(xml, name):
    m = _re.search(rf"<{name}\b[^>]*>([\s\S]*?)</{name}>", xml)
    return _clean(m.group(1)) if m else ""


def _clean(text):
    text = _re.sub(r"<!\[CDATA\[([\s\S]*?)\]\]>", r"\1", text)
    text = _re.sub(r"<[^>]+>", " ", text)
    return _re.sub(r"\s+", " ", _html.unescape(text)).strip()
