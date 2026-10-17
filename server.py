"""
aster-server: runs deployed notebooks for aster-runner.

Ways to run it:
  python server.py --poll   always-on, asking the runner for queued runs (the default setup)
  python server.py          an HTTP server the runner calls (SERVER_URL)
  python run.py             one run, as a GitHub Actions job (.github/workflows/run.yml)

The runner (Cloudflare Worker) keeps the schedules; when a notebook is due, or someone
asks for a run, it calls POST /run here. This server reads the notebook from the runner,
runs its cells top to bottom in a fresh Python session (aster_kernel.py), and reports
every step's result back to the runner, which saves the run in Supabase.

  GET  /        health check (also what keeps the Space awake)
  POST /run     {"workflow": id, "trigger": "schedule", "slot": "..."}  -> 202
                header X-Aster-Token: the runner's token

Environment (Space secrets): ASTER_RUNNER_URL, ASTER_RUNNER_TOKEN.
"""

import hmac
import json
import os
import subprocess
import sys
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HERE = os.path.dirname(os.path.abspath(__file__))
RUNNER_URL = os.environ.get("ASTER_RUNNER_URL", "").rstrip("/")
TOKEN = os.environ.get("ASTER_RUNNER_TOKEN", "").strip()
CELL_TIMEOUT = 600            # seconds one step may take
KEEP = {"output": 4000, "error": 4000, "value": 8000}

_running = set()              # workflows with a run in progress
_lock = threading.Lock()

# IPv6-only server: talk to the runner over IPv6 rather than trying IPv4 first.
if os.environ.get("ASTER_IPV6_ONLY"):
    import socket

    _getaddrinfo = socket.getaddrinfo
    socket.getaddrinfo = lambda *a, **k: [x for x in _getaddrinfo(*a, **k) if x[0] == socket.AF_INET6] or _getaddrinfo(*a, **k)


# One kept-alive connection to the runner per thread. Outbound traffic is what the free
# tier meters (1 GB a month), and a fresh TLS handshake for every poll cost ten times
# more than the poll itself.
_conn = threading.local()


def runner(method, path, body=None):
    import http.client
    import urllib.parse
    host = urllib.parse.urlparse(RUNNER_URL).netloc
    data = None if body is None else json.dumps(body, separators=(",", ":")).encode()
    headers = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json",
               "User-Agent": "aster-server/0.1"}
    for attempt in (1, 2):
        conn = getattr(_conn, "c", None)
        if conn is None:
            conn = _conn.c = http.client.HTTPSConnection(host, timeout=30)
        try:
            conn.request(method, path, body=data, headers=headers)
            response = conn.getresponse()
            payload = response.read()
        except (http.client.HTTPException, OSError):
            conn.close()
            _conn.c = None
            if attempt == 2:
                raise
            continue
        if response.status >= 400:
            raise RuntimeError(f"runner {path}: HTTP {response.status} {payload[:200]!r}")
        return json.loads(payload or b"null")


def notebook(workflow):
    for w in runner("GET", "/api/workflows")["workflows"]:
        if w["id"] == workflow:
            return w["definition"]
    raise LookupError(f"no workflow {workflow!r}")


def trim(value, limit):
    text = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False)
    return value if len(text) <= limit else {"$truncated": text[:limit]}


def run_notebook(workflow, trigger, slot):
    """Runs a deployed notebook and reports it; returns "done" or "failed"."""
    run_id = None
    status = "failed"
    try:
        nb = notebook(workflow)
        cells = [{"title": c.get("title", ""), "status": "queued"} for c in nb.get("cells", [])]
        progress = {"kind": "python", "slot": slot, "cells": cells}
        run_id = runner("POST", "/api/svc/runs", {"workflow": workflow, "trigger": trigger,
                                                   "progress": progress})["id"]
        env = dict(os.environ, ASTER_WORKFLOW=workflow, ASTER_SLOT=slot or "",
                   PYTHONPATH=HERE, PYTHONIOENCODING="utf-8")
        kernel = subprocess.Popen([sys.executable, "-u", os.path.join(HERE, "aster_kernel.py")],
                                  stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True,
                                  encoding="utf-8", env=env, cwd=HERE)
        failed = None
        try:
            for i, cell in enumerate(nb.get("cells", [])):
                cells[i]["status"] = "running"
                runner("POST", f"/api/svc/runs/{run_id}", {"status": "running", "progress": progress})
                kernel.stdin.write(json.dumps({"op": "run", "id": str(i), "code": cell.get("code", "")}) + "\n")
                kernel.stdin.flush()
                reply = read_reply(kernel)
                cells[i] = {
                    "title": cell.get("title", ""),
                    "status": "done" if reply.get("ok") else "failed",
                    "ms": reply.get("ms", 0),
                    "count": reply.get("count"),
                    "output": reply.get("stdout", "")[-KEEP["output"]:],
                    "error": reply.get("error", "")[-KEEP["error"]:],
                    "value": trim(reply.get("value"), KEEP["value"]),
                }
                if not reply.get("ok"):
                    failed = f"step {i + 1} ({cell.get('title') or 'untitled'}) failed"
                    for rest in cells[i + 1:]:
                        rest["status"] = "skipped"
                    break
        finally:
            kernel.kill()
        status = "failed" if failed else "done"
        runner("POST", f"/api/svc/runs/{run_id}", {"status": status, "error": failed, "progress": progress})
    except Exception as err:  # noqa: BLE001 - reported on the run
        print(f"run {workflow}: {err}", flush=True)
        if run_id is not None:
            try:
                runner("POST", f"/api/svc/runs/{run_id}", {"status": "failed", "error": str(err),
                                                           "progress": {"kind": "python"}})
            except Exception:  # noqa: BLE001
                pass
    finally:
        with _lock:
            _running.discard(workflow)
    return status


def read_reply(kernel):
    result = {}

    def read():
        for line in kernel.stdout:
            try:
                result.update(json.loads(line))
                return
            except json.JSONDecodeError:
                continue

    reader = threading.Thread(target=read, daemon=True)
    reader.start()
    reader.join(CELL_TIMEOUT)
    if reader.is_alive():
        kernel.kill()
        return {"ok": False, "error": f"Took longer than {CELL_TIMEOUT} seconds; stopped."}
    return result or {"ok": False, "error": "The Python session stopped."}


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.reply(200, {"ok": True, "running": sorted(_running)})

    def do_POST(self):
        if self.path != "/run":
            return self.reply(404, {"error": "not found"})
        given = self.headers.get("X-Aster-Token", "")
        if not TOKEN or not hmac.compare_digest(given.encode(), TOKEN.encode()):
            return self.reply(401, {"error": "unauthorized"})
        try:
            body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0)) or 0) or b"{}")
        except json.JSONDecodeError:
            return self.reply(400, {"error": "body must be JSON"})
        workflow = body.get("workflow", "")
        with _lock:
            if workflow in _running:
                return self.reply(409, {"error": f"{workflow} is already running"})
            _running.add(workflow)
        threading.Thread(target=run_notebook, daemon=True,
                         args=(workflow, body.get("trigger", "manual"), body.get("slot", ""))).start()
        self.reply(202, {"started": workflow})

    def reply(self, status, data):
        payload = json.dumps(data).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, fmt, *args):
        print(time.strftime("%H:%M:%S"), fmt % args, flush=True)


POLL_SECONDS = 30            # how often to ask for queued runs
STORAGE_EVERY = 600           # seconds between Cloud Storage size checks

METADATA = "http://metadata.google.internal/computeMetadata/v1/"


def metadata(path):
    """A value from the Google Cloud metadata server ("" when not on Google Cloud)."""
    try:
        request = urllib.request.Request(METADATA + path, headers={"Metadata-Flavor": "Google"})
        with urllib.request.urlopen(request, timeout=3) as response:
            return response.read().decode()
    except Exception:  # noqa: BLE001
        return ""


def machine():
    """What this machine is: provider, type, zone, CPUs (sent with every heartbeat)."""
    zone = metadata("instance/zone").rsplit("/", 1)[-1]
    kind = metadata("instance/machine-type").rsplit("/", 1)[-1]
    return {
        "provider": "Google Cloud" if zone else "",
        "machineType": kind, "zone": zone, "project": metadata("project/project-id"),
        "cpus": os.cpu_count() or 0, "python": sys.version.split()[0],
    }


def stats():
    """Disk, memory, load and uptime right now."""
    import shutil
    disk = shutil.disk_usage("/")
    mem = {}
    try:
        for line in open("/proc/meminfo"):
            key, value = line.split(":", 1)
            mem[key] = int(value.split()[0]) * 1024
    except OSError:
        pass
    try:
        uptime = float(open("/proc/uptime").read().split()[0])
    except OSError:
        uptime = 0
    load = os.getloadavg() if hasattr(os, "getloadavg") else (0, 0, 0)
    return {
        "diskTotal": disk.total, "diskUsed": disk.used,
        "memTotal": mem.get("MemTotal", 0), "memAvailable": mem.get("MemAvailable", 0),
        "swapTotal": mem.get("SwapTotal", 0), "swapFree": mem.get("SwapFree", 0),
        "load": [round(x, 2) for x in load], "uptime": round(uptime),
    }


def cloud_storage_bytes(project):
    """Bytes in this project's Cloud Storage buckets (the free tier includes 5 GB), using
    the VM's own service account. None when it can't be read."""
    try:
        token = json.loads(metadata("instance/service-accounts/default/token") or "{}").get("access_token")
        if not token or not project:
            return None

        def get(url):
            request = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
            with urllib.request.urlopen(request, timeout=20) as response:
                return json.loads(response.read())

        total = 0
        api = "https://storage.googleapis.com/storage/v1"
        for bucket in get(f"{api}/b?project={project}").get("items", []):
            page = ""
            while True:
                listing = get(f"{api}/b/{bucket['name']}/o?fields=items(size),nextPageToken" + page)
                total += sum(int(o.get("size", 0)) for o in listing.get("items", []))
                if not listing.get("nextPageToken"):
                    break
                page = "&pageToken=" + listing["nextPageToken"]
        return total
    except Exception:  # noqa: BLE001
        return None


def poll():
    """Asks the runner for queued runs every few seconds and starts them. Needs no open
    port: every connection goes out from here."""
    import socket
    host = socket.gethostname()
    about = machine()
    last_beat = 0
    storage, storage_at = None, 0
    while True:
        try:
            beat = time.time() - last_beat >= 60
            with _lock:
                running = sorted(_running)
            body = {"heartbeat": beat, "host": host, "running": running}
            if beat:
                if time.time() - storage_at >= STORAGE_EVERY:
                    storage, storage_at = cloud_storage_bytes(about["project"]), time.time()
                body["machine"] = about
                body["stats"] = stats() | {"cloudStorage": storage}
            jobs = runner("POST", "/api/svc/claim", body)["jobs"]
            if beat:
                last_beat = time.time()
            for job in jobs:
                workflow = job["workflow"]
                with _lock:
                    if workflow in _running:
                        print(f"{workflow} is already running; skipped", flush=True)
                        continue
                    _running.add(workflow)
                print(time.strftime("%H:%M:%S"), "run", workflow, job.get("trigger", ""), flush=True)
                threading.Thread(target=run_notebook, daemon=True,
                                 args=(workflow, job.get("trigger", "manual"), job.get("slot", ""))).start()
        except Exception as err:  # noqa: BLE001 - keep polling through outages
            print(time.strftime("%H:%M:%S"), "poll:", err, flush=True)
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    if not RUNNER_URL or not TOKEN:
        sys.exit("ASTER_RUNNER_URL and ASTER_RUNNER_TOKEN must be set")
    if "--poll" in sys.argv:
        poll()
    else:
        ThreadingHTTPServer(("0.0.0.0", int(os.environ.get("PORT", "7860"))), Handler).serve_forever()
