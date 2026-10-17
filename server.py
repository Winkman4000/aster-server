"""
aster-server: runs deployed notebooks for aster-runner.

Two ways to use it: as a GitHub Actions job (run.py, started by the runner through the
GitHub API; .github/workflows/run.yml), or as a long-running HTTP server (below).

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


def runner(method, path, body=None):
    request = urllib.request.Request(
        RUNNER_URL + path, method=method,
        data=None if body is None else json.dumps(body).encode(),
        headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json",
                 "User-Agent": "aster-server/0.1"})
    with urllib.request.urlopen(request, timeout=60) as response:
        return json.loads(response.read() or b"null")


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


if __name__ == "__main__":
    if not RUNNER_URL or not TOKEN:
        print("ASTER_RUNNER_URL and ASTER_RUNNER_TOKEN must be set", flush=True)
    ThreadingHTTPServer(("0.0.0.0", int(os.environ.get("PORT", "7860"))), Handler).serve_forever()
