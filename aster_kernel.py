"""
Runs notebook cells for Aster, one Python session per notebook.

Aster starts this with `python -u aster_kernel.py` and talks to it in JSON lines:

  -> {"op": "run", "id": "c3", "code": "..."}
  <- {"id": "c3", "ok": true, "stdout": "...", "value": <preview>, "count": 12, "ms": 840}
  -> {"op": "reset"}                 (a fresh session, as if the notebook just opened)
  <- {"reset": true}

Cells share one namespace, top to bottom, like a notebook. A cell's value is its last
line when that's an expression (as in Jupyter or marimo); lists also report a count.
"""

import ast
import contextlib
import io
import json
import sys
import time
import traceback

PREVIEW_CHARS = 40_000

_protocol = sys.stdout  # replies go here; cells' prints are captured separately


def fresh_namespace():
    ns = {"__name__": "__notebook__"}
    exec("from aster import *", ns)
    return ns


def preview(value):
    """JSON for Aster to show, cut to a sensible size."""
    try:
        text = json.dumps(value, ensure_ascii=False, indent=2, default=repr)
    except Exception:  # noqa: BLE001
        text = json.dumps(repr(value))
    if len(text) > PREVIEW_CHARS:
        return {"$truncated": text[:PREVIEW_CHARS]}
    return json.loads(text)


def run_cell(code, ns):
    """Executes one cell; returns (value, has_value)."""
    tree = ast.parse(code, "<cell>", "exec")
    last = tree.body[-1] if tree.body else None
    if isinstance(last, ast.Expr):
        tree.body.pop()
        exec(compile(tree, "<cell>", "exec"), ns)
        return eval(compile(ast.Expression(last.value), "<cell>", "eval"), ns), True
    exec(compile(tree, "<cell>", "exec"), ns)
    return None, False


def cell_traceback():
    """The traceback from the cell's own code down, without the kernel's frames."""
    etype, err, tb = sys.exc_info()
    frames = [f for f in traceback.extract_tb(tb) if f.filename != __file__]
    lines = traceback.format_list(frames) + traceback.format_exception_only(etype, err)
    return "".join(lines).rstrip()


def main():
    ns = fresh_namespace()
    for line in sys.stdin:
        try:
            msg = json.loads(line)
        except json.JSONDecodeError:
            continue
        if msg.get("op") == "reset":
            ns = fresh_namespace()
            reply = {"reset": True}
        elif msg.get("op") == "run":
            out = io.StringIO()
            start = time.perf_counter()
            reply = {"id": msg.get("id")}
            try:
                with contextlib.redirect_stdout(out), contextlib.redirect_stderr(out):
                    value, has_value = run_cell(msg.get("code", ""), ns)
                reply["ok"] = True
                if has_value and value is not None:
                    reply["value"] = preview(value)
                    if isinstance(value, (list, tuple, dict)):
                        reply["count"] = len(value)
            except BaseException:  # noqa: BLE001 - every error goes back to the cell
                reply["ok"] = False
                reply["error"] = cell_traceback()
            reply["stdout"] = out.getvalue()[-PREVIEW_CHARS:]
            reply["ms"] = round((time.perf_counter() - start) * 1000)
        else:
            reply = {"error": f"unknown op {msg.get('op')!r}"}
        _protocol.write(json.dumps(reply, ensure_ascii=False) + "\n")
        _protocol.flush()


if __name__ == "__main__":
    main()
