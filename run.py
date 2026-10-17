"""One notebook run, as a GitHub Actions job: the runner starts it with workflow_dispatch."""

import os
import sys

from server import run_notebook

status = run_notebook(os.environ["WORKFLOW"], os.environ.get("TRIGGER") or "manual", os.environ.get("SLOT", ""))
print(f"{os.environ['WORKFLOW']}: {status}", flush=True)
sys.exit(0 if status == "done" else 1)
