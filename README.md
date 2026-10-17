# aster-server

Runs [Aster](https://github.com/Winkman4000) notebooks: Python workflows edited in the
Aster desktop app and deployed to aster-runner (a Cloudflare Worker).

When a notebook is due, the runner starts the `run` workflow here with the notebook's
id. The job reads the notebook from the runner, runs its steps top to bottom
(`aster_kernel.py`, with the helpers in `aster.py`), and reports each step's result back.

No notebooks or keys live in this repo. The runner's URL and token are repository
secrets (`ASTER_RUNNER_URL`, `ASTER_RUNNER_TOKEN`).
