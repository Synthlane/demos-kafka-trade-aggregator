"""
Entry point — run with:

    faust -A main worker -l info

Or from this directory (same as above; no subcommand would only print CLI help and exit):

    python main.py
"""
import sys

from app import app   # noqa: F401  — Faust app instance
import agents         # noqa: F401  — registers all agents and tasks

if __name__ == "__main__":
    # `app.main()` is the Faust Click CLI. With no argv after the script name it
    # prints usage and exits — looks like "worker died silently". Default to worker.
    if len(sys.argv) == 1:
        sys.argv.extend(["worker", "-l", "info"])
    app.main()