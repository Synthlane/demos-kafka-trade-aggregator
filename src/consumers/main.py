"""
Entry point — run with:

    faust -A main worker -l info
"""
from app import app   # noqa: F401  — Faust app instance
import agents         # noqa: F401  — registers all agents and tasks

if __name__ == "__main__":
    app.main()