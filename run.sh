#!/usr/bin/env bash
set -euo pipefail

# Load environment variables from .env
if [ -f .env ]; then
  set -a
  source .env
  set +a
fi

# Add uv to PATH if installed via PowerShell installer
export PATH="$HOME/.local/bin:$HOME/.cargo/bin:$PATH"

# Usage:
#   # Unified Faust consumer (tumbling window, default)
#   ./run.sh faust -A src.consumers.app worker -l info
#
#   # Hopping window mode
#   WINDOW_TYPE=hopping ./run.sh faust -A src.consumers.app worker -l info
#
#   # Late arrival test
#   ./run.sh src/consumers/publish_late.py
#   LATE_MINUTES=5 ./run.sh src/consumers/publish_late.py
#
#   # Topic management
#   ./run.sh src/consumers/manage_topic.py describe
#   ./run.sh src/consumers/manage_topic.py list

exec uv run "$@"
