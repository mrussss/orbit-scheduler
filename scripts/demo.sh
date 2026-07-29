#!/usr/bin/env bash
set -Eeuo pipefail

root_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)

for command_name in curl docker go; do
  command -v "$command_name" >/dev/null || {
    echo "required command not found: $command_name" >&2
    exit 1
  }
done

if ! docker info >/dev/null 2>&1; then
  echo "Docker is installed but the daemon is not available." >&2
  exit 1
fi

echo "Orbit Scheduler — isolated Phase 5 end-to-end demo"
echo
echo "This run will:"
echo "  1. start a temporary PostgreSQL 16 container"
echo "  2. apply all production migrations"
echo "  3. launch orbit-server and two orbit-worker processes"
echo "  4. create a Project, Token, and delayed Mock task through HTTP"
echo "  5. verify gRPC fetch, lease renewal, result reporting, and querying"
echo "  6. gracefully stop both workers and remove temporary resources"
echo

"$root_dir/scripts/smoke_phase5.sh"

echo
echo "Orbit demo completed successfully."
echo "No local database, token, process, or container from this run was retained."
echo "See docs/job-ready-validation.md for the recorded acceptance evidence."
