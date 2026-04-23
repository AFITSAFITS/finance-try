#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORKSPACE_DIR="$(cd "${ROOT_DIR}/.." && pwd)"
PYTHON_BIN="${WORKSPACE_DIR}/third_party/thsdk/.venv/bin/python"

if [ ! -x "${PYTHON_BIN}" ]; then
  echo "error: thsdk venv not found, run scripts/setup_third_party.sh first" >&2
  exit 1
fi

exec "${PYTHON_BIN}" "${ROOT_DIR}/scripts/verify_thsdk.py" "$@"
