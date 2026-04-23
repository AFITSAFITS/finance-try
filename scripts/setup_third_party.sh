#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORKSPACE_DIR="$(cd "${ROOT_DIR}/.." && pwd)"
THIRD_PARTY_DIR="${WORKSPACE_DIR}/third_party"
PYTHON_BIN="${PYTHON_BIN:-python3.11}"

if ! command -v "${PYTHON_BIN}" >/dev/null 2>&1; then
  echo "error: ${PYTHON_BIN} not found" >&2
  exit 1
fi

mkdir -p "${THIRD_PARTY_DIR}"

if [ ! -d "${THIRD_PARTY_DIR}/easytrader/.git" ]; then
  git clone https://github.com/shidenggui/easytrader.git "${THIRD_PARTY_DIR}/easytrader"
fi

if [ ! -d "${THIRD_PARTY_DIR}/thsdk/.git" ]; then
  git clone https://github.com/panghu11033/thsdk.git "${THIRD_PARTY_DIR}/thsdk"
fi

"${PYTHON_BIN}" -m venv "${THIRD_PARTY_DIR}/easytrader/.venv"
source "${THIRD_PARTY_DIR}/easytrader/.venv/bin/activate"
pip install -q -U pip setuptools wheel
pip install -q -e "${THIRD_PARTY_DIR}/easytrader"
deactivate

"${PYTHON_BIN}" -m venv "${THIRD_PARTY_DIR}/thsdk/.venv"
source "${THIRD_PARTY_DIR}/thsdk/.venv/bin/activate"
pip install -q -U pip setuptools wheel
pip install -q pandas thsdk
deactivate

echo "third-party setup complete"
echo "easytrader: ${THIRD_PARTY_DIR}/easytrader/.venv"
echo "thsdk: ${THIRD_PARTY_DIR}/thsdk/.venv"
