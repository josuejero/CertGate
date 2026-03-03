#!/usr/bin/env bash
set -euo pipefail

SUPPORTED_PYTHON_MAJOR=3
SUPPORTED_PYTHON_MIN_MINOR=10
SUPPORTED_PYTHON_MAX_MINOR=13

python_version_supported() {
  local version=$1
  local major minor
  local IFS='.'
  read -r major minor _ <<<"$version"
  minor=${minor:-0}
  if [[ -z "$major" ]] || [[ "$major" -ne $SUPPORTED_PYTHON_MAJOR ]]; then
    return 1
  fi
  if (( minor < SUPPORTED_PYTHON_MIN_MINOR || minor > SUPPORTED_PYTHON_MAX_MINOR )); then
    return 1
  fi
  return 0
}

python_version_from() {
  local cmd=$1
  local version
  if ! version=$("$cmd" - <<'PY' 2>/dev/null
import sys
sys.stdout.write(f"{sys.version_info.major}.{sys.version_info.minor}")
PY
  ); then
    return 1
  fi
  echo "$version"
}

find_supported_python_cmd() {
  local candidate
  local version
  local candidates=(python3.13 python3.12 python3.11 python3.10 python3 python)
  for candidate in "${candidates[@]}"; do
    if ! command -v "$candidate" >/dev/null 2>&1; then
      continue
    fi
    if version=$(python_version_from "$candidate"); then
      if python_version_supported "$version"; then
        echo "$candidate"
        return 0
      fi
    fi
  done
  return 1
}

PYTHON_CMD=${PYTHON_CMD:-}
if [[ -n "$PYTHON_CMD" ]]; then
  if ! command -v "$PYTHON_CMD" >/dev/null 2>&1; then
    echo "ERROR: $PYTHON_CMD is not available on PATH. Provide a supported interpreter (Python 3.10–3.13)."
    exit 1
  fi
  if ! version=$(python_version_from "$PYTHON_CMD"); then
    echo "ERROR: Unable to detect the version of $PYTHON_CMD."
    exit 1
  fi
  if ! python_version_supported "$version"; then
    echo "ERROR: $PYTHON_CMD is Python $version, which falls outside the supported 3.10–3.13 range."
    echo "Set PYTHON_CMD to a supported interpreter (for example python3.13) and rerun the bootstrap script."
    exit 1
  fi
else
  if ! PYTHON_CMD=$(find_supported_python_cmd); then
    echo "ERROR: No supported Python 3 interpreter (3.10–3.13) found on PATH. Install one and rerun the bootstrap script."
    exit 1
  fi
fi

if [[ ! -d ".venv" || ! -x ".venv/bin/python" ]]; then
  echo "Creating a virtual environment with $PYTHON_CMD."
  "$PYTHON_CMD" -m venv .venv
else
  venv_version=""
  if venv_version=$(python_version_from ".venv/bin/python"); then
    if python_version_supported "$venv_version"; then
      echo "Reusing existing .venv virtual environment (Python $venv_version)."
    else
      echo "Existing .venv uses Python $venv_version, which is not supported."
      echo "Recreating .venv with $PYTHON_CMD."
      rm -rf .venv
      "$PYTHON_CMD" -m venv .venv
    fi
  else
    echo "Existing .venv is unusable (could not read its Python interpreter)."
    echo "Recreating .venv with $PYTHON_CMD."
    rm -rf .venv
    "$PYTHON_CMD" -m venv .venv
  fi
fi

# shellcheck source=/dev/null
source ".venv/bin/activate"
python -m pip install --upgrade pip
python -m pip install -r requirements.txt

deactivate >/dev/null 2>&1 || true

cat <<'MSG'
Bootstrap complete. Activate the environment with 'source .venv/bin/activate' before running python scripts.
MSG
