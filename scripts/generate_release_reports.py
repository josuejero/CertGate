from __future__ import annotations

import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
SRC_ROOT = BASE_DIR / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from certgate.cli import main as generate_release_reports_main


if __name__ == "__main__":
    generate_release_reports_main()
