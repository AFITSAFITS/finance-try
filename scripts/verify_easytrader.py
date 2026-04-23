#!/usr/bin/env python3
from __future__ import annotations

import json
import platform
import sys

import easytrader
from easytrader.api import use


def main() -> int:
    trader = use("xq")
    payload = {
        "ok": True,
        "project": "easytrader",
        "version": getattr(easytrader, "__version__", "unknown"),
        "python": sys.version.split()[0],
        "platform": platform.platform(),
        "verified_mode": "xq",
        "trader_class": trader.__class__.__name__,
        "note": "Windows client broker automation paths are not verified in this environment.",
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
