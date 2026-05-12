#!/usr/bin/env python3
"""Enforce Cognis license posture on a ScanCode JSON report.

Allowlist: MIT, Apache-2.0, BSD-2/3-Clause, ISC, MPL-2.0, Unlicense, CC0-1.0.
Blocklist (explicit): SSPL, BUSL-*, Elastic, Commons-Clause, FSL-*, AGPL-*,
PolyForm-*, "Open WebUI License", "Chatwoot Enterprise License",
"Sustainable Use License".

Exits non-zero on any blocked license OR any non-allowlisted license found.
"""

import json
import sys
from pathlib import Path

ALLOWED = {
    "mit",
    "apache-2.0",
    "bsd-2-clause",
    "bsd-3-clause",
    "isc",
    "mpl-2.0",
    "unlicense",
    "cc0-1.0",
    # ScanCode emits these for common boilerplate; not licenses per se
    "public-domain",
    "boost-1.0",
    "zlib",
}

BLOCKED_PREFIXES = (
    "agpl",
    "gpl-3",  # GPLv3 — would conflict with Apache-2.0 mixing under our redistribution model
    "sspl",
    "busl",
    "elastic-license",
    "commons-clause",
    "fsl",
    "polyform",
    "open-webui",
    "chatwoot-enterprise",
    "sustainable-use",
    "tongyi-qianwen",
)

IGNORED_PATHS = (
    "node_modules/",
    "tmp/",
    "log/",
    "vendor/bundle/",
    ".git/",
)


def is_ignored(path: str) -> bool:
    return any(seg in path for seg in IGNORED_PATHS)


def main(report_path: str) -> int:
    data = json.loads(Path(report_path).read_text(encoding="utf-8"))
    files = data.get("files", [])

    blocked = []
    unknown = []

    for f in files:
        path = f.get("path", "")
        if is_ignored(path):
            continue
        for det in f.get("license_detections", []) or []:
            key = (det.get("license_expression") or "").lower()
            if not key:
                continue
            # Block exact match against blocklist prefixes
            if any(key.startswith(p) for p in BLOCKED_PREFIXES):
                blocked.append((path, key))
                continue
            # Allowlist contains?
            tokens = [t.strip() for t in key.replace("(", " ").replace(")", " ").split()]
            non_allowed = [
                t
                for t in tokens
                if t not in ALLOWED and t not in ("and", "or", "with")
            ]
            if non_allowed:
                unknown.append((path, key))

    if blocked:
        print("ERROR: blocked licenses detected:", file=sys.stderr)
        for p, k in blocked:
            print(f"  {p}: {k}", file=sys.stderr)
    if unknown:
        print("ERROR: licenses not on allowlist (review manually):", file=sys.stderr)
        for p, k in unknown:
            print(f"  {p}: {k}", file=sys.stderr)

    if blocked or unknown:
        return 1

    print("OK: all detected licenses are on the Cognis allowlist.")
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("usage: check_no_proprietary.py <scancode.json>", file=sys.stderr)
        sys.exit(2)
    sys.exit(main(sys.argv[1]))
