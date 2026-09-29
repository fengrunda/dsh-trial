#!/usr/bin/env python3
"""Fail if a dsh session jsonl(.zstd) contains team-rooms bus→model injects.

Route C Option C: executors must NOT be room members, so session history
must not contain room bus deliveries as user/message.

Heuristics (any hit → exit 1):
  1. user/message content starts with "[team-room "
  2. source.plugin in {dsh-team-rooms, dsh-background-agents}
     AND source.form == "relay"   (bus deliverPosted / catchUp)
  3. same plugins + form == "notice" AND text looks like a room brief
     (contains "team room" / "Room id:" / "You are a helpful assistant in team room")

Exit 0 if clean (or empty session). Prints a short JSON summary to stdout.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any


ROOM_PLUGINS = {"dsh-team-rooms", "dsh-background-agents"}
BRIEF_MARKERS = (
    "you are a helpful assistant in team room",
    "room id:",
    "team room ",
)


def load_events(path: Path) -> list[dict[str, Any]]:
    s = str(path)
    if s.endswith(".zstd") or path.suffix == ".zstd":
        text = subprocess.check_output(["zstd", "-dc", s], text=True, errors="replace")
    else:
        text = path.read_text(errors="replace")
    out: list[dict[str, Any]] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            out.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return out


def text_of(content: Any) -> str:
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    chunks: list[str] = []

    def walk(x: Any) -> None:
        if isinstance(x, str):
            chunks.append(x)
        elif isinstance(x, dict):
            if isinstance(x.get("text"), str):
                chunks.append(x["text"])
            for k, v in x.items():
                if k != "text":
                    walk(v)
        elif isinstance(x, list):
            for i in x:
                walk(i)

    walk(content)
    return "\n".join(chunks)


def classify_hit(src: dict[str, Any], text: str) -> str | None:
    plugin = src.get("plugin") or ""
    form = src.get("form") or ""
    low = text.lower()
    if text.lstrip().startswith("[team-room "):
        return "content:[team-room"
    if plugin in ROOM_PLUGINS and form == "relay":
        return f"plugin:{plugin}/form:relay"
    if plugin in ROOM_PLUGINS and form == "notice":
        if any(m in low for m in BRIEF_MARKERS):
            return f"plugin:{plugin}/form:notice-brief"
    # content-only brief / bus without source (defensive)
    if text.lstrip().startswith("[team-room"):
        return "content:[team-room-loose"
    return None


def scan(path: Path) -> dict[str, Any]:
    events = load_events(path)
    hits: list[dict[str, Any]] = []
    user_msgs = 0
    for i, o in enumerate(events):
        if o.get("type") != "user/message":
            continue
        user_msgs += 1
        d = o.get("data") or {}
        src = d.get("source") or {}
        if not isinstance(src, dict):
            src = {}
        text = text_of(d.get("content"))
        reason = classify_hit(src, text)
        if reason:
            hits.append(
                {
                    "event_index": i,
                    "reason": reason,
                    "plugin": src.get("plugin"),
                    "form": src.get("form"),
                    "kind": src.get("kind"),
                    "text_preview": text[:160].replace("\n", " "),
                }
            )
    return {
        "path": str(path),
        "user_message_count": user_msgs,
        "room_inject_hits": len(hits),
        "clean": len(hits) == 0,
        "hits": hits[:20],
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("session", type=Path, help="session.v3.jsonl or .jsonl.zstd")
    ap.add_argument("--json", action="store_true", help="always print full JSON")
    args = ap.parse_args()
    if not args.session.exists():
        print(f"ERROR: missing session file: {args.session}", file=sys.stderr)
        return 2
    result = scan(args.session)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["clean"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
