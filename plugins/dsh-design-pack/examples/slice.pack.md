---
slice_id: example-20260924-demo
role: hub-foreman
goal: Demo medium ticket — read this pack only; do not join room catch-up for context.
done_when:
  - Confirm pack fields readable
  - Write a 5-line summary to summary_out
  - Close ticket (do not keep sticky)
contrast_paths:
  - /workspace/docs/2026-09-24-dsh-route-c-plugin-gap.md
forbidden:
  - Knowledge Hub business src/ edits
  - Restart product broker
  - Writing NEW_API_KEY
parent_index: /home/box/.dsh/supervisor/thin-state/index.json
summary_out: /home/box/.dsh/supervisor/thin-state/summaries/example-20260924-demo.md
max_bytes_hint: 12288
---

# Slice: example-20260924-demo

## Context (bounded)

This is a **scaffold example**. Real packs replace this body with the slice
spec, acceptance checks, and pointers. Prefer links + offset reads over paste.

## Non-goals

- No team-rooms inject required for this slice.
- No Hermes dependency on this dsh plugin path.

## Close checklist

1. `summary_out` written (status, artifacts, blockers).
2. If Goal says so: `khub-dsh-complete-notify.py` (existing script).
3. `session/close` / ticket retire — do not leave a fat sticky session.
