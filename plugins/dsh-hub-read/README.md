# dsh-hub-read (0.1.0-dev)

Thin read-only Cordis plugin exposing knowledge-hub **read** tools to dsh profiles.

- `hub_browse` — browse the ontology catalog (paginated; miss = structured `items: []`).
- `hub_resolve` — resolve one ontology id (miss = structured `{found:false}`).
- `hub_neighborhood_read` — one-hop neighborhood; **default-off** (only mounted
  when the profile config sets `neighborhoodEnabled: true`; the wire itself
  rejects calls without `enabled=True`).

## How it calls the Hub

Every tool call spawns one real subprocess:

```
cwd  = <hubRepoRoot>            # default /workspace/hermes-work/knowledge-hub
env  PYTHONPATH = <hubRepoRoot>/src
cmd  = python3 -u -c "<tiny driver importing knowledge_hub.dsh_read_wire>"
```

The driver (`lib/wire.js` → `WIRE_DRIVER`) imports the merged wire module
(`hub-kg-dsh-hub-read-wire-v1-s2`, PR #86) and calls `hub_browse` /
`hub_resolve` / `hub_neighborhood_read` with the exact Python signatures.
No HTTP, no reimplementation, no write/promote/apply/CSV path.

## Hard rules

- **Bank pinned to `dsh-dev`** (the wire's `DEV_SCOPE_PLACEHOLDER`); the tools
  do not accept a bank argument and the wire rejects anything else.
- **Read-only**: only the three read entrypoints are reachable.
- **Default-off neighborhood**: gated both at registration (profile config)
  and at the wire (`enabled=True` required).
- **Honest empties**: misses return structured empties, never retried.

## Config (`cordis.patch.yml` insert id `hub-read`)

| key | default | meaning |
| --- | --- | --- |
| `hubRepoRoot` | `/workspace/hermes-work/knowledge-hub` | absolute path to the Hub checkout |
| `pythonBin` | `python3` | Python interpreter |
| `neighborhoodEnabled` | `false` | mount `hub_neighborhood_read` |

## Files

- `lib/index.js` — Cordis entry (`name`, `inject = ['tools']`, `apply`).
- `lib/wire.js` — subprocess bridge + tool definitions (pure, testable).
- `cordis.patch.yml` — bundle patch row.
- `test/wire.test.mjs` — `node --test` unit tests (subprocess mocked).

## Test

```sh
npm test        # node --test test/wire.test.mjs
npm run check   # node --check on both lib files
```

## Boundaries

- Dev scaffold; **do not merge into broker-khub-prod profiles** until
  parent/user approves.
- Plugin stays read-only: no write/promote/apply tool will be added here.
