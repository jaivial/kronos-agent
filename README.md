# Kronos Agent Pipeline

Multi-agent orchestration SDK where every agent is a fresh `claude -p` subprocess. SQLite is the shared brain. FastAPI is the router. No orchestrator agent needed.

```
kronos_submit(prompt="fix the login bug")
│
▼
┌─────────────────────────────────────────────────────────────┐
│ SDK (FastAPI :8199) — router, NOT an agent                  │
│ Creates task_flow row → spawns first agent in bg thread     │
└─────────────────────┬───────────────────────────────────────┘
                      │  fresh `claude -p` subprocess
                      ▼
┌──────────────────────────┐
│ 1. wide_research         │  Qdrant code search → affected files,
│    (25 turns, 600s)      │  impact scores → writes react_flow graph
│    Tools: kraken_find,   │  to SQLite + artifact to disk
│    kraken_impact         │
└──────────┬───────────────┘
           │ agent_step_complete → POST /internal/step-complete
           ▼
┌──────────────────────────┐
│ 2. prompt_enhancer       │  Raw prompt → structured spec:
│    (15 turns, 300s)      │  intent, scope, acceptance criteria,
│    Tools: db_read/write  │  risks, enhanced_prompt
└──────────┬───────────────┘
           │
           ▼
┌──────────────────────────┐
│ 3. planner               │  Spec → ordered phases with skill
│    (30 turns, 600s)      │  assignments. Each phase gets its
│    Tools: db_read/write, │  own executor subprocess.
│    kraken_find           │
└──────────┬───────────────┘
           │
           ▼
┌──────────────────────────┐
│ 4a. executor (phase 1)   │  Each phase = separate `claude -p`
│ 4b. executor (phase 2)   │  Fresh process, cold start.
│ 4c. executor (phase N)   │  Files from previous phases ARE on disk.
│    (50 turns, 900s each) │  ONLY agent that modifies code.
│    Tools: Read/Write/    │
│    Edit/Bash             │
└──────────┬───────────────┘
           │
           ▼
┌──────────────────────────┐
│ 5. validator_1           │  Build gate. Runs tsc --noEmit +
│    (20 turns, 600s)      │  lint + build. PASS/FAIL with
│    Tools: Bash, Read     │  exact file:line errors.
└──────────┬───────────────┘
           │ FAIL → retry from phase 1 executor (max 3)
           │ PASS ↓
           ▼
┌──────────────────────────┐
│ 6. validator_2           │  Browser test via agent-browser
│    (30 turns, 900s)      │  MCP. Drives real Chrome, checks
│    Tools: agent-browser  │  console errors, screenshots.
│    MCP                   │  PASS/FAIL/INCONCLUSIVE.
└──────────┬───────────────┘
           │
           ▼
     flow status = done
     webhooks fire
     WebSocket broadcasts
```

## Architecture

| Component | Role |
|-----------|------|
| `agent_pipeline.py` | FastAPI SDK — pipeline router, REST API, WebSocket, webhook dispatch |
| `agent_spawner.py` | Builds `claude -p` commands with role prompts + MCP configs. Supports per-flow binary selection (`claude` or `claudio`) |
| `agent_mcp.py` | Notification MCP — `agent_step_complete` callback to SDK |
| `sqlite_mcp.py` | Data MCP — agents read flow state, write artifacts, store react_flow graphs |
| `agent_db.py` | SQLite WAL data layer — task_flows, steps, events, skills, subagents, webhooks |
| `pipeline_config.py` | Centralized config — all tuneable params via env vars. Auto-resolves `claude` binary via `shutil.which()` |
| `inbox_listener.py` | IMAP polling + Mailgun webhook for email-based task submission |
| `kronos_pipeline_mcp.py` | Claude session MCP — submit/monitor from any Claude or claudio session |
| `kanban-ui/` | React + @xyflow/react — dark theme flow list + impact graph viewer |

## Install

```bash
# From GitHub
npm install jaivial/kronos-agent

# Or from local
npm install /path/to/Kronos-Pipeline
```

The postinstall script automatically:
1. Bootstraps the SQLite database
2. Installs Python deps (fastapi, uvicorn, pydantic)
3. Installs + starts `kronos-pipeline.service` via systemd
4. Registers `kronos-pipeline` MCP in `~/.claude/settings.json` + `~/.claudio/settings.json`
5. Smoke tests SDK + MCP

After install, **restart your Claude session** for MCP tools to appear.

## Usage

### From any Claude/claudio session (MCP)

```
kronos_submit(prompt="fix the login button z-index on mobile", project_path="/var/www/kraken/Dashboard")
```

Use `claudio` instead of `claude` per task:

```
kronos_submit(prompt="fix the login bug", claude_bin="claudio")
```

8 MCP tools available: `kronos_submit`, `kronos_status`, `kronos_list`, `kronos_cancel`, `kronos_retry`, `kronos_react_flow`, `kronos_step_detail`, `kronos_stats`

### Via HTTP API

```bash
# Submit task (defaults to auto-detected claude binary)
curl -X POST http://localhost:8199/api/submit \
  -H 'Content-Type: application/json' \
  -d '{"prompt":"fix the header overflow bug","project_path":"/var/www/kraken/Dashboard"}'

# Submit task with claudio binary
curl -X POST http://localhost:8199/api/submit \
  -H 'Content-Type: application/json' \
  -d '{"prompt":"fix the header overflow bug","project_path":"/var/www/kraken/Dashboard","claude_bin":"claudio"}'

# Check status
curl http://localhost:8199/api/flows/{flow_id}

# List flows
curl http://localhost:8199/api/flows

# Get impact graph
curl http://localhost:8199/api/flows/{flow_id}/react-flow
```

### Via email

Send an email with subject `[pipeline] /path/to/project` — the body becomes the task prompt. Configure IMAP or Mailgun webhook.

## Binary Selection (`claude` vs `claudio`)

Every task flow can choose which CLI binary to use. This lets external apps route tasks to different AI backends dynamically.

| Parameter | Values | Behavior |
|-----------|--------|----------|
| `claude_bin` (API) / `claude_bin` (MCP) | `"claude"`, `"claudio"`, or omit | Per-task override. Falls back to auto-detection if omitted. |

Auto-detection order:
1. `CLAUDE_BIN` env var
2. `shutil.which("claude")` → resolves full path at startup
3. `"claude"` as final fallback

The selected binary is stored in `task_flows.claude_bin` and used for **all agents** in that flow (research, planner, executor, validators).

## REST API

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/submit` | POST | Submit new task flow (accepts `claude_bin`, `priority`, `metadata`) |
| `/api/flows` | GET | List flows (filter by status) |
| `/api/flows/{id}` | GET | Full flow state + steps + events + artifacts |
| `/api/flows/{id}/react-flow` | GET | React Flow impact graph JSON |
| `/api/flows/{id}/steps/{step_id}` | GET | Step detail with artifact data |
| `/api/flows/{id}` | DELETE | Cancel flow |
| `/api/retry/{id}` | POST | Retry failed flow |
| `/api/stats` | GET | Summary stats |
| `/api/events` | GET | Event stream |
| `/api/skills` | GET/POST | CRUD custom skills |
| `/api/skills/{id}` | GET/DELETE | Get/delete skill |
| `/api/subagents` | GET/POST | CRUD subagents |
| `/api/subagents/{id}` | GET/DELETE | Subagent with linked skills |
| `/api/subagents/{id}/skills` | POST/DELETE | Link/unlink skills |
| `/api/webhooks` | GET/POST | CRUD outbound webhooks |
| `/api/webhooks/{id}` | DELETE | Delete webhook |
| `/api/inbox/mailgun` | POST | Mailgun inbound webhook |
| `/ws` | WebSocket | Real-time event stream |

## Multi-Phase Execution

The planner decomposes tasks into 1-8 phases. Each phase runs as a **separate executor subprocess** — a fresh `claude -p` with no memory of previous phases. Files modified in earlier phases persist on disk.

Key design decisions:
- **Fresh spawn per phase**: no context pollution, no runaway token costs
- **Cold start friendly**: each phase description includes enough context to work independently
- **Sequential ordering**: phases run one after another, the SDK orchestrates the loop
- **Per-phase retry**: if a phase fails, it retries (max 3) before failing the flow

## Skill Assignment (Planner)

| Skill | File types |
|-------|-----------|
| `frontend-designer` | `.tsx` return JSX + Tailwind |
| `frontend-react` | `.tsx` body, hooks, state |
| `frontend-hooks` | `hooks/use*.ts` |
| `frontend-types` | `types.ts` |
| `frontend-constants` | `constants.ts` |
| `frontend-atoms` | `atoms/*.tsx`, `ui/` components |
| `frontend-translator` | `locales/*.json` |
| `frontend-endpoints` | `api/endpoints.ts` |
| `frontend-websockets` | `signalr/*.ts` |
| `frontend-ux` | a11y, keyboard, loading states |
| `backend-dev` | `**/*.cs` |

## Error Handling

The pipeline handles errors at multiple levels:

- **API errors** (429 rate limit, 500 server error): parsed from claude's JSON output and stored as clear error messages in `task_flows.error_reason`
- **Agent crashes**: `_run_agent` wraps in try/except, logs stack traces to journald, marks step as failed
- **Phase failures**: retry up to `PIPELINE_MAX_RETRIES` (default 3) before failing the flow
- **SDK crashes**: systemd `Restart=on-failure` auto-restarts the service within 10 seconds
- **Orphan cleanup**: on startup, `sweep_orphans()` resets any flows left in transient states

## Configuration

All tuneable via environment variables:

| Variable | Default | Purpose |
|----------|---------|---------|
| `PIPELINE_PORT` | `8199` | SDK HTTP port |
| `PIPELINE_MAX_RETRIES` | `3` | Max retry attempts per phase |
| `PIPELINE_SDK_URL` | `http://localhost:8199` | SDK base URL |
| `PIPELINE_INBOX_POLL_INTERVAL` | `60` | IMAP poll interval (seconds) |
| `PIPELINE_IMAP_HOST` | — | IMAP host for inbox listener |
| `PIPELINE_IMAP_USER` | — | IMAP username |
| `PIPELINE_IMAP_PASS` | — | IMAP password |
| `CLAUDE_BIN` | auto-detect | Override claude binary path globally |
| `PIPELINE_LOG_LEVEL` | `info` | Log level |

## License

MIT
