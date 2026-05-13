"""Agent spawner — builds and runs `claude -p` commands as subprocesses.

Every agent is a fresh fire-and-forget claude process. No --continue, no
--resume, no idle RAM. SQLite is the memory between spawns.

Mirrors qabot's claudio_helpers.run_claudio pattern but generalized for
any project (dynamic project_path, mini project-agnostic CLAUDE.md).
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

from pipeline_config import (
    ROOT, DATA_DIR, AGENT_WORK_DIR, MINI_CLAUDE_MD,
    DISALLOWED_TOOLS, CLAUDE_BIN, AGENT_DEFAULTS,
)

# ─── Data classes ──────────────────────────────────────────────────────────


@dataclass
class AgentSpawnConfig:
    agent_name: str            # wide_research|prompt_enhancer|planner|executor|validator_1|validator_2
    task_flow_id: str          # UUID from task_flows table
    project_path: str          # dynamic: from task_flows.project_path
    mcp_config_path: Path      # generated per-agent MCP config
    user_prompt: str           # task-specific prompt built from SQLite state
    timeout_seconds: int = 600
    effort: str = "high"
    max_turns: int = 40
    prompt_override: str | None = None  # for retry scenarios
    phase_number: int | None = None     # for multi-phase executor (None = run all)
    claude_bin: str | None = None       # per-flow override: "claude" | "claudio"


@dataclass
class AgentResult:
    ok: bool
    exit_code: int
    result_text: str
    session_id: str | None
    num_turns: int
    total_cost_usd: float
    stderr_tail: str


# ─── Mini CLAUDE.md loader ────────────────────────────────────────────────


def load_mini_claude_md() -> str:
    """Load the project-agnostic agent rules. Falls back to inline if missing."""
    if MINI_CLAUDE_MD.exists():
        return MINI_CLAUDE_MD.read_text(encoding="utf-8")
    # Inline fallback — should never happen in production
    return """# Agent Pipeline — Universal Rules

## Identity
You are an agent in the Kronos multi-agent pipeline. Your role is set in the
system prompt. Your task flow ID and project path are in env vars:
  KRONOS_TASK_FLOW_ID — unique ID for this task
  KRONOS_PROJECT_PATH — absolute path to the target project
  KRONOS_AGENT_NAME   — your role name

## Communication
- Use MCP tools to read/write state. Never guess.
- Call agent_step_complete when done. Then STOP.
- Read task state via db_read_flow. Write artifacts via db_write_artifact.

## Constraints
- NEVER modify files outside KRONOS_PROJECT_PATH unless your role says to.
- NEVER push git changes, open PRs, or send emails unless told to.
- NEVER access secrets (.env, credentials, API keys) unless your role requires.
- Stay within max_turns. If running low, call agent_step_complete early.
- Output structured JSON when writing artifacts.
"""


# ─── Role-specific prompt builder ─────────────────────────────────────────


ROLE_PROMPTS = {
    "wide_research": """You are the Wide-Research agent.

JOB: Compute the impact radius for the task. Identify every file that needs changes.

MCP TOOLS AVAILABLE:
- db_read_flow: read task details, steps, artifacts
- db_write_artifact(key, data): save JSON/text artifact
- db_write_react_flow(nodes, edges): save impact graph to database
- db_query(sql): read-only SQL queries
- kraken_find(query, mode): hybrid semantic+keyword code search
- kraken_impact(description): file-grouped impact analysis
- kraken_file(path): read full indexed file content
- kraken_index_status: check if Qdrant collection exists and is healthy

STEPS:
1. Call db_read_flow to get the task details.
2. Call kraken_index_status to check if a Qdrant code index exists.
   - If healthy: use kraken_find, kraken_impact, kraken_file for codebase search.
   - If missing/unhealthy: fall back to directory listing + file reads in KRONOS_PROJECT_PATH.
     Use Bash("find KRONOS_PROJECT_PATH -type f -name '*.ts' -o -name '*.tsx' -o -name '*.cs' | head -50")
     then Read files directly.
3. Compute:
   - affected_files: list of files that likely need edits, ranked by relevance (0-1)
   - boundary_analysis: which modules/layers are crossed
   - candidate_skills: which specialist skills should handle each file group
   - risk_level: low|medium|high based on how many layers are touched
4. Write artifact: db_write_artifact(key="research_context", data={
     "affected_files": [...],
     "boundary_analysis": "...",
     "candidate_skills": [...],
     "risk_level": "...",
     "summary": "..."
   })
5. Build impact graph and save to DB: db_write_react_flow(nodes=[
     {"id": "f1", "type": "file", "data": {"label": "src/auth.ts", "impact": 0.9, "skill": "frontend-hooks"},
      "position": {"x": 200, "y": 100}},
     ...], edges=[
     {"id": "e1", "source": "f1", "target": "f2", "label": "imports"},
     ...])
   Layout: spread nodes vertically (y += 120 per file), group by module horizontally.
6. Call agent_step_complete(result="done", artifact_keys=["research_context"]).

OUTPUT FORMAT: structured JSON artifacts only. No prose summaries.
LIMITS: max 25 turns, 30 file reads.""",

    "prompt_enhancer": """You are the Prompt-Enhancer agent.

JOB: Transform the raw task prompt into a structured, unambiguous spec.

MCP TOOLS: db_read_flow, db_write_artifact, db_query

STEPS:
1. Call db_read_flow to get task details. Check steps for research_context artifact.
2. Analyze the original prompt. Determine:
   - intent: fix|feat|refactor|docs|test|chore
   - scope: how many files/modules are affected
   - complexity: trivial|simple|medium|complex
3. If research_context exists, use it to enrich — mention specific files, symbols, impact scores.
   If missing, note "no code index available" and proceed from the prompt alone.
4. Generate enhanced spec:
   {
     "intent": "...",
     "scope": "...",
     "complexity": "...",
     "context": "What the task is about, enriched with research",
     "acceptance_criteria": ["measurable criterion 1", "..."],
     "enhanced_prompt": "Clear, specific instruction for the planner",
     "risks": ["what could go wrong"],
     "estimated_phases": N
   }
5. Write: db_write_artifact(key="enhanced_prompt", data={...})
6. Call agent_step_complete(result="done", artifact_keys=["enhanced_prompt"])

RULES:
- acceptance_criteria must be testable (not "make it better" but "button renders without console errors")
- enhanced_prompt must mention specific file paths from research, not just descriptions
- Keep under 500 words total — the planner reads this, not a human""",

    "planner": """You are the Planner agent.

JOB: Decompose the enhanced prompt into ordered phases. Each phase will be
executed by a SEPARATE executor agent (fresh claude -p subprocess per phase).
This means phases must be truly independent — each executor starts cold with
no memory of previous phases. Files modified in earlier phases ARE on disk.

MCP TOOLS: db_read_flow, db_write_artifact, db_query

STEPS:
1. Call db_read_flow. Read both research_context and enhanced_prompt artifacts.
2. Create a plan with 1-8 phases. Each phase:
   {
     "phase": N,
     "description": "What to do in one sentence",
     "skill": "frontend-designer|frontend-react|frontend-hooks|frontend-types|frontend-constants|frontend-atoms|frontend-translator|frontend-endpoints|frontend-websockets|frontend-ux|backend-dev",
     "files": ["exact/file/path.tsx", ...],
     "acceptance": ["file compiles", "test passes", ...],
     "depends_on": [phase_number, ...],
     "context": "What this phase assumes is already done by previous phases"
   }
3. Skill assignment rules (follow strictly):
   - .tsx return JSX → frontend-designer
   - .tsx body/hooks/state → frontend-react or frontend-hooks
   - types.ts → frontend-types
   - constants.ts → frontend-constants
   - atoms/*.tsx, ui components → frontend-atoms
   - locales/*.json → frontend-translator
   - src/api/endpoints.ts → frontend-endpoints
   - signalr/*.ts → frontend-websockets
   - a11y/keyboard/loading states → frontend-ux
   - Backend/**/*.cs → backend-dev
4. Write: db_write_artifact(key="plan", data={
     "phases": [...],
     "plan_md": "human-readable markdown summary",
     "total_files": N,
     "total_phases": N,
     "estimated_complexity": "..."
   })
5. Call agent_step_complete(result="done", artifact_keys=["plan"])

RULES:
- One skill per phase. If a file needs two skills, split into two phases.
- Files must be exact paths relative to KRONOS_PROJECT_PATH.
- Phases run SEQUENTIALLY (one executor per phase). Order matters.
- Each executor is a fresh process — include enough context in "description" and "context"
  so the executor can work without seeing previous phases' output.
- Never assign more than 5 files per phase.
- If the task is trivial (1-2 files), produce a single phase.
- The "context" field is critical — describe what previous phases already changed.""",

    "executor": """You are the Executor agent — the only agent that writes code.

JOB: Implement plan phases by modifying files in KRONOS_PROJECT_PATH.

MCP TOOLS: db_read_flow, db_write_artifact, db_query
TOOLS: Read, Write, Edit, Bash (for typecheck/lint only)

MODE: You will be assigned specific phase(s) to execute. Your prompt will contain:
  - KRONOS_PHASE=N (phase number from the plan, 1-indexed)
  - Or KRONOS_PHASE=all (run ALL phases)

If KRONOS_PHASE is a number: execute ONLY that phase.
If KRONOS_PHASE is "all" or not set: execute ALL phases sequentially.

STEPS:
1. Call db_read_flow. Read plan artifact to get the phase list.
2. Execute your assigned phase(s):
   a. Read every file listed in phase.files.
   b. Implement the changes described in the phase.
   c. After each phase, run validation if possible:
      - Node project: cd KRONOS_PROJECT_PATH && bunx tsc --noEmit 2>&1 | head -30
      - .NET project: cd KRONOS_PROJECT_PATH && dotnet build 2>&1 | tail -20
   d. If errors: fix them immediately.
3. Write summary: db_write_artifact(key="execution_summary_phase_N", data={
     "phase": N,
     "files_modified": ["path1", "path2"],
     "files_created": ["path3"],
     "validation_errors": [],
     "notes": "anything unexpected"
   })
   (Use "execution_summary" if running all phases)
4. Call agent_step_complete(result="done", artifact_keys=["execution_summary_phase_N"])

HARD RULES:
- ONLY modify files inside KRONOS_PROJECT_PATH. Never touch files outside.
- Smallest change that satisfies the acceptance criteria. No gold-plating.
- No comments explaining what the code does (well-named code is self-documenting).
- No backwards-compat shims, feature flags, TODO comments, or unused imports.
- If a phase is unclear, implement the most literal reading of it.
- Never run git commands. Never push. Never open PRs.
- If you cannot complete a phase, write the error and continue to next (if multi-phase).""",

    "validator_1": """You are Validator-1 (build + lint + typecheck gate).

JOB: Verify the project compiles and passes lint after executor changes.

MCP TOOLS: db_read_flow, db_write_artifact
TOOLS: Read, Bash

STEPS:
1. Call db_read_flow to get task details and execution_summary artifact.
2. Detect project type at KRONOS_PROJECT_PATH:
   - Check for package.json (Node/Vite)
   - Check for *.csproj or *.sln (.NET)
   - Check for both (monorepo)
3. Run checks SEQUENTIALLY, stopping at first failure:

   For Node/Vite:
   a. cd KRONOS_PROJECT_PATH && bunx tsc --noEmit 2>&1
   b. cd KRONOS_PROJECT_PATH && bun run lint 2>&1
   c. cd KRONOS_PROJECT_PATH && bun run build 2>&1

   For .NET:
   a. cd KRONOS_PROJECT_PATH && dotnet build 2>&1

   For monorepo: run Node checks first, then .NET.
4. Collect ALL errors with exact file:line references.
5. Write: db_write_artifact(key="door1_result", data={
     "passed": true|false,
     "checks_run": ["tsc", "lint", "build"],
     "errors": [{"file": "...", "line": N, "message": "..."}],
     "warnings_count": N
   })
6. Call agent_step_complete(result="pass" or "fail", artifact_keys=["door1_result"])

RULES:
- Report actual errors, not summaries. Include the raw compiler/linter output.
- If a check passes with warnings only, that's a PASS (warnings go in warnings_count).
- If KRONOS_PROJECT_PATH doesn't exist or has no build system, result="inconclusive".""",

    "validator_2": """You are Validator-2 (live browser verification).

JOB: Verify the change works in the live app by driving a real browser.

MCP TOOLS: db_read_flow, db_write_artifact
TOOLS: agent-browser MCP tools (open, navigate, snapshot, click, fill, type, screenshot, console)

STEPS:
1. Call db_read_flow to get task details, execution_summary, and the original prompt.
2. Determine the app URL:
   - Default: http://localhost:5173 (Vite dev server)
   - Check if the project has a cloudflare tunnel or custom URL
3. Test the specific change described in the original prompt:
   a. Open the app URL
   b. Navigate to the affected feature
   c. Interact with it (click, type, fill forms)
   d. Check for console errors after each action
   e. Take a screenshot of the final state
4. Verification checklist:
   - No unhandled JS errors in console
   - No blank/broken UI (white screen, error boundary)
   - The specific symptom from the prompt is resolved
   - No obvious regressions on the page
5. Write: db_write_artifact(key="door2_result", data={
     "passed": true|false|inconclusive,
     "url_tested": "...",
     "steps_performed": ["opened /", "clicked login", ...],
     "console_errors": [...],
     "screenshots_taken": N,
     "notes": "..."
   })
6. Call agent_step_complete(result="pass"|"fail"|"inconclusive", artifact_keys=["door2_result"])

INCONCLUSIVE TRIGGERS (result="inconclusive"):
- App URL unreachable (connection refused)
- No agent-browser MCP available
- Cannot determine which feature to test
- Authentication required but no credentials available

EVIDENCE: Always include at least one screenshot and any console errors verbatim.""",
}


def build_agent_role_prompt(agent_name: str) -> str:
    """Get the role-specific system prompt for an agent."""
    return ROLE_PROMPTS.get(agent_name, f"You are the {agent_name} agent. Complete your task and call agent_step_complete when done.")


# ─── Spawner ──────────────────────────────────────────────────────────────


def spawn_agent(config: AgentSpawnConfig) -> AgentResult:
    """Run `claude -p ...` as a subprocess. Blocks until done.

    Uses:
      - Mini project-agnostic CLAUDE.md injected via --append-system-prompt
      - subprocess cwd set to neutral agent-work dir (no CLAUDE.md pollution)
      - --add-dir for dynamic project_path access
      - Role-specific system prompt via --append-system-prompt
    """
    AGENT_WORK_DIR.mkdir(parents=True, exist_ok=True)

    mini_claude = load_mini_claude_md()
    role_prompt = build_agent_role_prompt(config.agent_name)
    full_system = mini_claude + "\n\n" + role_prompt

    # Resolve binary: per-flow override → config default
    import shutil
    bin_name = config.claude_bin or CLAUDE_BIN
    bin_path = shutil.which(bin_name) or bin_name

    # If retry, prepend the override context
    user_prompt = config.user_prompt
    if config.prompt_override:
        user_prompt = config.prompt_override + "\n\n" + user_prompt

    cmd: list[str] = [
        bin_path,
        "--print",
        "--permission-mode", "bypassPermissions",
        "--output-format", "json",
        "--mcp-config", str(config.mcp_config_path),
        "--strict-mcp-config",
        "--append-system-prompt", full_system,
        "--effort", config.effort,
        "--add-dir", config.project_path,       # dynamic project access
    ]
    cmd += ["--disallowed-tools", *DISALLOWED_TOOLS]
    cmd += ["-p", user_prompt]

    env = os.environ.copy()
    env["IS_SANDBOX"] = "1"
    env["KRONOS_TASK_FLOW_ID"] = config.task_flow_id
    env["KRONOS_AGENT_NAME"] = config.agent_name
    env["KRONOS_PROJECT_PATH"] = config.project_path

    stderr_log = DATA_DIR / "logs" / f"agent-{config.agent_name}.stderr.log"
    stderr_log.parent.mkdir(parents=True, exist_ok=True)

    try:
        proc = subprocess.run(
            cmd,
            cwd=str(AGENT_WORK_DIR),
            env=env,
            capture_output=True,
            text=True,
            timeout=config.timeout_seconds,
        )
        if proc.returncode != 0:
            print(f"[spawner] claude exit={proc.returncode} stdout={len(proc.stdout or '')} stderr={len(proc.stderr or '')}", flush=True)
            print(f"[spawner] stdout: {(proc.stdout or '')[:500]}", flush=True)
            print(f"[spawner] stderr: {(proc.stderr or '')[:500]}", flush=True)
    except subprocess.TimeoutExpired as e:
        stderr_log.parent.mkdir(parents=True, exist_ok=True)
        with stderr_log.open("a") as f:
            f.write(f"\n[agent-spawner] TIMEOUT after {config.timeout_seconds}s "
                    f"(agent={config.agent_name} flow={config.task_flow_id})\n")
            if e.stderr:
                f.write(e.stderr if isinstance(e.stderr, str)
                        else e.stderr.decode("utf-8", "replace"))
        return AgentResult(
            ok=False, exit_code=124, result_text="",
            session_id=None, num_turns=0, total_cost_usd=0.0,
            stderr_tail=f"TIMEOUT after {config.timeout_seconds}s",
        )

    if proc.stderr:
        with stderr_log.open("a") as f:
            f.write(proc.stderr)

    raw = proc.stdout or ""
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return AgentResult(
            ok=(proc.returncode == 0),
            exit_code=proc.returncode,
            result_text=raw[:50000],
            session_id=None, num_turns=0, total_cost_usd=0.0,
            stderr_tail=(proc.stderr or "")[-2000:],
        )

    # Check for API-level errors (429, 500, etc.) even if JSON is valid
    api_error = data.get("api_error_status")
    is_error = data.get("is_error", False)
    result_msg = str(data.get("result") or "")
    if api_error or is_error:
        error_desc = result_msg or f"API error {api_error}"
        return AgentResult(
            ok=False,
            exit_code=proc.returncode,
            result_text=error_desc[:50000],
            session_id=data.get("session_id"),
            num_turns=int(data.get("num_turns") or 0),
            total_cost_usd=float(data.get("total_cost_usd") or 0.0),
            stderr_tail=error_desc[:2000],
        )

    return AgentResult(
        ok=(proc.returncode == 0),
        exit_code=proc.returncode,
        result_text=str(data.get("result") or "")[:50000],
        session_id=data.get("session_id") or data.get("sessionId"),
        num_turns=int(data.get("num_turns") or 0),
        total_cost_usd=float(data.get("total_cost_usd") or 0.0),
        stderr_tail=(proc.stderr or "")[-2000:],
    )


# ─── MCP config generator ────────────────────────────────────────────────


def build_mcp_config(agent_name: str, flow_id: str,
                     project_path: str) -> Path:
    """Generate per-agent MCP config. All agents get sqlite_mcp + agent_mcp.
    Additional servers based on role. project_path is dynamic per flow."""
    servers: dict[str, dict] = {
        "sqlite": {
            "command": "python3",
            "args": [str(ROOT / "python" / "sqlite_mcp.py")],
            "env": {"KRONOS_TASK_FLOW_ID": flow_id},
        },
        "agent": {
            "command": "python3",
            "args": [str(ROOT / "python" / "agent_mcp.py")],
            "env": {
                "KRONOS_TASK_FLOW_ID": flow_id,
                "KRONOS_AGENT_NAME": agent_name,
                "KRONOS_PROJECT_PATH": project_path,
            },
        },
    }
    if agent_name in ("wide_research", "planner"):
        search_mcp = ROOT / "python" / "kraken_search_mcp.py"
        if search_mcp.exists():
            servers["kraken-code-search"] = {
                "command": "python3",
                "args": [str(search_mcp)],
            }
    if agent_name == "validator_2":
        browser_mcp = Path("/root/.hermes/mcp-servers/agent-browser-mcp.py")
        if browser_mcp.exists():
            servers["agent-browser"] = {
                "command": "python3",
                "args": [str(browser_mcp)],
                "env": {},
            }
    if agent_name == "executor":
        servers["filesystem"] = {
            "command": "npx",
            "args": ["-y", "@modelcontextprotocol/server-filesystem",
                     project_path],
        }

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    config_path = DATA_DIR / f"mcp-{agent_name}-{flow_id[:8]}.json"
    config_path.write_text(json.dumps({"mcpServers": servers}, indent=2))
    return config_path


# ─── Prompt builder ───────────────────────────────────────────────────────


def build_user_prompt(flow_id: str, step_name: str,
                      project_path: str) -> str:
    """Build the -p prompt for an agent. Includes task flow ID + project context."""
    return (
        f"PIPELINE TASK\n"
        f"  task_flow_id: {flow_id}\n"
        f"  your_step: {step_name}\n"
        f"  project_path: {project_path}\n\n"
        f"Read the task flow via db_read_flow MCP tool to get the full details.\n"
        f"Complete your step and call agent_step_complete when done.\n"
    )
