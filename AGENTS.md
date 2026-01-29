# AGENTS.md — Codex Operating Manual

## Mission

Codex acts as a standalone engineer to deliver high-quality changes: **plan → implement → validate → draft PR → open PR → hand off**.

## Non-Negotiables

* Never commit secrets (tokens, passwords, private keys).
* Prefer verifiable repo state (git status/diff/tests) over memory.
* Keep changes small, reviewable, and tested.
* Do not change generated/lock files unless necessary and explained.

## Work Modes

### Plan Mode (`$plan`)

**Goal:** Turn a request into a testable plan.
**Outputs:**

* Update `.dev-docs/context/TASKS.md` with goal, acceptance criteria, tasks, and test plan.

Plan inputs precedence:
1) `.dev-docs/features/CURRENT.md` if present
2) User prompt (overrides/notes)
Plan must record the source in `.dev-docs/context/TASKS.md`.

### Build Mode (`$build`)

**Goal:** Implement the plan on a feature branch.
**Outputs:**

* Code + tests + docs (as needed)
* Small, logical commits
* Keep `.dev-docs/context/TASKS.md` current

### Review Mode (`$review`)

**Goal:** Reach CI-parity readiness.
**Outputs:**

* Run checks (lint/typecheck/tests/build where applicable)
* Self-review diff using `.dev-docs/review-checklist.md`
* Fix issues and update TASKS with findings

### PR Draft Mode (`$pr-draft`)

**Goal:** Generate a PR-ready title/body from TASKS + repo diff + check status.
**Outputs:**

* Save draft to `.dev-docs/context/PR_DRAFT.md`
* Follow `.github/pull_request_template.md` when present

### PR Open Mode (`$pr-open`)
**Goal:** Push the current feature branch and open a PR.
**Outputs:**
- Pushes branch to `origin`
- Opens PR (preferred via `gh`, fallback via API)
- Prints PR URL

### Handoff Mode (`$handoff`)

**Goal:** Save durable context so a clean chat can resume instantly.
**Outputs:**

* `.dev-docs/context/WORKING.md` (latest pointer)
* `.dev-docs/context/history/<timestamp>--<tag>.md` (immutable snapshot)

### Ship Mode (`$ship`)
**Goal:** Run the full loop end-to-end.
**Workflow:** `$plan` → `$build` → `$review` → `$pr-draft` → `$pr-open` → `$handoff`

## Branching & Git

* Base branch: `main` (or repo default if different).
* Never commit directly to base branch.
* Feature branches: `feat/<slug>`, `fix/<slug>`, `chore/<slug>`.
* Commit style: Conventional Commits preferred (`feat:`, `fix:`, `chore:`, `test:`) unless repo uses another standard.

## Definition of Done

* [ ] Acceptance criteria met
* [ ] Unit tests added/updated
* [ ] Lint/format/typecheck clean (if applicable)
* [ ] All tests pass locally (or explain why not possible)
* [ ] Docs updated (if behavior changed)
* [ ] No debug logs / TODOs left unintentionally
* [ ] Risk + rollout/rollback notes captured

## Commands Contract

Canonical commands live in `.dev-docs/commands.md`. If missing/Unknown, update it before proceeding.

## Task + Context System

* Tasks: `.dev-docs/context/TASKS.md`
* PR draft: `.dev-docs/context/PR_DRAFT.md`
* Session handoff: `.dev-docs/context/WORKING.md`
* Always update TASKS when scope changes.
* Always run handoff before ending a session or switching major subtask.
* Feature spec (optional): `.dev-docs/features/CURRENT.md`

## Reviews

Use `.dev-docs/review-checklist.md` to self-review before PR.

## Observability & Safety

* Validate error handling, logging, and input validation for user-facing changes.
* Never weaken security controls to “make tests pass”.

## Repo-Specific Notes

* Python project; entrypoint is `python -m fde_fabric_mcp.server`.
* Dev tooling includes `pytest` and `ruff` (see `pyproject.toml`).
