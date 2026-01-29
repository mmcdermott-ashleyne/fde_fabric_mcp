# PR Draft

## Title

feat: expand basic template and monitor urls

## Body

# Summary

Adds Codex dev-docs scaffolding, expands the basic notebook template with orchestration sections/exit payloads, and updates Fabric pipeline monitor URLs to include the pipeline id.

# Why

The basic notebook template needs standardized orchestration inputs/sections, and monitor URLs should resolve correctly in the Fabric UI. The repository also needs baseline Codex workflow scaffolding.

# What Changed

* Added AGENTS.md, .dev-docs scaffolding, and PR template.
* Expanded `basic.json` with orchestration parameters, section markdown, and finalize/exit payloads.
* Updated pipeline monitor URL construction to include pipeline id and experience parameter.

# How Tested

* `ruff format .` (failed: ruff not available)
* `ruff check .` (failed: ruff not available)
* `pytest` (failed: pytest not available)

# Risks

* Low. Template/documentation changes and a monitor URL tweak only.

# Rollout / Rollback

* Rollout: merge and deploy as usual.
* Rollback: revert the commits if any issues arise.

## Metadata

* Branch: feat/basic-template-orchestration
* Commit: edafe1ef4a00d63276cdff9b7c387c5c9e2635c8
* Diff Stat:

  *  .dev-docs/commands.md                              |  40 ++++++
  *  .dev-docs/context/PR_DRAFT.md                      |  19 +++
  *  .dev-docs/context/TASKS.md                         |  39 +++++
  *  .dev-docs/context/WORKING.md                       |   5 +
  *  .dev-docs/features/CURRENT.md                      |  84 +++++++++++
  *  .dev-docs/review-checklist.md                      |  33 +++++
  *  .github/pull_request_template.md                   |  31 ++++
  *  AGENTS.md                                          | 115 +++++++++++++++
  *  .../tools/notebook_templates/basic.json            | 160 ++++++++++++++++++++-
  *  src/fde_fabric_mcp/tools/pipelines.py              |  21 ++-
* Working Tree Clean: Yes
* Checks:

  * Lint: Unknown (ruff not available)
  * Typecheck: Unknown
  * Tests: Unknown (pytest not available)
  * Build: Unknown
