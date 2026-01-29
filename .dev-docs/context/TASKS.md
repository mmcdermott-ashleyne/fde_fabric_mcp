# Tasks

## Current Goal

* Update `basic.json` notebook template to include orchestration parameters, section markdown layout, and finalize/exit payloads consistent with `python_file_to_delta_template.json`.

## Source

* `.dev-docs/features/CURRENT.md`

## Acceptance Criteria

* `basic.json` includes the three orchestration parameters: `input_stage`, `run_interval_utc`, `input_data_interval_utc`.
* `basic.json` mirrors the section markdown layout (headings and step sequence) used in `python_file_to_delta_template.json`.
* A Step 5 “Finalize and exit payloads” section exists and returns orchestrator-friendly success/failure payloads using the same exit mechanism as the reference template.
* Helper utilities remain minimal and generic (no project-specific identifiers).
* Template stays reusable (no hardcoded environment/workspace specifics).

## Plan

* [x] Inspect `python_file_to_delta_template.json` for the exact section markdown headings and finalize/exit pattern.
* [x] Expand `basic.json` with matching markdown sections and step ordering.
* [x] Add the orchestration parameters and minimal helper functions to `basic.json`.
* [x] Add Step 5 finalize/exit payloads (success/failure) mirroring the reference pattern.
* [x] Validate JSON structure and template readability.

## Test Plan

* Manual review: confirm markdown sections match the reference template and payload fields are present.
* Sanity check: ensure `basic.json` remains valid JSON.

## Review Findings

* `ruff` not available in PATH (format/lint not run).
* `pytest` not available in PATH (tests not run).

## Risks / Notes

* Ensure no project-specific constants or IDs are introduced.
