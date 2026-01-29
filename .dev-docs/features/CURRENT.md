# Feature: Add core orchestration fields + section markdown + finalize/exit payloads to `basic.json`

## Problem

`basic.json` doesn’t include the core orchestration parameters our Fabric architecture requires, and it also lacks the standardized section-markdown layout and “finalize + exit payload” pattern used in `python_file_to_delta_template.json`. This leads to inconsistent notebooks and extra developer work.

## Goals

* Add required orchestration inputs:

  * `input_stage` (dev/test/prod)
  * `run_interval_utc` (orchestrator run timestamp)
  * `input_data_interval_utc` (data timestamp used to derive the daily slice)
* Mirror the **section markdown structure** from `python_file_to_delta_template.json` so generated notebooks have consistent headers and step layout.
* Add **Step 5: Finalize and exit payloads** matching the reference template’s approach.
* Keep lightweight helper functions for easy connections/utilities.
* Ensure output remains a reusable template.

## Non-goals

* Rebuilding the templating system or introducing a large framework.
* Adding complex orchestration logic (dependencies, retry policies, alerts).
* Hardcoding any workspace/lakehouse/project-specific values.

## Requirements

* **Files**

  * Update:
    `...\tools\notebook_templates\basic.json`
  * Reference:
    `...\tools\notebook_templates\python_file_to_delta_template.json`

* **Must-have orchestration fields**

  * `input_stage`: environment selector (`dev` / `test` / `prod`)
  * `run_interval_utc`: orchestrator run timestamp
  * `input_data_interval_utc`: timestamp used to derive the daily slice

* **Section markdown (match reference template)**

  * `basic.json` must include markdown cells that clearly delineate sections/steps like the reference template.
  * Include a consistent step sequence (at minimum) as markdown headings, e.g.:

    1. Overview / Purpose
    2. Parameters / Orchestration Inputs
    3. Helper Functions / Utilities
    4. Main Logic (placeholder)
    5. **Finalize and exit payloads**
  * Headings should follow the same style used in the reference (same heading levels, naming conventions, and separator formatting).

* **5) Finalize and exit payloads**

  * Include a dedicated section/cell that creates a standardized payload object containing:

    * status (success/failure)
    * `input_stage`, `run_interval_utc`, `input_data_interval_utc`
    * basic run metadata (start/end time, duration; row counts if applicable)
    * failure info (exception message + stack/trace or equivalent)
  * Exit behavior must follow the same pattern as `python_file_to_delta_template.json` so the orchestrator can reliably parse outputs.

* **Helper functions**

  * Keep a small set of simple helpers (connection setup, config read, logging helpers).
  * No hard dependencies on project-specific paths/IDs.

* **Template purity**

  * No hardcoded environment values or workspace-specific identifiers.
  * Use placeholders where needed and label them clearly.

## Acceptance Criteria

* A notebook generated from `basic.json` has the same **section markdown layout** as the reference template (consistent step headings and flow).
* The three orchestration parameters are present and used in the template.
* Step 5 exists and returns an orchestrator-friendly payload on both success and failure, using the reference template’s exit mechanism.
* Helpers remain minimal and useful.
* The result is clearly a reusable template (no hardcoded project specifics).

## Notes / Links

* Source directory: `...\tools\notebook_templates`
* Reference pattern: `python_file_to_delta_template.json`

