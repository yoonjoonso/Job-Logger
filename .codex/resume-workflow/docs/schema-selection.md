# Selection Output Contract

This document defines the deterministic contract for selection output written to:

`.codex/resume-workflow/data/builds/{job}/selection.yml`

The purpose of `selection.yml` is to record exactly which verified source facts were selected for a given job-targeted resume build, why they were selected, and which downstream summary claims and resume bullets are allowed to be generated from them.

## Goals

- Make selection reproducible from the same inputs.
- Preserve end-to-end traceability from every generated statement back to concrete fact IDs.
- Prevent fabricated experience, unsupported scope inflation, or summary claims that exceed the evidence in the fact set.
- Give downstream generation a strict allowlist instead of loose narrative guidance.

## Required Inputs

The selector must not produce `selection.yml` unless all of the following inputs are available for the target `{job}` build.

### 1. Build Context

- `job`: stable job build identifier used in `data/builds/{job}/`.
- `target_role`: normalized role title for the target application.
- `target_company`: company name, if known.
- `target_posting`: normalized job posting text or parsed requirement set.
- `build_timestamp`: ISO-8601 timestamp for when the selection run started.
- `selector_version`: deterministic selector version or ruleset version.

### 2. Verified Fact Inventory

A canonical fact inventory must exist before selection. Each selectable fact must have:

- `fact_id`: globally unique stable identifier.
- `category`: one of `experience`, `achievement`, `project`, `skill`, `domain`, `education`, `credential`, or another controlled category defined upstream.
- `statement`: normalized factual statement.
- `evidence`: source-backed proof reference or provenance metadata.
- `confidence`: upstream verification confidence, if used by the pipeline.
- `source_scope`: where the fact came from, such as work history, project archive, certification records, or skills inventory.
- `dates`: normalized date or date range if applicable.
- `tags`: normalized keywords used for requirement matching.

The selector may only reference facts that already exist in the verified inventory. It must never create new `fact_id` values during selection.

### 3. Requirement Signals

The selector must receive a normalized requirement set derived from the job posting. Each requirement signal should include:

- `requirement_id`: stable identifier within the build.
- `kind`: such as `must_have`, `preferred`, `domain`, `tool`, `leadership`, `scope`, or `outcome`.
- `text`: normalized requirement text.
- `priority`: deterministic ranking weight.
- `keywords`: normalized match tokens.

### 4. Selection Policy

The selector must use an explicit policy configuration, including:

- maximum number of summary claims allowed
- maximum number of resume bullets allowed
- section-specific capacity limits
- scoring weights
- tie-break rules
- exclusion rules
- safety checks

If policy inputs are missing, selection must fail rather than improvise defaults silently.

## Output File

`selection.yml` is the single source of truth for which facts may be used by downstream summary and bullet generation for the specified `{job}` build.

The file must be valid YAML and deterministic in structure, field names, ordering policy, and identifier usage.

## Required Top-Level Shape

```yaml
job: acme-senior-product-manager
selector_version: "1.0"
build_timestamp: "2026-04-09T20:15:00-07:00"
inputs:
  target_role: Senior Product Manager
  target_company: Acme
  target_posting_id: acme-spm-2026-04
  fact_inventory_version: "2026-04-09"
  requirement_profile_version: "2026-04-09"
policy:
  max_summary_claims: 3
  max_bullets: 8
  tie_break_order:
    - higher_requirement_priority
    - higher_evidence_strength
    - higher_recency
    - broader_reusable_scope
    - lower_fact_id
selection:
  selected_fact_ids:
    - fact.exp.pm.014
    - fact.ach.pm.022
  rejected_fact_ids:
    - fact.exp.old.003
  excluded_fact_ids:
    - fact.skill.unverified.002
coverage:
  requirements:
    - requirement_id: req-001
      matched_fact_ids:
        - fact.exp.pm.014
      coverage_strength: direct
summary_claims:
  - claim_id: sum-001
    text: Led roadmap delivery for a cross-functional B2B platform initiative.
    fact_ids:
      - fact.exp.pm.014
      - fact.ach.pm.022
bullets:
  - bullet_id: bul-001
    section: experience
    text: Drove roadmap execution for a B2B platform launch, aligning engineering and GTM stakeholders.
    fact_ids:
      - fact.exp.pm.014
      - fact.ach.pm.022
checks:
  passed: true
  violations: []
```

## Field Semantics

### `job`

- Must equal the `{job}` directory name.
- Must be stable across reruns for the same target build.

### `selector_version`

- Required string identifying the deterministic selection logic or ruleset version.
- Any selector logic change that can affect selection outcomes must bump this value.

### `build_timestamp`

- Required ISO-8601 timestamp.
- Records execution time only; it must not affect ranking or tie-break behavior.

### `inputs`

Required provenance block for reproducibility.

- `target_role`: normalized target role.
- `target_company`: normalized company name if available; otherwise `null`.
- `target_posting_id`: stable posting identifier if available; otherwise `null`.
- `fact_inventory_version`: version, hash, or date identifier for the fact inventory used.
- `requirement_profile_version`: version, hash, or date identifier for requirement normalization used.

Additional provenance fields are allowed if they are deterministic and do not replace required fields.

### `policy`

Required block capturing the policy values actually used during selection. This is not commentary. It is the executed policy snapshot for the run.

### `selection`

Required allowlist block.

- `selected_fact_ids`: all facts approved for downstream use.
- `rejected_fact_ids`: considered but not chosen because they ranked below selected alternatives or did not improve coverage.
- `excluded_fact_ids`: facts disallowed before ranking because they violated exclusion or safety rules.

All three lists must contain only existing `fact_id` values from the verified fact inventory.

### `coverage`

Required mapping from job requirements to selected evidence.

For each relevant requirement:

- `requirement_id` must match an input requirement.
- `matched_fact_ids` must be a non-empty subset of `selection.selected_fact_ids`.
- `coverage_strength` must use a controlled label such as `direct`, `partial`, `adjacent`, or `none`.

If a high-priority requirement has no selected evidence, it must still appear with `coverage_strength: none` unless the policy explicitly scopes it out.

### `summary_claims`

Required ordered list of claims permitted for top-summary generation.

Each entry must include:

- `claim_id`: stable per-file identifier such as `sum-001`.
- `text`: a concise claim statement.
- `fact_ids`: one or more supporting fact IDs.

### `bullets`

Required ordered list of allowed resume bullets.

Each entry must include:

- `bullet_id`: stable per-file identifier such as `bul-001`.
- `section`: target resume section such as `experience`, `projects`, `skills`, or `education`.
- `text`: bullet text.
- `fact_ids`: one or more supporting fact IDs.

### `checks`

Required result of safety validation.

- `passed`: boolean.
- `violations`: list of machine-readable or human-readable validation failures.

If `checks.passed` is `false`, downstream generators must treat the file as invalid for production output.

## Deterministic Storage of Selected Fact IDs

Selected fact IDs are the core contract. They must be stored in three places only:

- `selection.selected_fact_ids`: global allowlist of all facts permitted for downstream generation.
- `summary_claims[*].fact_ids`: fact IDs supporting each allowed summary claim.
- `bullets[*].fact_ids`: fact IDs supporting each allowed bullet.

Rules:

- Every fact ID used in `summary_claims` or `bullets` must also appear in `selection.selected_fact_ids`.
- No downstream text may rely on facts outside those explicit lists.
- Fact IDs must be stored as arrays, never as comma-separated strings.
- Fact IDs within each array must be ordered deterministically.
- Recommended ordering is the same order as the canonical fact inventory or ascending lexical ID order. One rule must be chosen and applied consistently.

## Traceability Rules for Summary Claims

Summary claims are high-risk because they compress multiple facts into broader positioning language. The selector must enforce strict mapping rules.

- Every summary claim must cite at least one fact ID.
- Every meaningful clause in a summary claim must be supportable by the cited facts.
- If a claim combines multiple dimensions, such as role scope plus business outcome, the cited fact set must support both dimensions.
- A summary claim must not introduce new duration, seniority, industry depth, leadership breadth, team size, revenue impact, or ownership scope unless those attributes exist in the mapped facts.
- If one part of a claim lacks support, the entire claim must be rewritten narrower or excluded.

Examples:

- Allowed: "Led cross-functional roadmap execution for a B2B platform launch." Only if mapped facts support leadership, cross-functional collaboration, roadmap work, and platform launch context.
- Not allowed: "Senior product leader with deep fintech expertise." if mapped facts show only one product role and no verified fintech domain evidence.

## Traceability Rules for Bullets

Bullets must also map directly to selected fact IDs.

- Every bullet must cite at least one fact ID.
- A bullet may synthesize wording for clarity, but it must not add unstated metrics, tools, ownership, scale, or outcomes.
- If a bullet includes a metric, percentage, dollar figure, team size, or timeframe, that detail must appear in at least one mapped fact.
- If a bullet merges two facts, both fact IDs must be listed.
- If two mapped facts conflict, the bullet must use the narrower compatible wording or be rejected.

## Exclusion Rules

Facts must be placed in `selection.excluded_fact_ids` and blocked from downstream use if any of the following apply:

- The fact is unverified or lacks required provenance.
- The fact duplicates another fact with weaker evidence and adds no distinct coverage value.
- The fact is stale relative to policy recency thresholds and loses against stronger recent evidence for the same requirement.
- The fact is only adjacent to the requirement and would overstate direct experience if used narratively.
- The fact depends on unsupported inference, such as assuming ownership, seniority, strategy responsibility, or business impact beyond the source statement.
- The fact would reveal confidential, restricted, or disallowed information under resume policy.
- The fact conflicts with a better-supported fact and the conflict cannot be resolved safely.

Excluded facts are not eligible for later resurrection by generation steps.

## Tie-Break Rules

When multiple candidate facts satisfy the same requirement with equivalent baseline relevance, the selector must break ties using a fixed ordered rule set. The recommended order is:

1. Higher requirement priority coverage.
2. Stronger evidence quality or verification strength.
3. More direct match over adjacent match.
4. More recent relevant experience.
5. Broader reusability across multiple requirements without exaggeration.
6. Higher signal density, meaning the fact supports more resume value per bullet slot.
7. Stable lexical order on `fact_id` as the final deterministic fallback.

Rules:

- Tie-break order must be documented in `policy.tie_break_order`.
- The selector must not use randomness, non-stable collection order, or LLM preference as a final chooser.
- If two facts remain indistinguishable after all criteria, lexical `fact_id` order decides.

## Safety Checks

The selector must validate the output before writing `checks.passed: true`.

### Anti-Fabrication Checks

- No summary claim or bullet may contain a concrete detail absent from its mapped fact IDs.
- No claim may upgrade participation into ownership, leadership, strategy, or end-to-end accountability unless explicitly supported.
- No claim may convert exposure into expertise, or collaboration into management.
- No claim may upgrade "supported", "contributed", or "partnered" into "led", "owned", or "drove" unless verified.
- No claim may expand one project or one role into broad repeated experience without evidence across multiple facts.

### Scope Control Checks

- Seniority labels must match verified role evidence.
- Domain labels must match verified domain facts.
- Quantified impact must match exact or safely rounded supported values according to policy.
- Tool or technology claims must reflect actual use, not merely adjacent team exposure.
- Time-in-role or years-of-experience claims must only be produced from explicit date-backed facts or approved duration calculations.

### Consistency Checks

- Every `summary_claims[*].fact_ids` value must be a subset of `selection.selected_fact_ids`.
- Every `bullets[*].fact_ids` value must be a subset of `selection.selected_fact_ids`.
- Every selected fact must serve at least one purpose: requirement coverage, summary support, or bullet support.
- No rejected or excluded fact ID may appear in any claim or bullet mapping.
- No empty `fact_ids` arrays are allowed.
- No duplicate `claim_id` or `bullet_id` values are allowed.

### Failure Behavior

If any safety check fails:

- `checks.passed` must be `false`.
- Every violation must be listed in `checks.violations`.
- The file may still be written for debugging, but downstream resume generation must refuse to treat it as approved production input.

## Authoring Constraints for Downstream Generators

Downstream summary and bullet generators must treat `selection.yml` as a hard contract.

- They may paraphrase claim or bullet text only within the semantic bounds of mapped fact IDs.
- They may drop approved items for space.
- They may not add new facts, new metrics, new scope claims, or new inferred domain depth.
- They may not cite a fact that is not listed in the item's `fact_ids`.
- They may not compose a new summary claim from selected facts unless the pipeline explicitly allows claim regeneration under the same safety checks.

## Minimal Validation Checklist

Before accepting a `selection.yml` file, validate:

1. Required top-level keys exist.
2. All referenced fact IDs exist in the verified fact inventory.
3. All mapped fact IDs are subsets of `selection.selected_fact_ids`.
4. No excluded or rejected fact IDs appear in claims or bullets.
5. Coverage entries reference valid requirement IDs.
6. Safety checks pass, or the file is explicitly marked invalid.
7. Ordering rules are deterministic and reproducible.

## Non-Negotiable Principle

`selection.yml` is an evidence-constrained selection record, not a creative brief. If a claim cannot be traced cleanly to verified `fact_id` values, it must not appear in the output.
