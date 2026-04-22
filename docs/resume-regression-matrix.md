# Resume Regression Matrix

This matrix defines representative resume-generation fixtures and expected metadata outcomes for `scripts/generate-resume.py`.

It is intentionally lightweight:

- automated checks compare a stable subset of `resume-metadata.json`
- fixture inputs live in `tests/resume/fixtures.json`
- golden subsets live in `tests/resume/golden/*.json`
- the runner is `scripts/test-resume-regressions.py`

## Scope

Automated coverage in the current matrix:

- selected resume profile
- fit tier (`direct_fit`, `adjacent_fit`, `low_match_general`)
- final layout
- certification visibility
- supplemental education inclusion
- presence of experience keep/drop explainability

Manual or upstream checks:

- archetype ranking
  This is owned by `log_job.py` / JD analysis and should be inspected via top-level `metadata.json`, not only `resume-metadata.json`.
- positive certification ordering
  The current DB fixtures do not yet produce a role with matched cert-specific requirements strong enough to surface ordered certifications. The matrix keeps a manual placeholder for that case.

## Fixtures

| Fixture | Intent | Expected Profile | Expected Layout | Certs | Supplemental Edu |
|---|---|---|---|---|---|
| `online_direct` | Strong direct game-server/backend role using matched DB requirements | `backend` | `grouped_standard` | hidden | excluded |
| `gameplay_direct` | Direct gameplay role with no security/network elevation | `gameplay` | `grouped_standard` | hidden | excluded |
| `cyber_adjacent_ea` | Security-adjacent software role where cyber framing should win, but certs remain hidden | `cyber` | `grouped_standard` | hidden | included |
| `cyber_low_match_general` | Weak cyber/security fit where summary should fall back to general | `general` | `grouped_standard` | hidden | included |
| `cert_order_manual` | Placeholder for future positive cert-order case | manual | manual | visible + ordered | likely included |

## Expected Metadata Outcomes

Each automated fixture is reduced to a stable snapshot with these keys:

- `archetype`
- `selected_profile_key`
- `fit_tier`
- `final_layout`
- `cert_count`
- `cerritos_included`
- `experience_explainability_present`
- `drop_explainability_present`

Those snapshots are checked into:

- `tests/resume/golden/online_direct.json`
- `tests/resume/golden/gameplay_direct.json`
- `tests/resume/golden/cyber_adjacent_ea.json`
- `tests/resume/golden/cyber_low_match_general.json`

## Running

Use the same Python environment that can already run `scripts/generate-resume.py`.

Example:

```bash
python scripts/test-resume-regressions.py --python python
```

To update snapshots after an intentional logic change:

```bash
python scripts/test-resume-regressions.py --python python --update
```

## Dependency Note

The regression runner shells out to `scripts/generate-resume.py`, so it requires the same runtime dependencies as the active resume generator.

At minimum:

- `python-docx`
- `PyYAML`

`docx2pdf` is optional because the generator already tolerates PDF export failure.

## Archetype Ranking Check

Archetype ranking is upstream from this harness.

For that regression:

1. run `log_job.py` on a representative JD or existing role
2. inspect top-level `metadata.json`
3. verify:
   - selected `archetype`
   - keyword basis
   - any exposed archetype reasoning or analysis payload

That check should stay separate from the resume generator harness because `scripts/generate-resume.py` consumes an archetype; it does not rank them.
