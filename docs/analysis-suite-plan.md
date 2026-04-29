# Implementation Plan: Snapshot Analysis Suite

## Overview

This plan implements the offline analysis surface described in
[docs/analysis-suite-design.md](./analysis-suite-design.md).

The goal is to ship five product commands:

- `summarize`
- `diff`
- `audit`
- `ai-pack`
- `graph`

and validate them against locally generated snapshots and the wrapper image flow.

## Architecture Decisions

- Build a reusable `analysis` module instead of embedding logic in CLI handlers.
- Use snapshot metadata and JSONL indexes as the primary read path.
- Load raw YAML lazily for audit, topology, and diff details.
- Render Markdown by default, with JSON and Mermaid where appropriate.
- Support extracted directories plus `.tar.gz` and `.zip` inputs in v1.

## Task List

### Phase 1: Foundations

#### Task 1: Add analysis module scaffolding and snapshot workspace loading

Description:
Build a reusable snapshot input loader that can open extracted directories and compressed snapshot bundles.

Acceptance criteria:
- [ ] A snapshot path can resolve to a local workspace root
- [ ] `.tar.gz` and `.zip` bundles are unpacked transparently
- [ ] Unit tests cover path detection and archive extraction

Verification:
- [ ] `cargo test analysis::source::tests -- --nocapture`

Dependencies:
- None

Files likely touched:
- `src/analysis/mod.rs`
- `src/analysis/source.rs`
- `Cargo.toml`

Estimated scope:
- Medium

#### Task 2: Add snapshot model loading and reusable index parsers

Description:
Load run metadata, index files, and lazy raw-object helpers into a stable `Snapshot` model.

Acceptance criteria:
- [ ] `RunReport` and index types deserialize from snapshot files
- [ ] Snapshot helpers expose resource, relation, and log lookup APIs
- [ ] Unit tests cover JSONL and YAML loading

Verification:
- [ ] `cargo test analysis::snapshot::tests -- --nocapture`

Dependencies:
- Task 1

Files likely touched:
- `src/gather/report.rs`
- `src/gather/agent_artifacts.rs`
- `src/analysis/snapshot.rs`

Estimated scope:
- Medium

### Checkpoint: Foundation

- [x] Snapshot loading works for extracted and compressed inputs
- [x] Tests pass for core model loading

### Phase 2: Domain Queries

#### Task 3: Implement reusable topology and workload queries

Description:
Add query helpers for services, pods, nodes, ingress backends, owner chains, restart hotspots, and missing resource settings.

Acceptance criteria:
- [ ] Service -> pod matching works from selectors and pod labels
- [ ] Pod -> node and owner relationships are queryable
- [ ] Restart and readiness helpers work from pod and node YAML

Verification:
- [ ] `cargo test analysis::queries::tests -- --nocapture`

Dependencies:
- Task 2

Files likely touched:
- `src/analysis/queries.rs`
- `src/analysis/snapshot.rs`

Estimated scope:
- Medium

#### Task 4: Implement audit rule engine

Description:
Add concrete, explainable audit rules for security and upgrade risk findings.

Acceptance criteria:
- [ ] RBAC, workload, exposure, and API-version checks produce findings with evidence
- [ ] Findings have stable severities and rule ids
- [ ] Tests cover representative risky resources

Verification:
- [ ] `cargo test analysis::audit::tests -- --nocapture`

Dependencies:
- Task 2
- Task 3

Files likely touched:
- `src/analysis/audit.rs`
- `src/analysis/queries.rs`

Estimated scope:
- Medium

### Checkpoint: Query Layer

- [x] Topology and audit primitives are testable in isolation
- [x] Audit findings are stable and evidence-backed

### Phase 3: Command Engines

#### Task 5: Implement `summarize`

Description:
Produce Markdown and JSON incident summaries from one snapshot.

Acceptance criteria:
- [ ] Summary includes run status, hotspots, readiness problems, and exposure summary
- [ ] JSON output is machine-readable
- [ ] Tests cover representative output sections

Verification:
- [ ] `cargo test analysis::summary::tests -- --nocapture`

Dependencies:
- Task 3
- Task 4

Files likely touched:
- `src/analysis/summary.rs`
- `src/cli.rs`

Estimated scope:
- Medium

#### Task 6: Implement `diff`

Description:
Compare two snapshots and report resource, namespace, image, and warning/failure drift.

Acceptance criteria:
- [ ] Added, removed, and changed resources are reported
- [ ] Image drift from `app-versions.yaml` is included
- [ ] Output works in Markdown and JSON

Verification:
- [ ] `cargo test analysis::diff::tests -- --nocapture`

Dependencies:
- Task 2
- Task 3

Files likely touched:
- `src/analysis/diff.rs`
- `src/cli.rs`

Estimated scope:
- Medium

#### Task 7: Implement `graph`

Description:
Export topology graphs as Mermaid and JSON.

Acceptance criteria:
- [ ] Owner, ingress, service, pod, and node edges are emitted
- [ ] Namespace filtering works
- [ ] Tests cover stable Mermaid edge generation

Verification:
- [ ] `cargo test analysis::graph::tests -- --nocapture`

Dependencies:
- Task 3

Files likely touched:
- `src/analysis/graph.rs`
- `src/cli.rs`

Estimated scope:
- Medium

#### Task 8: Implement `ai-pack`

Description:
Generate a compact AI triage bundle from one snapshot.

Acceptance criteria:
- [ ] `AI-PACK.md` is generated
- [ ] `ai-pack.json` is generated
- [ ] `graph.mmd` is generated through the shared graph engine

Verification:
- [ ] `cargo test analysis::ai_pack::tests -- --nocapture`

Dependencies:
- Task 5
- Task 7

Files likely touched:
- `src/analysis/ai_pack.rs`
- `src/cli.rs`

Estimated scope:
- Medium

### Checkpoint: Command Surface

- [x] All commands compile and run locally
- [x] Unit tests cover command engines

### Phase 4: CLI, Docs, and Validation

#### Task 9: Integrate CLI, docs, and help text

Description:
Wire the new subcommands into Clap, document usage, and update handoff docs.

Acceptance criteria:
- [x] CLI help exposes all new commands and options
- [x] README examples are updated
- [x] design and implementation docs stay aligned with shipped behavior

Verification:
- [x] `cargo run -- --help`
- [x] `cargo run -- summarize --help`

Dependencies:
- Tasks 5-8

Files likely touched:
- `src/cli.rs`
- `README.md`
- wrapper docs in the sibling repo after rebundling

Estimated scope:
- Small

#### Task 10: Rebuild, rebundle, validate, and fix findings

Description:
Build the Linux binary, rebundle the wrapper image, run offline and cluster validation, fix defects, and retest.

Acceptance criteria:
- [ ] `cargo test` targeted suites pass
- [ ] Linux binary builds successfully
- [ ] Wrapper image builds and runs
- [ ] Snapshot analysis commands work against a real generated snapshot
- [ ] Any bugs found during validation are fixed and revalidated

Verification:
- [ ] targeted `cargo test` suites pass
- [ ] `cross build --target x86_64-unknown-linux-musl --release --bin kubectl-crust-gather`
- [ ] wrapper image validation against primary cluster snapshot

Dependencies:
- Tasks 1-9

Files likely touched:
- upstream and wrapper repos

Estimated scope:
- Large

## Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Large snapshots slow analysis | High | Use indexes first and lazy-load raw YAML |
| Older snapshots lack agent artifacts | Medium | Fail clearly and document the requirement |
| Audit rules produce noisy findings | Medium | Keep rules narrow and attach evidence paths |
| Diff output is too noisy | Medium | Provide top-level summaries with capped samples |
| Graphs become unreadable on large clusters | Medium | Add namespace filter and cap displayed nodes if needed |

## Open Questions

- Whether OCI-backed offline analysis should be part of the first shipped version.
- Whether `ai-pack` should embed log excerpts by default or only references.

## Verification Checklist

- [ ] Every command has unit coverage for its core logic
- [ ] Snapshot loading works across directory and compressed archive inputs
- [x] A real snapshot generated by the current collector can be summarized, diffed, audited, graphed, and packed

## Final Validation Notes

- Unit tests passed for:
  - `analysis::source::tests`
  - `analysis::snapshot::tests`
  - `analysis::queries::tests`
  - `analysis::summary::tests`
  - `analysis::audit::tests`
  - `analysis::graph::tests`
  - `analysis::diff::tests`
  - `analysis::ai_pack::tests`
  - `gather::agent_artifacts::tests`
  - `gather::config::tests`
- CLI help was validated through `cargo run -- --help` and `cargo run -- collect --help`.
- Real snapshot validation was run on the local primary cluster in safe mode and exercised:
  - `summarize`
  - `audit`
  - `graph`
  - `ai-pack`
  - `diff`
- A real validation bug was fixed during this campaign:
  - agent indexes originally recorded unsanitized paths while archive writes used sanitized paths
  - this broke lazy raw-YAML loading for resources whose names contained `:`
- [ ] The wrapper repo bundles the final tested binary
