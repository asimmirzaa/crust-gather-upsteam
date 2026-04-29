# Snapshot Analysis Suite Design

## Status
Accepted

## Date
2026-04-29

## Goal

Turn `crust-gather` from a snapshot collector into an offline analysis product that can:

- compare snapshots
- summarize incident-relevant signals
- audit security and upgrade risks
- generate AI-friendly triage packs
- export navigable topology graphs

The design must build on the snapshot artifacts already emitted by the collector, especially:

- `analysis-schema.yaml`
- `run-report.yaml`
- `run-stats.yaml`
- `run-failures.yaml`
- `run-warnings.yaml`
- `AGENT-START.md`
- `resource-index.jsonl`
- `relation-index.jsonl`
- `log-index.jsonl`
- `snapshot.sqlite`

## Product Surface

The analysis suite adds five new offline commands:

1. `crust-gather summarize`
2. `crust-gather diff`
3. `crust-gather audit`
4. `crust-gather ai-pack`
5. `crust-gather graph`

These commands operate on extracted snapshot directories or compressed snapshot archives.

Shared CLI contract:

- single-snapshot commands use `--snapshot`
- pairwise commands use `--before` and `--after`
- all commands accept `--format markdown|json`
- commands print to stdout by default
- file-producing commands accept `--output`

## Design Principles

### Reuse Snapshot Metadata First

Use the agent-oriented artifacts as the primary navigation layer. Only open raw YAML or log files when the higher-level artifacts do not contain enough information.

This keeps analysis:

- fast
- deterministic
- AI-friendly
- resilient to large snapshots

### Offline-First

All commands work without cluster access. The analysis suite must not depend on a live API server.

### Output for Humans and Agents

Every command should emit readable Markdown by default and structured JSON when requested.

### Stable, Composable Building Blocks

The implementation should create a reusable analysis layer instead of embedding logic directly in each CLI subcommand.

## Architecture

Introduce a new top-level module:

- `src/analysis/`

It contains five layers.

### 1. Snapshot Workspace

Responsibility:

- accept a snapshot path
- detect whether it is:
  - an extracted snapshot directory
  - a directory containing `snapshot/`
  - a `.tar.gz`
  - a `.zip`
- materialize an accessible local workspace

Design:

- extracted directories are used in place
- compressed archives are unpacked to a temp directory
- commands read from the unpacked workspace and do not mutate the original snapshot

### 2. Snapshot Model Loader

Responsibility:

- load run metadata
- load agent indexes
- provide helpers for loading raw YAML objects only when needed

Primary types:

- `Snapshot`
- `SnapshotPaths`
- `SnapshotIndexes`

Data sources:

- YAML:
  - `analysis-schema.yaml`
  - `run-report.yaml`
  - `run-stats.yaml`
  - `run-failures.yaml`
  - `run-warnings.yaml`
  - `app-versions.yaml`
- JSONL:
  - `resource-index.jsonl`
  - `relation-index.jsonl`
  - `log-index.jsonl`
- raw YAML resources under:
  - `cluster/`
  - `namespaces/`

### 3. Domain Queries

Responsibility:

- expose reusable offline queries over a snapshot

Examples:

- list pods with restart hotspots
- find non-ready nodes
- match services to pods via selectors
- resolve ingress backends to services
- compute owner chains from `relation-index.jsonl`
- find risky RBAC bindings
- compare image versions between snapshots

This layer is the product core. The CLI commands should be thin renderers over these queries.

## Snapshot Analysis Schema Contract

The analysis suite needs a non-SQLite compatibility contract.

Add a small YAML artifact to every snapshot:

- `analysis-schema.yaml`

Contents:

- `schema_version`
- `collector_version`
- `collector_revision`
- `features`
  - `resource_index`
  - `relation_index`
  - `log_index`
  - `snapshot_sqlite`
  - `agent_start`

Rationale:

- SQLite is optional for analysis
- JSONL and YAML readers still need version negotiation
- unsupported snapshots should fail clearly and early

### 4. Analysis Engines

Responsibility:

- implement higher-level analyses using domain queries

Engines:

- `summary`
- `diff`
- `audit`
- `ai_pack`
- `graph`

### 5. Renderers

Responsibility:

- render Markdown, JSON, and Mermaid outputs

Formats:

- Markdown for humans
- JSON for automation
- Mermaid for topology visualization

## Command Designs

### `summarize`

Purpose:

- produce an incident-oriented cluster summary

Inputs:

- one snapshot

Outputs:

- Markdown by default
- JSON when requested

Signals included:

- run success / warnings / failures
- top failing collectors
- non-ready nodes
- non-ready pods
- restart hotspots
- pods with repeated log errors
- namespaces with highest pod/log volume
- workloads missing requests or limits
- services without matching pods
- external exposure summary:
  - LoadBalancers
  - NodePorts
  - Ingresses
  - Gateways
  - HTTPRoutes
  - TLSRoutes

### `diff`

Purpose:

- compare two snapshots and report change

Inputs:

- `before`
- `after`

Outputs:

- Markdown by default
- JSON when requested

Comparison axes:

- resource added / removed / changed
- counts by kind
- changes by namespace
- image changes from `app-versions.yaml`
- new warnings / failures
- node set changes
- external exposure changes
  - ingress changes
  - gateway route changes

Change detection strategy:

- compare by resource id from `resource-index.jsonl`
- for shared ids, compare semantic canonicalized object content

Canonicalization rules:

- parse YAML into a structured value
- remove volatile fields:
  - `metadata.resourceVersion`
  - `metadata.managedFields`
  - `metadata.uid`
  - `metadata.generation` when only rollout bookkeeping changed
  - recorder annotations used only by time-series snapshots
- compare canonical JSON encodings, not raw YAML bytes

### `audit`

Purpose:

- flag security and upgrade risks

Inputs:

- one snapshot

Outputs:

- Markdown by default
- JSON when requested

Checks in v1:

- cluster-admin bindings
- wildcard RBAC rules
- privileged containers
- hostPath mounts
- hostNetwork / hostPID / hostIPC
- containers missing resource limits
- public service exposure:
  - `LoadBalancer`
  - `NodePort`
- beta / alpha API usage as upgrade risk
- pods running as root or allowing privilege escalation when visible in spec
- service accounts mounted into privileged workloads
- Gateway API public exposure:
  - listeners on externally exposed gateways
  - routes bound to exposed gateways

### `ai-pack`

Purpose:

- generate a compact offline investigation pack for AI agents

Inputs:

- one snapshot

Outputs:

- output directory containing:
  - `AI-PACK.md`
  - `ai-pack.json`
  - `graph.mmd`

Contents:

- overall run summary
- top warnings and failures
- likely hotspots
- top noisy logs with previews
- suggested first files to inspect
- workload and topology hints
- concise operating instructions for agents

Design note:

This does not duplicate the full snapshot. It builds a compact navigation bundle around it.

### `graph`

Purpose:

- export offline topology views

Inputs:

- one snapshot
- optional namespace focus

Outputs:

- Mermaid `flowchart` text
- JSON edge/node export when requested

Edges in v1:

- owner relationships from `relation-index.jsonl`
- pod -> node
- service -> pod via label selectors
- ingress -> service via backend references
- gateway -> route bindings where present
- route -> service backends for HTTPRoute and TLSRoute where present

## Archive Compatibility

The analysis suite should support:

- extracted snapshot directories
- `.tar.gz` snapshots
- `.zip` snapshots

OCI-backed analysis is deferred. The serving path already supports OCI, but the first analysis suite should focus on local snapshot bundles.

## Data Model Reuse

Existing types should be reused where practical:

- `RunReport`
- `CollectorStats`
- `RunMessage`
- `ResourceIndexEntry`
- `RelationIndexEntry`
- `LogIndexEntry`

Required change:

- these types need `Deserialize` in addition to `Serialize`
- add a shared CLI input/output layer:
  - `SnapshotInput`
  - `ComparisonInput`
  - `AnalysisFormat`
  - `OutputTarget`

## Tradeoffs

### Why Not Build Everything on SQLite?

The snapshot already includes `snapshot.sqlite`, but the analysis suite should not depend on it as the single source of truth.

Reasons:

- older snapshots may not have it
- the JSONL files are easier to inspect and debug
- topology and audit checks still need raw YAML access

Decision:

- JSONL + YAML is primary
- SQLite is optional verification or future optimization

### Why Markdown + JSON + Mermaid?

- Markdown is the best default for operator review
- JSON supports automation and future MCP tooling
- Mermaid keeps graph export dependency-free

## Risks

### Large Snapshots

Risk:

- reading too many raw YAML files eagerly

Mitigation:

- load indexes first
- lazily load raw resource YAML only for targeted analyses

### Schema Drift Across Snapshots

Risk:

- older snapshots may not have the agent artifacts

Mitigation:

- fail clearly when required analysis inputs are missing
- keep fallback surface small in v1

### False Positives in Audit

Risk:

- security heuristics can over-report

Mitigation:

- document each finding with rule name and evidence path
- keep checks concrete and explainable

## Validation Strategy

Validation must cover:

1. unit tests for snapshot loading and individual checks
2. golden-style tests for Markdown/JSON outputs
3. end-to-end runs against real generated snapshots
4. wrapper rebuild and local cluster validation after bundling the new binary

## Implementation Boundaries

In this campaign, the implementation will include:

- all five commands
- the shared analysis module
- snapshot archive loading
- tests for core analysis logic
- wrapper rebundle and validation

Deferred unless discovered necessary during implementation:

- OCI-backed analysis inputs
- graphical UI
- fully customizable policy engine
- cross-snapshot time series storage beyond pairwise diff
