use std::collections::{BTreeMap, HashMap};
use std::fs;

use anyhow::Context;
use k8s_openapi::api::core::v1::Pod;
use rusqlite::{Connection, params};
use serde::Serialize;
use serde_json::Value;
use tempfile::NamedTempFile;

use crate::gather::report::{CollectorStats, RunMessage, RunReport};
use crate::gather::representation::{ArchivePath, Representation};
use crate::scanners::pod_support::pod_container_refs;

const INDEX_SCHEMA_VERSION: i64 = 1;
const CLUSTER_SCOPE: &str = "_cluster";
const LINE_PREVIEW_LIMIT: usize = 240;

#[derive(Clone, Debug, Serialize)]
pub struct OwnerRefEntry {
    pub api_version: String,
    pub kind: String,
    pub name: String,
    pub uid: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ResourceReference {
    pub relation: String,
    pub target_kind: String,
    pub target_namespace: Option<String>,
    pub target_name: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct ResourceIndexEntry {
    pub id: String,
    pub api_version: String,
    pub kind: String,
    pub namespace: Option<String>,
    pub name: String,
    pub path: String,
    pub uid: Option<String>,
    pub resource_version: Option<String>,
    pub created_at: Option<String>,
    pub phase: Option<String>,
    pub node_name: Option<String>,
    pub service_account: Option<String>,
    pub labels: BTreeMap<String, String>,
    pub annotation_keys: Vec<String>,
    pub owner_refs: Vec<OwnerRefEntry>,
    pub containers: Vec<crate::scanners::pod_support::PodContainerRef>,
    pub references: Vec<ResourceReference>,
}

#[derive(Clone, Debug, Serialize)]
pub struct RelationIndexEntry {
    pub source_id: String,
    pub source_path: String,
    pub relation: String,
    pub target_id: String,
    pub target_kind: String,
    pub target_namespace: Option<String>,
    pub target_name: String,
    pub target_path: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct LogIndexEntry {
    pub path: String,
    pub resource_path: String,
    pub resource_id: Option<String>,
    pub namespace: Option<String>,
    pub name: String,
    pub kind: String,
    pub container: Option<String>,
    pub log_kind: String,
    pub source: String,
    pub size_bytes: usize,
    pub line_count: usize,
    pub warn_count: usize,
    pub error_count: usize,
    pub first_line: Option<String>,
    pub last_line: Option<String>,
}

#[derive(Clone, Debug)]
struct ObservedLogEntry {
    path: String,
    resource_path: String,
    namespace: Option<String>,
    name: String,
    kind: String,
    container: Option<String>,
    log_kind: String,
    source: String,
    size_bytes: usize,
    line_count: usize,
    warn_count: usize,
    error_count: usize,
    first_line: Option<String>,
    last_line: Option<String>,
}

#[derive(Clone, Debug)]
pub struct FinalizedAgentArtifacts {
    pub agent_start: String,
    pub resource_index: String,
    pub relation_index: String,
    pub log_index: String,
    pub sqlite_bytes: Vec<u8>,
}

#[derive(Default)]
pub struct AgentArtifactsState {
    resources: BTreeMap<String, ResourceIndexEntry>,
    logs: BTreeMap<String, ObservedLogEntry>,
}

impl AgentArtifactsState {
    pub fn observe(&mut self, repr: &Representation) -> anyhow::Result<()> {
        match repr.path() {
            ArchivePath::Cluster(path) | ArchivePath::Namespaced(path) => {
                let path = path_to_string(path.as_path())?;
                let resource = parse_resource_entry(path, repr.data())?;
                self.resources.insert(resource.path.clone(), resource);
            }
            ArchivePath::Logs(path) => {
                let path = path_to_string(path.as_path())?;
                let log = parse_log_entry(path, repr.data())?;
                self.logs.insert(log.path.clone(), log);
            }
            ArchivePath::Empty
            | ArchivePath::NamespacedList(_)
            | ArchivePath::ClusterList(_)
            | ArchivePath::Custom(_) => {}
        }

        Ok(())
    }

    pub fn finalize(
        &self,
        report: &RunReport,
        stats: &BTreeMap<String, CollectorStats>,
        failures: &[RunMessage],
        warnings: &[RunMessage],
    ) -> anyhow::Result<FinalizedAgentArtifacts> {
        let resources = self.resources.values().cloned().collect::<Vec<_>>();
        let resource_by_id = resources
            .iter()
            .map(|resource| (resource.id.clone(), resource.clone()))
            .collect::<HashMap<_, _>>();
        let relations = build_relations(&resources, &resource_by_id);
        let logs = self
            .logs
            .values()
            .cloned()
            .map(|log| log.finalize(&self.resources))
            .collect::<Vec<_>>();

        Ok(FinalizedAgentArtifacts {
            agent_start: render_agent_start(report, &resources, &relations, &logs),
            resource_index: jsonl(&resources)?,
            relation_index: jsonl(&relations)?,
            log_index: jsonl(&logs)?,
            sqlite_bytes: build_sqlite(
                report, stats, failures, warnings, &resources, &relations, &logs,
            )?,
        })
    }
}

fn render_agent_start(
    report: &RunReport,
    resources: &[ResourceIndexEntry],
    relations: &[RelationIndexEntry],
    logs: &[LogIndexEntry],
) -> String {
    let status = if report.success {
        "successful"
    } else {
        "partial-or-failed"
    };
    let context = report.inputs.context.as_deref().unwrap_or("<default>");
    let finished_at = report
        .finished_at
        .map(|value| value.to_rfc3339())
        .unwrap_or_else(|| "<unknown>".to_string());

    format!(
        "# AGENT START\n\
\n\
This snapshot is structured for fast AI-assisted triage.\n\
\n\
Summary\n\
- status: {status}\n\
- collector: {} {}\n\
- revision: {}\n\
- context: {context}\n\
- started_at: {}\n\
- finished_at: {finished_at}\n\
- duration_ms: {}\n\
- resource_index_entries: {}\n\
- relation_index_entries: {}\n\
- log_index_entries: {}\n\
- warnings: {}\n\
- failures: {}\n\
\n\
Read Order\n\
1. `run-report.yaml` for overall success, inputs, and totals.\n\
2. `run-failures.yaml` and `run-warnings.yaml` for partial or suspicious collection.\n\
3. `run-stats.yaml` for per-collector coverage.\n\
4. `resource-index.jsonl` to find objects quickly by kind, namespace, owner, node, or service account.\n\
5. `relation-index.jsonl` to traverse object relationships. `owned-by` points from child to owner.\n\
6. `log-index.jsonl` to find the highest-signal logs before opening raw files.\n\
7. `snapshot.sqlite` for SQL-based navigation across the same indexes.\n\
\n\
Archive Layout\n\
- `cluster/` holds cluster-scoped object YAML.\n\
- `namespaces/` holds namespaced object YAML and pod log files.\n\
- `app-versions.yaml` lists pod container images.\n\
\n\
SQLite Tables\n\
- `run_summary`\n\
- `run_messages`\n\
- `resources`\n\
- `relations`\n\
- `logs`\n\
\n\
Primary Keys\n\
- resource ids use `{}` for namespaced objects and `{}` for cluster-scoped objects.\n\
- `resource-index.jsonl` and `relation-index.jsonl` use the same ids as `snapshot.sqlite`.\n",
        report.identity.collector_name,
        report.identity.collector_version,
        report.identity.collector_revision,
        report.started_at.to_rfc3339(),
        report.duration_ms.unwrap_or_default(),
        resources.len(),
        relations.len(),
        logs.len(),
        report.warnings.len(),
        report.failures.len(),
        "apiVersion/Kind/namespace/name",
        "apiVersion/Kind/_cluster/name",
    )
}

fn jsonl<T: Serialize>(values: &[T]) -> anyhow::Result<String> {
    let mut output = values
        .iter()
        .map(serde_json::to_string)
        .collect::<Result<Vec<_>, _>>()
        .map(|lines| lines.join("\n"))
        .map_err(anyhow::Error::from)?;

    if !output.is_empty() {
        output.push('\n');
    }

    Ok(output)
}

fn build_sqlite(
    report: &RunReport,
    stats: &BTreeMap<String, CollectorStats>,
    failures: &[RunMessage],
    warnings: &[RunMessage],
    resources: &[ResourceIndexEntry],
    relations: &[RelationIndexEntry],
    logs: &[LogIndexEntry],
) -> anyhow::Result<Vec<u8>> {
    let file = NamedTempFile::new()?;
    let path = file.path().to_path_buf();
    let conn = Connection::open(&path)?;

    conn.execute_batch(
        "
        PRAGMA journal_mode = OFF;
        PRAGMA synchronous = OFF;
        CREATE TABLE run_summary (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            schema_version INTEGER NOT NULL,
            collector_name TEXT NOT NULL,
            collector_version TEXT NOT NULL,
            collector_revision TEXT NOT NULL,
            context TEXT,
            source TEXT NOT NULL,
            output_path TEXT NOT NULL,
            output_encoding TEXT NOT NULL,
            node_log_mode TEXT NOT NULL,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            duration_ms INTEGER,
            success INTEGER NOT NULL,
            total_collectors INTEGER NOT NULL,
            listed_objects INTEGER NOT NULL,
            collected_objects INTEGER NOT NULL,
            written_files INTEGER NOT NULL,
            failed_objects INTEGER NOT NULL,
            warnings INTEGER NOT NULL,
            resource_index_entries INTEGER NOT NULL,
            relation_index_entries INTEGER NOT NULL,
            log_index_entries INTEGER NOT NULL,
            inputs_json TEXT NOT NULL,
            stats_json TEXT NOT NULL
        );
        CREATE TABLE run_messages (
            severity TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            phase TEXT NOT NULL,
            collector TEXT NOT NULL,
            object TEXT,
            message TEXT NOT NULL
        );
        CREATE TABLE resources (
            id TEXT PRIMARY KEY,
            api_version TEXT NOT NULL,
            kind TEXT NOT NULL,
            namespace TEXT,
            name TEXT NOT NULL,
            path TEXT NOT NULL,
            uid TEXT,
            resource_version TEXT,
            created_at TEXT,
            phase TEXT,
            node_name TEXT,
            service_account TEXT,
            labels_json TEXT NOT NULL,
            annotation_keys_json TEXT NOT NULL,
            owner_refs_json TEXT NOT NULL,
            containers_json TEXT NOT NULL,
            references_json TEXT NOT NULL
        );
        CREATE TABLE relations (
            source_id TEXT NOT NULL,
            source_path TEXT NOT NULL,
            relation TEXT NOT NULL,
            target_id TEXT NOT NULL,
            target_kind TEXT NOT NULL,
            target_namespace TEXT,
            target_name TEXT NOT NULL,
            target_path TEXT
        );
        CREATE TABLE logs (
            path TEXT PRIMARY KEY,
            resource_path TEXT NOT NULL,
            resource_id TEXT,
            namespace TEXT,
            name TEXT NOT NULL,
            kind TEXT NOT NULL,
            container TEXT,
            log_kind TEXT NOT NULL,
            source TEXT NOT NULL,
            size_bytes INTEGER NOT NULL,
            line_count INTEGER NOT NULL,
            warn_count INTEGER NOT NULL,
            error_count INTEGER NOT NULL,
            first_line TEXT,
            last_line TEXT
        );
        CREATE INDEX idx_resources_kind_namespace_name ON resources(kind, namespace, name);
        CREATE INDEX idx_relations_source ON relations(source_id, relation);
        CREATE INDEX idx_relations_target ON relations(target_id, relation);
        CREATE INDEX idx_logs_resource ON logs(resource_id, container, log_kind);
        ",
    )?;

    conn.execute(
        "
        INSERT INTO run_summary (
            id, schema_version, collector_name, collector_version, collector_revision,
            context, source, output_path, output_encoding, node_log_mode,
            started_at, finished_at, duration_ms, success,
            total_collectors, listed_objects, collected_objects, written_files,
            failed_objects, warnings, resource_index_entries, relation_index_entries,
            log_index_entries, inputs_json, stats_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ",
        params![
            1_i64,
            INDEX_SCHEMA_VERSION,
            report.identity.collector_name,
            report.identity.collector_version,
            report.identity.collector_revision,
            report.inputs.context,
            report.inputs.source,
            report.inputs.output_path,
            report.inputs.output_encoding,
            report.inputs.node_log_mode,
            report.started_at.to_rfc3339(),
            report.finished_at.map(|value| value.to_rfc3339()),
            report.duration_ms,
            if report.success { 1_i64 } else { 0_i64 },
            report.totals.collectors as i64,
            report.totals.listed_objects as i64,
            report.totals.collected_objects as i64,
            report.totals.written_files as i64,
            report.totals.failed_objects as i64,
            report.totals.warnings as i64,
            resources.len() as i64,
            relations.len() as i64,
            logs.len() as i64,
            serde_json::to_string(&report.inputs)?,
            serde_json::to_string(stats)?,
        ],
    )?;

    for message in failures {
        insert_message(&conn, "failure", message)?;
    }
    for message in warnings {
        insert_message(&conn, "warning", message)?;
    }

    for resource in resources {
        conn.execute(
            "
            INSERT INTO resources (
                id, api_version, kind, namespace, name, path, uid, resource_version,
                created_at, phase, node_name, service_account, labels_json,
                annotation_keys_json, owner_refs_json, containers_json, references_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ",
            params![
                resource.id,
                resource.api_version,
                resource.kind,
                resource.namespace,
                resource.name,
                resource.path,
                resource.uid,
                resource.resource_version,
                resource.created_at,
                resource.phase,
                resource.node_name,
                resource.service_account,
                serde_json::to_string(&resource.labels)?,
                serde_json::to_string(&resource.annotation_keys)?,
                serde_json::to_string(&resource.owner_refs)?,
                serde_json::to_string(&resource.containers)?,
                serde_json::to_string(&resource.references)?,
            ],
        )?;
    }

    for relation in relations {
        conn.execute(
            "
            INSERT INTO relations (
                source_id, source_path, relation, target_id, target_kind,
                target_namespace, target_name, target_path
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ",
            params![
                relation.source_id,
                relation.source_path,
                relation.relation,
                relation.target_id,
                relation.target_kind,
                relation.target_namespace,
                relation.target_name,
                relation.target_path,
            ],
        )?;
    }

    for log in logs {
        conn.execute(
            "
            INSERT INTO logs (
                path, resource_path, resource_id, namespace, name, kind, container,
                log_kind, source, size_bytes, line_count, warn_count, error_count,
                first_line, last_line
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ",
            params![
                log.path,
                log.resource_path,
                log.resource_id,
                log.namespace,
                log.name,
                log.kind,
                log.container,
                log.log_kind,
                log.source,
                log.size_bytes as i64,
                log.line_count as i64,
                log.warn_count as i64,
                log.error_count as i64,
                log.first_line,
                log.last_line,
            ],
        )?;
    }

    drop(conn);
    fs::read(path).map_err(Into::into)
}

fn insert_message(conn: &Connection, severity: &str, message: &RunMessage) -> anyhow::Result<()> {
    conn.execute(
        "INSERT INTO run_messages (severity, timestamp, phase, collector, object, message) VALUES (?, ?, ?, ?, ?, ?)",
        params![
            severity,
            message.timestamp.to_rfc3339(),
            message.phase,
            message.collector,
            message.object,
            message.message,
        ],
    )?;

    Ok(())
}

fn parse_resource_entry(path: String, data: &str) -> anyhow::Result<ResourceIndexEntry> {
    let value = serde_yaml::from_str::<Value>(data)
        .with_context(|| format!("failed to parse resource YAML for {path}"))?;
    let api_version = get_string(&value, &["apiVersion"])
        .ok_or_else(|| anyhow::anyhow!("resource missing apiVersion: {path}"))?;
    let kind = get_string(&value, &["kind"])
        .ok_or_else(|| anyhow::anyhow!("resource missing kind: {path}"))?;
    let name = get_string(&value, &["metadata", "name"])
        .ok_or_else(|| anyhow::anyhow!("resource missing metadata.name: {path}"))?;
    let namespace = get_string(&value, &["metadata", "namespace"]);
    let uid = get_string(&value, &["metadata", "uid"]);
    let resource_version = get_string(&value, &["metadata", "resourceVersion"]);
    let created_at = get_string(&value, &["metadata", "creationTimestamp"]);
    let labels = get_map(&value, &["metadata", "labels"]);
    let annotation_keys = get_map(&value, &["metadata", "annotations"])
        .into_keys()
        .collect::<Vec<_>>();
    let owner_refs = get_array(&value, &["metadata", "ownerReferences"])
        .into_iter()
        .filter_map(|entry| {
            let api_version = get_string(entry, &["apiVersion"])?;
            let kind = get_string(entry, &["kind"])?;
            let name = get_string(entry, &["name"])?;
            Some(OwnerRefEntry {
                api_version,
                kind,
                name,
                uid: get_string(entry, &["uid"]),
            })
        })
        .collect::<Vec<_>>();

    let phase = get_string(&value, &["status", "phase"]);
    let mut node_name = None;
    let mut service_account = None;
    let mut containers = vec![];
    let mut references = vec![];

    if api_version == "v1" && kind == "Pod" {
        if let Ok(pod) = serde_yaml::from_str::<Pod>(data) {
            node_name = pod.spec.as_ref().and_then(|spec| spec.node_name.clone());
            service_account = pod
                .spec
                .as_ref()
                .and_then(|spec| spec.service_account_name.clone());
            containers = pod_container_refs(&pod);
            references.extend(pod_references(&pod, namespace.clone()));
        }
    }

    Ok(ResourceIndexEntry {
        id: resource_id(&api_version, &kind, namespace.as_deref(), &name),
        api_version,
        kind,
        namespace,
        name,
        path,
        uid,
        resource_version,
        created_at,
        phase,
        node_name,
        service_account,
        labels,
        annotation_keys,
        owner_refs,
        containers,
        references,
    })
}

fn pod_references(pod: &Pod, namespace: Option<String>) -> Vec<ResourceReference> {
    let Some(spec) = pod.spec.as_ref() else {
        return vec![];
    };

    let mut references = vec![];

    if let Some(service_account) = spec.service_account_name.as_ref() {
        references.push(ResourceReference {
            relation: "uses-service-account".to_string(),
            target_kind: "ServiceAccount".to_string(),
            target_namespace: namespace.clone(),
            target_name: service_account.clone(),
        });
    }

    if let Some(node_name) = spec.node_name.as_ref() {
        references.push(ResourceReference {
            relation: "scheduled-on".to_string(),
            target_kind: "Node".to_string(),
            target_namespace: None,
            target_name: node_name.clone(),
        });
    }

    if let Some(volumes) = spec.volumes.as_ref() {
        for volume in volumes {
            if let Some(config_map) = volume.config_map.as_ref() {
                references.push(ResourceReference {
                    relation: "references-configmap".to_string(),
                    target_kind: "ConfigMap".to_string(),
                    target_namespace: namespace.clone(),
                    target_name: config_map.name.clone(),
                });
            }
            if let Some(secret) = volume.secret.as_ref() {
                references.push(ResourceReference {
                    relation: "references-secret".to_string(),
                    target_kind: "Secret".to_string(),
                    target_namespace: namespace.clone(),
                    target_name: secret.secret_name.clone().unwrap_or_default(),
                });
            }
            if let Some(claim) = volume.persistent_volume_claim.as_ref() {
                references.push(ResourceReference {
                    relation: "references-persistentvolumeclaim".to_string(),
                    target_kind: "PersistentVolumeClaim".to_string(),
                    target_namespace: namespace.clone(),
                    target_name: claim.claim_name.clone(),
                });
            }
        }
    }

    references.retain(|reference| !reference.target_name.is_empty());
    references
}

fn build_relations(
    resources: &[ResourceIndexEntry],
    resource_by_id: &HashMap<String, ResourceIndexEntry>,
) -> Vec<RelationIndexEntry> {
    let mut relations = vec![];

    for resource in resources {
        for owner in &resource.owner_refs {
            let owner_namespace = resource.namespace.clone();
            let target_id = resource_id(
                owner.api_version.as_str(),
                owner.kind.as_str(),
                owner_namespace.as_deref(),
                owner.name.as_str(),
            );
            relations.push(RelationIndexEntry {
                source_id: resource.id.clone(),
                source_path: resource.path.clone(),
                relation: "owned-by".to_string(),
                target_kind: owner.kind.clone(),
                target_namespace: owner_namespace,
                target_name: owner.name.clone(),
                target_path: resource_by_id
                    .get(&target_id)
                    .map(|entry| entry.path.clone()),
                target_id,
            });
        }

        for reference in &resource.references {
            let target_id = resource_id(
                infer_api_version(reference.target_kind.as_str()),
                reference.target_kind.as_str(),
                reference.target_namespace.as_deref(),
                reference.target_name.as_str(),
            );
            relations.push(RelationIndexEntry {
                source_id: resource.id.clone(),
                source_path: resource.path.clone(),
                relation: reference.relation.clone(),
                target_kind: reference.target_kind.clone(),
                target_namespace: reference.target_namespace.clone(),
                target_name: reference.target_name.clone(),
                target_path: resource_by_id
                    .get(&target_id)
                    .map(|entry| entry.path.clone()),
                target_id,
            });
        }
    }

    relations.sort_by(|left, right| {
        (
            left.source_id.as_str(),
            left.relation.as_str(),
            left.target_id.as_str(),
        )
            .cmp(&(
                right.source_id.as_str(),
                right.relation.as_str(),
                right.target_id.as_str(),
            ))
    });
    relations.dedup_by(|left, right| {
        left.source_id == right.source_id
            && left.relation == right.relation
            && left.target_id == right.target_id
    });
    relations
}

fn infer_api_version(kind: &str) -> &'static str {
    match kind {
        "Node" | "Pod" | "ConfigMap" | "Secret" | "PersistentVolumeClaim" | "ServiceAccount" => {
            "v1"
        }
        _ => "v1",
    }
}

impl ObservedLogEntry {
    fn finalize(self, resources: &BTreeMap<String, ResourceIndexEntry>) -> LogIndexEntry {
        let resolved_resource = resources.get(&self.resource_path);
        let resource_id = resolved_resource.map(|resource| resource.id.clone());
        let kind = resolved_resource
            .map(|resource| resource.kind.clone())
            .unwrap_or(self.kind);

        LogIndexEntry {
            path: self.path,
            resource_path: self.resource_path,
            resource_id,
            namespace: self.namespace,
            name: self.name,
            kind,
            container: self.container,
            log_kind: self.log_kind,
            source: self.source,
            size_bytes: self.size_bytes,
            line_count: self.line_count,
            warn_count: self.warn_count,
            error_count: self.error_count,
            first_line: self.first_line,
            last_line: self.last_line,
        }
    }
}

fn parse_log_entry(path: String, data: &str) -> anyhow::Result<ObservedLogEntry> {
    let parsed = parse_log_path(&path)?;
    let lines = data.lines().map(str::trim).collect::<Vec<_>>();
    let warn_count = lines
        .iter()
        .filter(|line| {
            let line = line.to_ascii_lowercase();
            line.contains("warning") || line.starts_with("warn") || line.contains("[warn")
        })
        .count();
    let error_count = lines
        .iter()
        .filter(|line| {
            let line = line.to_ascii_lowercase();
            line.contains("error") || line.starts_with("err") || line.contains("[error")
        })
        .count();

    Ok(ObservedLogEntry {
        path,
        resource_path: parsed.resource_path,
        namespace: parsed.namespace,
        name: parsed.name,
        kind: parsed.kind,
        container: parsed.container,
        log_kind: parsed.log_kind,
        source: parsed.source,
        size_bytes: data.as_bytes().len(),
        line_count: lines.len(),
        warn_count,
        error_count,
        first_line: lines.first().map(|line| limit_preview(line)),
        last_line: lines.last().map(|line| limit_preview(line)),
    })
}

struct ParsedLogPath {
    resource_path: String,
    namespace: Option<String>,
    name: String,
    kind: String,
    container: Option<String>,
    log_kind: String,
    source: String,
}

fn parse_log_path(path: &str) -> anyhow::Result<ParsedLogPath> {
    let parts = path.split('/').collect::<Vec<_>>();
    match parts.as_slice() {
        [
            "namespaces",
            namespace,
            api_version,
            kind,
            name,
            container,
            "current.log",
        ] => Ok(ParsedLogPath {
            resource_path: format!("namespaces/{namespace}/{api_version}/{kind}/{name}.yaml"),
            namespace: Some((*namespace).to_string()),
            name: (*name).to_string(),
            kind: (*kind).to_string(),
            container: Some((*container).to_string()),
            log_kind: "current".to_string(),
            source: "pod-container".to_string(),
        }),
        [
            "namespaces",
            namespace,
            api_version,
            kind,
            name,
            container,
            "previous.log",
        ] => Ok(ParsedLogPath {
            resource_path: format!("namespaces/{namespace}/{api_version}/{kind}/{name}.yaml"),
            namespace: Some((*namespace).to_string()),
            name: (*name).to_string(),
            kind: (*kind).to_string(),
            container: Some((*container).to_string()),
            log_kind: "previous".to_string(),
            source: "pod-container".to_string(),
        }),
        ["cluster", api_version, kind, name, rest @ ..] if !rest.is_empty() => {
            let relative_path = rest.join("/");
            let (log_kind, source) = match relative_path.as_str() {
                "kubelet-log-path.log" => ("node-legacy", "node"),
                "kubelet.log" => ("node-unit", "node"),
                _ if rest.len() == 1 && relative_path.ends_with(".log") => ("node-unit", "node"),
                _ => ("node-custom", "node-custom"),
            };

            Ok(ParsedLogPath {
                resource_path: format!("cluster/{api_version}/{kind}/{name}.yaml"),
                namespace: None,
                name: (*name).to_string(),
                kind: (*kind).to_string(),
                container: None,
                log_kind: log_kind.to_string(),
                source: source.to_string(),
            })
        }
        _ => Err(anyhow::anyhow!("unsupported log path format: {path}")),
    }
}

fn resource_id(api_version: &str, kind: &str, namespace: Option<&str>, name: &str) -> String {
    format!(
        "{api_version}/{kind}/{}/{}",
        namespace.unwrap_or(CLUSTER_SCOPE),
        name
    )
}

fn get_value<'a>(value: &'a Value, path: &[&str]) -> Option<&'a Value> {
    let mut current = value;
    for segment in path {
        current = current.as_object()?.get(*segment)?;
    }
    Some(current)
}

fn get_string(value: &Value, path: &[&str]) -> Option<String> {
    get_value(value, path)?.as_str().map(ToString::to_string)
}

fn get_map(value: &Value, path: &[&str]) -> BTreeMap<String, String> {
    get_value(value, path)
        .and_then(Value::as_object)
        .map(|value| {
            value
                .iter()
                .filter_map(|(key, value)| Some((key.clone(), value.as_str()?.to_string())))
                .collect()
        })
        .unwrap_or_default()
}

fn get_array<'a>(value: &'a Value, path: &[&str]) -> Vec<&'a Value> {
    get_value(value, path)
        .and_then(Value::as_array)
        .map(|value| value.iter().collect())
        .unwrap_or_default()
}

fn limit_preview(line: &str) -> String {
    let line = line.trim();
    let mut preview = line.chars().take(LINE_PREVIEW_LIMIT).collect::<String>();
    if line.chars().count() > LINE_PREVIEW_LIMIT {
        preview.push_str("...");
    }
    preview
}

fn path_to_string(path: &std::path::Path) -> anyhow::Result<String> {
    path.to_str()
        .map(ToString::to_string)
        .ok_or_else(|| anyhow::anyhow!("archive path is not valid UTF-8"))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use chrono::Utc;

    use crate::gather::report::{RunIdentity, RunInputs, RunReport};

    use super::{AgentArtifactsState, ResourceIndexEntry};
    use crate::gather::representation::{ArchivePath, Representation};

    #[test]
    fn finalize_builds_resource_relation_log_indexes_and_sqlite() {
        let mut state = AgentArtifactsState::default();
        state
            .observe(
                &Representation::new()
                    .with_path(ArchivePath::Namespaced(
                        "namespaces/default/v1/pod/web-123.yaml".into(),
                    ))
                    .with_data(
                        r#"
apiVersion: v1
kind: Pod
metadata:
  name: web-123
  namespace: default
  ownerReferences:
    - apiVersion: apps/v1
      kind: ReplicaSet
      name: web-abc
spec:
  nodeName: worker-1
  serviceAccountName: web-sa
  containers:
    - name: web
      image: nginx:1.27
status:
  phase: Running
"#,
                    ),
            )
            .unwrap();
        state
            .observe(
                &Representation::new()
                    .with_path(ArchivePath::Namespaced(
                        "namespaces/default/apps-v1/replicaset/web-abc.yaml".into(),
                    ))
                    .with_data(
                        r#"
apiVersion: apps/v1
kind: ReplicaSet
metadata:
  name: web-abc
  namespace: default
"#,
                    ),
            )
            .unwrap();
        state
            .observe(
                &Representation::new()
                    .with_path(ArchivePath::Cluster("cluster/v1/node/worker-1.yaml".into()))
                    .with_data(
                        r#"
apiVersion: v1
kind: Node
metadata:
  name: worker-1
"#,
                    ),
            )
            .unwrap();
        state
            .observe(
                &Representation::new()
                    .with_path(ArchivePath::Logs(
                        "namespaces/default/v1/pod/web-123/web/current.log".into(),
                    ))
                    .with_data("INFO boot\nWARN cache miss\nERROR boom\n"),
            )
            .unwrap();

        let report = RunReport {
            identity: RunIdentity::default(),
            inputs: RunInputs::default(),
            started_at: Utc::now(),
            finished_at: Some(Utc::now()),
            duration_ms: Some(42),
            success: true,
            totals: Default::default(),
            stats: BTreeMap::new(),
            warnings: vec![],
            failures: vec![],
        };

        let artifacts = state
            .finalize(&report, &BTreeMap::new(), &[], &[])
            .expect("finalize to succeed");

        assert!(artifacts.agent_start.contains("resource-index.jsonl"));
        assert!(artifacts.resource_index.contains("\"kind\":\"Pod\""));
        assert!(
            artifacts
                .relation_index
                .contains("\"relation\":\"owned-by\"")
        );
        assert!(
            artifacts
                .relation_index
                .contains("\"relation\":\"scheduled-on\"")
        );
        assert!(artifacts.log_index.contains("\"warn_count\":1"));
        assert!(artifacts.log_index.contains("\"error_count\":1"));
        assert!(artifacts.sqlite_bytes.starts_with(b"SQLite format 3"));
    }

    #[test]
    fn resource_id_format_is_stable_for_cluster_and_namespace_scopes() {
        let entry = ResourceIndexEntry {
            id: "v1/Pod/default/web".into(),
            api_version: "v1".into(),
            kind: "Pod".into(),
            namespace: Some("default".into()),
            name: "web".into(),
            path: "namespaces/default/v1/pod/web.yaml".into(),
            uid: None,
            resource_version: None,
            created_at: None,
            phase: None,
            node_name: None,
            service_account: None,
            labels: BTreeMap::new(),
            annotation_keys: vec![],
            owner_refs: vec![],
            containers: vec![],
            references: vec![],
        };

        assert_eq!(entry.id, "v1/Pod/default/web");
        assert!(super::resource_id("v1", "Node", None, "worker-1").contains("/_cluster/"));
    }

    #[test]
    fn nested_custom_node_logs_are_supported_and_classified() {
        let mut state = AgentArtifactsState::default();
        state
            .observe(
                &Representation::new()
                    .with_path(ArchivePath::Cluster("cluster/v1/node/worker-1.yaml".into()))
                    .with_data(
                        r#"
apiVersion: v1
kind: Node
metadata:
  name: worker-1
"#,
                    ),
            )
            .unwrap();
        state
            .observe(
                &Representation::new()
                    .with_path(ArchivePath::Logs(
                        "cluster/v1/node/worker-1/var/log/syslog".into(),
                    ))
                    .with_data("WARN test\n"),
            )
            .unwrap();

        let report = RunReport {
            identity: RunIdentity::default(),
            inputs: RunInputs::default(),
            started_at: Utc::now(),
            finished_at: Some(Utc::now()),
            duration_ms: Some(1),
            success: true,
            totals: Default::default(),
            stats: BTreeMap::new(),
            warnings: vec![],
            failures: vec![],
        };

        let artifacts = state.finalize(&report, &BTreeMap::new(), &[], &[]).unwrap();
        assert!(artifacts.log_index.contains("\"log_kind\":\"node-custom\""));
        assert!(artifacts.log_index.contains("\"source\":\"node-custom\""));
        assert!(
            artifacts
                .log_index
                .contains("\"resource_id\":\"v1/Node/_cluster/worker-1\"")
        );
    }
}
