use std::collections::{BTreeMap, BTreeSet};

use clap::Args;
use serde::Serialize;

use super::{
    cli::{AnalysisFormat, ComparisonInput, OutputOptions, emit_json, emit_text},
    queries::yaml_digest,
    snapshot::{AppVersionEntry, Snapshot},
};

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceChangeKind {
    Added,
    Removed,
    Changed,
}

#[derive(Clone, Debug, Serialize)]
pub struct ResourceChange {
    pub change: ResourceChangeKind,
    pub id: String,
    pub kind: String,
    pub namespace: Option<String>,
    pub name: String,
    pub path: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct CountDelta {
    pub scope: String,
    pub before: usize,
    pub after: usize,
    pub delta: isize,
}

#[derive(Clone, Debug, Serialize)]
pub struct NamespaceDelta {
    pub namespace: String,
    pub resource_before: usize,
    pub resource_after: usize,
    pub log_before: usize,
    pub log_after: usize,
}

#[derive(Clone, Debug, Serialize)]
pub struct ImageChange {
    pub namespace: String,
    pub name: String,
    pub container: String,
    pub before: String,
    pub after: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct DiffReport {
    pub before_context: Option<String>,
    pub after_context: Option<String>,
    pub resource_changes: Vec<ResourceChange>,
    pub counts_by_kind: Vec<CountDelta>,
    pub namespace_deltas: Vec<NamespaceDelta>,
    pub image_changes: Vec<ImageChange>,
    pub warning_delta: isize,
    pub failure_delta: isize,
}

#[derive(Clone, Debug, Args)]
pub struct DiffCommand {
    #[command(flatten)]
    pub input: ComparisonInput,

    #[command(flatten)]
    pub output: OutputOptions,
}

pub fn build(before: &Snapshot, after: &Snapshot) -> anyhow::Result<DiffReport> {
    let before_index = before
        .resources
        .iter()
        .map(|resource| (resource.id.clone(), resource))
        .collect::<BTreeMap<_, _>>();
    let after_index = after
        .resources
        .iter()
        .map(|resource| (resource.id.clone(), resource))
        .collect::<BTreeMap<_, _>>();

    let mut ids = before_index.keys().cloned().collect::<BTreeSet<_>>();
    ids.extend(after_index.keys().cloned());

    let mut resource_changes = vec![];
    for id in ids {
        match (before_index.get(&id), after_index.get(&id)) {
            (None, Some(resource)) => resource_changes.push(ResourceChange {
                change: ResourceChangeKind::Added,
                id,
                kind: resource.kind.clone(),
                namespace: resource.namespace.clone(),
                name: resource.name.clone(),
                path: resource.path.clone(),
            }),
            (Some(resource), None) => resource_changes.push(ResourceChange {
                change: ResourceChangeKind::Removed,
                id,
                kind: resource.kind.clone(),
                namespace: resource.namespace.clone(),
                name: resource.name.clone(),
                path: resource.path.clone(),
            }),
            (Some(before_resource), Some(after_resource)) => {
                let before_digest = yaml_digest(before, before_resource.path.as_str())?;
                let after_digest = yaml_digest(after, after_resource.path.as_str())?;
                if before_digest != after_digest {
                    resource_changes.push(ResourceChange {
                        change: ResourceChangeKind::Changed,
                        id,
                        kind: after_resource.kind.clone(),
                        namespace: after_resource.namespace.clone(),
                        name: after_resource.name.clone(),
                        path: after_resource.path.clone(),
                    });
                }
            }
            (None, None) => {}
        }
    }
    resource_changes.sort_by(|left, right| left.id.cmp(&right.id));

    Ok(DiffReport {
        before_context: before.report.inputs.context.clone(),
        after_context: after.report.inputs.context.clone(),
        resource_changes,
        counts_by_kind: count_deltas(before, after),
        namespace_deltas: namespace_deltas(before, after),
        image_changes: image_changes(before, after),
        warning_delta: after.warnings.len() as isize - before.warnings.len() as isize,
        failure_delta: after.failures.len() as isize - before.failures.len() as isize,
    })
}

pub fn render_markdown(report: &DiffReport) -> String {
    let mut out = String::new();
    out.push_str("# Snapshot Diff\n\n");
    out.push_str(&format!(
        "- before: {}\n- after: {}\n- resource_changes: {}\n- warning_delta: {}\n- failure_delta: {}\n\n",
        report.before_context.as_deref().unwrap_or("<default>"),
        report.after_context.as_deref().unwrap_or("<default>"),
        report.resource_changes.len(),
        report.warning_delta,
        report.failure_delta
    ));

    out.push_str("## Resource Changes\n\n");
    if report.resource_changes.is_empty() {
        out.push_str("- none\n\n");
    } else {
        for change in &report.resource_changes {
            out.push_str(&format!("- {:?} `{}`\n", change.change, change.id));
        }
        out.push('\n');
    }

    out.push_str("## Image Changes\n\n");
    if report.image_changes.is_empty() {
        out.push_str("- none\n\n");
    } else {
        for change in &report.image_changes {
            out.push_str(&format!(
                "- `{}/{}` container `{}`: `{}` -> `{}`\n",
                change.namespace, change.name, change.container, change.before, change.after
            ));
        }
        out.push('\n');
    }

    out.push_str("## Count Deltas\n\n");
    let count_deltas = report
        .counts_by_kind
        .iter()
        .filter(|delta| delta.delta != 0)
        .collect::<Vec<_>>();
    if count_deltas.is_empty() {
        out.push_str("- none\n");
    } else {
        for delta in count_deltas {
            out.push_str(&format!(
                "- `{}`: {} -> {} ({:+})\n",
                delta.scope, delta.before, delta.after, delta.delta
            ));
        }
    }
    out.push('\n');

    out.push_str("## Namespace Deltas\n\n");
    if report.namespace_deltas.is_empty() {
        out.push_str("- none\n");
    } else {
        for delta in &report.namespace_deltas {
            out.push_str(&format!(
                "- `{}` resources {} -> {}, logs {} -> {}\n",
                delta.namespace,
                delta.resource_before,
                delta.resource_after,
                delta.log_before,
                delta.log_after
            ));
        }
    }

    out
}

pub fn run(command: DiffCommand) -> anyhow::Result<()> {
    let before = Snapshot::open(command.input.before)?;
    let after = Snapshot::open(command.input.after)?;
    let report = build(&before, &after)?;

    match command.output.format {
        AnalysisFormat::Markdown => emit_text(
            command.output.output.as_ref(),
            render_markdown(&report).as_str(),
        ),
        AnalysisFormat::Json => emit_json(command.output.output.as_ref(), &report),
    }
}

fn count_deltas(before: &Snapshot, after: &Snapshot) -> Vec<CountDelta> {
    let before_counts = count_resources_by_scope(before);
    let after_counts = count_resources_by_scope(after);
    let mut scopes = before_counts.keys().cloned().collect::<BTreeSet<_>>();
    scopes.extend(after_counts.keys().cloned());

    scopes
        .into_iter()
        .map(|scope| {
            let before = *before_counts.get(&scope).unwrap_or(&0);
            let after = *after_counts.get(&scope).unwrap_or(&0);
            CountDelta {
                scope,
                before,
                after,
                delta: after as isize - before as isize,
            }
        })
        .collect()
}

fn count_resources_by_scope(snapshot: &Snapshot) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for resource in &snapshot.resources {
        *counts.entry(resource.kind.clone()).or_insert(0) += 1;
    }
    counts
}

fn namespace_deltas(before: &Snapshot, after: &Snapshot) -> Vec<NamespaceDelta> {
    let before_resources = namespaced_resource_counts(before);
    let after_resources = namespaced_resource_counts(after);
    let before_logs = namespaced_log_counts(before);
    let after_logs = namespaced_log_counts(after);
    let mut namespaces = before_resources.keys().cloned().collect::<BTreeSet<_>>();
    namespaces.extend(after_resources.keys().cloned());
    namespaces.extend(before_logs.keys().cloned());
    namespaces.extend(after_logs.keys().cloned());

    namespaces
        .into_iter()
        .filter_map(|namespace| {
            let resource_before = *before_resources.get(&namespace).unwrap_or(&0);
            let resource_after = *after_resources.get(&namespace).unwrap_or(&0);
            let log_before = *before_logs.get(&namespace).unwrap_or(&0);
            let log_after = *after_logs.get(&namespace).unwrap_or(&0);

            (resource_before != resource_after || log_before != log_after).then_some(
                NamespaceDelta {
                    namespace,
                    resource_before,
                    resource_after,
                    log_before,
                    log_after,
                },
            )
        })
        .collect()
}

fn namespaced_resource_counts(snapshot: &Snapshot) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for namespace in snapshot
        .resources
        .iter()
        .filter_map(|resource| resource.namespace.as_ref())
    {
        *counts.entry(namespace.clone()).or_insert(0) += 1;
    }
    counts
}

fn namespaced_log_counts(snapshot: &Snapshot) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for namespace in snapshot
        .logs
        .iter()
        .filter_map(|log| log.namespace.as_ref())
    {
        *counts.entry(namespace.clone()).or_insert(0) += 1;
    }
    counts
}

fn image_changes(before: &Snapshot, after: &Snapshot) -> Vec<ImageChange> {
    let before_index = image_index(&before.app_versions);
    let after_index = image_index(&after.app_versions);
    let mut keys = before_index.keys().cloned().collect::<BTreeSet<_>>();
    keys.extend(after_index.keys().cloned());

    let mut changes = vec![];
    for key in keys {
        let before_value = before_index.get(&key);
        let after_value = after_index.get(&key);
        if before_value != after_value {
            let (namespace, name, container) = key;
            changes.push(ImageChange {
                namespace,
                name,
                container,
                before: before_value.cloned().unwrap_or_else(|| "<missing>".into()),
                after: after_value.cloned().unwrap_or_else(|| "<missing>".into()),
            });
        }
    }
    changes
}

fn image_index(entries: &[AppVersionEntry]) -> BTreeMap<(String, String, String), String> {
    entries
        .iter()
        .map(|entry| {
            (
                (
                    entry.namespace.clone(),
                    entry.name.clone(),
                    entry.container.clone(),
                ),
                entry.version.clone(),
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path};

    use serde_json::Value;

    use crate::{
        analysis::{snapshot::Snapshot, test_support::sample_snapshot},
        gather::{agent_artifacts::ResourceIndexEntry, report::RunMessage},
    };

    use super::{ResourceChangeKind, build, render_markdown};

    #[test]
    fn diff_detects_removed_changed_and_image_drift() {
        let before = sample_snapshot("diff-before").expect("before");
        let after = sample_snapshot("diff-after").expect("after");

        mutate_after_snapshot(after.root()).expect("mutate");

        let before = Snapshot::open(before.root()).expect("before snapshot");
        let after = Snapshot::open(after.root()).expect("after snapshot");
        let report = build(&before, &after).expect("report");

        assert!(report.resource_changes.iter().any(|change| {
            matches!(change.change, ResourceChangeKind::Removed) && change.name == "orphan"
        }));
        assert!(report.resource_changes.iter().any(|change| {
            matches!(change.change, ResourceChangeKind::Changed) && change.name == "web-abc"
        }));
        assert!(
            report
                .image_changes
                .iter()
                .any(|change| change.after == "nginx:1.28.0")
        );
        assert_eq!(report.warning_delta, 1);
    }

    #[test]
    fn self_diff_omits_empty_count_and_namespace_sections() {
        let snapshot = sample_snapshot("diff-same").expect("snapshot");
        let snapshot = Snapshot::open(snapshot.root()).expect("snapshot");

        let report = build(&snapshot, &snapshot).expect("report");
        assert!(report.resource_changes.is_empty());
        assert!(report.image_changes.is_empty());
        assert!(report.namespace_deltas.is_empty());
        assert!(report.counts_by_kind.iter().all(|delta| delta.delta == 0));

        let markdown = render_markdown(&report);
        assert!(markdown.contains("## Count Deltas\n\n- none"));
        assert!(markdown.contains("## Namespace Deltas\n\n- none"));
    }

    fn mutate_after_snapshot(root: &Path) -> anyhow::Result<()> {
        let resource_index = root.join("resource-index.jsonl");
        let resources = fs::read_to_string(&resource_index)?
            .lines()
            .filter(|line| !line.trim().is_empty())
            .map(serde_json::from_str::<ResourceIndexEntry>)
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .filter(|resource| resource.name != "orphan")
            .collect::<Vec<_>>();
        write_jsonl(&resource_index, &resources)?;

        let app_versions = root.join("app-versions.yaml");
        let mut versions: Vec<Value> = serde_yaml::from_str(&fs::read_to_string(&app_versions)?)?;
        if let Some(entry) = versions.iter_mut().find(|entry| entry["name"] == "web-abc") {
            entry["version"] = Value::String("nginx:1.28.0".into());
        }
        fs::write(&app_versions, serde_yaml::to_string(&versions)?)?;

        let pod_path = root.join("namespaces/default/v1/pod/web-abc.yaml");
        let content = fs::read_to_string(&pod_path)?;
        fs::write(&pod_path, content.replace("nginx:1.27.0", "nginx:1.28.0"))?;

        let warnings_path = root.join("run-warnings.yaml");
        let mut warnings: Vec<RunMessage> =
            serde_yaml::from_str(&fs::read_to_string(&warnings_path)?)?;
        warnings.push(warnings[0].clone());
        fs::write(&warnings_path, serde_yaml::to_string(&warnings)?)?;
        Ok(())
    }

    fn write_jsonl(path: &Path, resources: &[ResourceIndexEntry]) -> anyhow::Result<()> {
        let mut content = resources
            .iter()
            .map(serde_json::to_string)
            .collect::<Result<Vec<_>, _>>()?
            .join("\n");
        content.push('\n');
        fs::write(path, content)?;
        Ok(())
    }
}
