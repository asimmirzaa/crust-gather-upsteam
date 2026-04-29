use std::collections::BTreeMap;

use clap::Args;
use serde::Serialize;

use crate::gather::{report::CollectorStats, report::RunTotals};

use super::{
    cli::{AnalysisFormat, OutputOptions, SnapshotInput, emit_json, emit_text},
    queries::{
        ContainerResourceGap, ExposureSurface, GatewayRouteTarget, NodeHealth, PodHealth,
        ServiceTarget, missing_resource_gaps, node_health, pod_health, service_targets,
        exposure_surfaces, gateway_route_targets,
    },
    snapshot::Snapshot,
};

const MARKDOWN_COLLECTOR_LIMIT: usize = 20;
const MARKDOWN_POD_LIMIT: usize = 40;
const MARKDOWN_LOG_LIMIT: usize = 40;
const MARKDOWN_GAP_LIMIT: usize = 60;
const MARKDOWN_EXPOSURE_LIMIT: usize = 40;
const MARKDOWN_NAMESPACE_LIMIT: usize = 40;

#[derive(Clone, Debug, Serialize)]
pub struct CollectorIssue {
    pub collector: String,
    pub listed_objects: usize,
    pub collected_objects: usize,
    pub written_files: usize,
    pub failed_objects: usize,
    pub warnings: usize,
}

#[derive(Clone, Debug, Serialize)]
pub struct NamespaceActivity {
    pub namespace: String,
    pub resource_count: usize,
    pub pod_count: usize,
    pub log_files: usize,
}

#[derive(Clone, Debug, Serialize)]
pub struct LogHotspot {
    pub namespace: Option<String>,
    pub kind: String,
    pub name: String,
    pub container: Option<String>,
    pub path: String,
    pub error_count: usize,
    pub warn_count: usize,
    pub line_count: usize,
}

#[derive(Clone, Debug, Serialize)]
pub struct SummaryReport {
    pub schema_version: u32,
    pub collector_version: String,
    pub collector_revision: String,
    pub success: bool,
    pub context: Option<String>,
    pub totals: RunTotals,
    pub collector_issues: Vec<CollectorIssue>,
    pub non_ready_nodes: Vec<NodeHealth>,
    pub pod_hotspots: Vec<PodHealth>,
    pub log_hotspots: Vec<LogHotspot>,
    pub namespace_activity: Vec<NamespaceActivity>,
    pub orphan_services: Vec<ServiceTarget>,
    pub missing_resource_gaps: Vec<ContainerResourceGap>,
    pub exposure_surfaces: Vec<ExposureSurface>,
    pub gateway_routes: Vec<GatewayRouteTarget>,
    pub warnings: usize,
    pub failures: usize,
}

#[derive(Clone, Debug, Args)]
pub struct SummaryCommand {
    #[command(flatten)]
    pub input: SnapshotInput,

    #[command(flatten)]
    pub output: OutputOptions,
}

pub fn build(snapshot: &Snapshot) -> anyhow::Result<SummaryReport> {
    let non_ready_nodes = node_health(snapshot)?
        .into_iter()
        .filter(|node| !node.ready)
        .collect::<Vec<_>>();
    let pod_hotspots = pod_health(snapshot)?
        .into_iter()
        .filter(|pod| {
            pod.restart_count > 0
                || pod.ready_containers < pod.total_containers
                || pod.reason.is_some()
                || !matches!(pod.phase.as_deref(), Some("Running" | "Succeeded"))
        })
        .collect::<Vec<_>>();
    let orphan_services = service_targets(snapshot)?
        .into_iter()
        .filter(|service| !service.selectors.is_empty() && service.ready_pods.is_empty())
        .collect::<Vec<_>>();

    Ok(SummaryReport {
        schema_version: snapshot.schema.schema_version,
        collector_version: snapshot.report.identity.collector_version.clone(),
        collector_revision: snapshot.report.identity.collector_revision.clone(),
        success: snapshot.report.success,
        context: snapshot.report.inputs.context.clone(),
        totals: snapshot.report.totals.clone(),
        collector_issues: collector_issues(&snapshot.stats),
        non_ready_nodes,
        pod_hotspots,
        log_hotspots: log_hotspots(snapshot),
        namespace_activity: namespace_activity(snapshot),
        orphan_services,
        missing_resource_gaps: missing_resource_gaps(snapshot)?,
        exposure_surfaces: exposure_surfaces(snapshot)?,
        gateway_routes: gateway_route_targets(snapshot)?,
        warnings: snapshot.warnings.len(),
        failures: snapshot.failures.len(),
    })
}

pub fn render_markdown(report: &SummaryReport) -> String {
    let mut out = String::new();
    out.push_str("# Snapshot Summary\n\n");
    out.push_str(&format!(
        "- success: {}\n- context: {}\n- collector: {} ({})\n- resources listed: {}\n- files written: {}\n- warnings: {}\n- failures: {}\n\n",
        report.success,
        report.context.as_deref().unwrap_or("<default>"),
        report.collector_version,
        report.collector_revision,
        report.totals.listed_objects,
        report.totals.written_files,
        report.warnings,
        report.failures,
    ));

    out.push_str("## Collectors With Issues\n\n");
    if report.collector_issues.is_empty() {
        out.push_str("- none\n\n");
    } else {
        for issue in report.collector_issues.iter().take(MARKDOWN_COLLECTOR_LIMIT) {
            out.push_str(&format!(
                "- `{}`: failed_objects={}, warnings={}, listed={}, collected={}, written={}\n",
                issue.collector,
                issue.failed_objects,
                issue.warnings,
                issue.listed_objects,
                issue.collected_objects,
                issue.written_files
            ));
        }
        append_omitted(&mut out, report.collector_issues.len(), MARKDOWN_COLLECTOR_LIMIT);
        out.push('\n');
    }

    out.push_str("## Cluster Health\n\n");
    if report.non_ready_nodes.is_empty() {
        out.push_str("- all nodes ready\n");
    } else {
        for node in &report.non_ready_nodes {
            out.push_str(&format!(
                "- node `{}` not ready{}\n",
                node.object.name,
                node.reason
                    .as_deref()
                    .map(|reason| format!(" ({reason})"))
                    .unwrap_or_default()
            ));
        }
    }
    if report.pod_hotspots.is_empty() {
        out.push_str("- no pod hotspots\n\n");
    } else {
        for pod in report.pod_hotspots.iter().take(MARKDOWN_POD_LIMIT) {
            out.push_str(&format!(
                "- pod `{}/{}` phase={:?} restarts={} ready={}/{}{}\n",
                pod.object.namespace.as_deref().unwrap_or("_cluster"),
                pod.object.name,
                pod.phase,
                pod.restart_count,
                pod.ready_containers,
                pod.total_containers,
                pod.reason
                    .as_deref()
                    .map(|reason| format!(" reason={reason}"))
                    .unwrap_or_default()
            ));
        }
        append_omitted(&mut out, report.pod_hotspots.len(), MARKDOWN_POD_LIMIT);
        out.push('\n');
    }

    out.push_str("## Log Hotspots\n\n");
    if report.log_hotspots.is_empty() {
        out.push_str("- no warning/error-heavy logs indexed\n\n");
    } else {
        for log in report.log_hotspots.iter().take(MARKDOWN_LOG_LIMIT) {
            out.push_str(&format!(
                "- `{}` errors={} warns={} lines={}\n",
                log.path, log.error_count, log.warn_count, log.line_count
            ));
        }
        append_omitted(&mut out, report.log_hotspots.len(), MARKDOWN_LOG_LIMIT);
        out.push('\n');
    }

    out.push_str("## Capacity Gaps\n\n");
    if report.missing_resource_gaps.is_empty() {
        out.push_str("- no missing resource request/limit gaps detected\n\n");
    } else {
        for gap in report
            .missing_resource_gaps
            .iter()
            .take(MARKDOWN_GAP_LIMIT)
        {
            out.push_str(&format!(
                "- `{}/{}` container `{}`: {}\n",
                gap.object.namespace.as_deref().unwrap_or("_cluster"),
                gap.object.name,
                gap.container,
                gap.issue
            ));
        }
        append_omitted(
            &mut out,
            report.missing_resource_gaps.len(),
            MARKDOWN_GAP_LIMIT,
        );
        out.push('\n');
    }

    out.push_str("## Exposure\n\n");
    if report.exposure_surfaces.is_empty() {
        out.push_str("- no external exposure detected\n");
    } else {
        for exposure in report
            .exposure_surfaces
            .iter()
            .take(MARKDOWN_EXPOSURE_LIMIT)
        {
            out.push_str(&format!(
                "- {} `{}/{}`: {}\n",
                exposure.exposure_type,
                exposure.namespace.as_deref().unwrap_or("_cluster"),
                exposure.name,
                exposure.detail
            ));
        }
        append_omitted(
            &mut out,
            report.exposure_surfaces.len(),
            MARKDOWN_EXPOSURE_LIMIT,
        );
    }
    if !report.orphan_services.is_empty() {
        for service in &report.orphan_services {
            out.push_str(&format!(
                "- orphan service `{}/{}` has selector {:?} but matched no ready pods\n",
                service.object.namespace.as_deref().unwrap_or("_cluster"),
                service.object.name,
                service.selectors
            ));
        }
    }
    if !report.gateway_routes.is_empty() {
        for route in &report.gateway_routes {
            out.push_str(&format!(
                "- route `{}/{}` -> gateways [{}] -> services [{}]\n",
                route.namespace.as_deref().unwrap_or("_cluster"),
                route.name,
                route.gateway_refs.join(", "),
                route.service_names.join(", ")
            ));
        }
    }
    out.push('\n');

    out.push_str("## Namespace Activity\n\n");
    if report.namespace_activity.is_empty() {
        out.push_str("- no namespaced resources collected\n");
    } else {
        for entry in report
            .namespace_activity
            .iter()
            .take(MARKDOWN_NAMESPACE_LIMIT)
        {
            out.push_str(&format!(
                "- `{}` resources={} pods={} log_files={}\n",
                entry.namespace, entry.resource_count, entry.pod_count, entry.log_files
            ));
        }
        append_omitted(
            &mut out,
            report.namespace_activity.len(),
            MARKDOWN_NAMESPACE_LIMIT,
        );
    }

    out
}

pub fn run(command: SummaryCommand) -> anyhow::Result<()> {
    let snapshot = Snapshot::open(command.input.snapshot)?;
    let report = build(&snapshot)?;

    match command.output.format {
        AnalysisFormat::Markdown => {
            emit_text(command.output.output.as_ref(), render_markdown(&report).as_str())
        }
        AnalysisFormat::Json => emit_json(command.output.output.as_ref(), &report),
    }
}

fn collector_issues(stats: &BTreeMap<String, CollectorStats>) -> Vec<CollectorIssue> {
    let mut issues = stats
        .iter()
        .filter(|(_, stat)| stat.failed_objects > 0 || stat.warnings > 0)
        .map(|(collector, stat)| CollectorIssue {
            collector: collector.clone(),
            listed_objects: stat.listed_objects,
            collected_objects: stat.collected_objects,
            written_files: stat.written_files,
            failed_objects: stat.failed_objects,
            warnings: stat.warnings,
        })
        .collect::<Vec<_>>();
    issues.sort_by(|left, right| {
        right
            .failed_objects
            .cmp(&left.failed_objects)
            .then_with(|| right.warnings.cmp(&left.warnings))
            .then_with(|| left.collector.cmp(&right.collector))
    });
    issues
}

fn log_hotspots(snapshot: &Snapshot) -> Vec<LogHotspot> {
    let mut hotspots = snapshot
        .logs
        .iter()
        .filter(|log| log.error_count > 0 || log.warn_count > 0)
        .map(|log| LogHotspot {
            namespace: log.namespace.clone(),
            kind: log.kind.clone(),
            name: log.name.clone(),
            container: log.container.clone(),
            path: log.path.clone(),
            error_count: log.error_count,
            warn_count: log.warn_count,
            line_count: log.line_count,
        })
        .collect::<Vec<_>>();
    hotspots.sort_by(|left, right| {
        right
            .error_count
            .cmp(&left.error_count)
            .then_with(|| right.warn_count.cmp(&left.warn_count))
            .then_with(|| left.path.cmp(&right.path))
    });
    hotspots
}

fn namespace_activity(snapshot: &Snapshot) -> Vec<NamespaceActivity> {
    let mut summary = BTreeMap::<String, NamespaceActivity>::new();

    for resource in snapshot.resources.iter().filter_map(|resource| {
        resource
            .namespace
            .as_ref()
            .map(|namespace| (namespace.clone(), resource.kind.as_str()))
    }) {
        let entry = summary
            .entry(resource.0.clone())
            .or_insert_with(|| NamespaceActivity {
                namespace: resource.0.clone(),
                resource_count: 0,
                pod_count: 0,
                log_files: 0,
            });
        entry.resource_count += 1;
        if resource.1 == "Pod" {
            entry.pod_count += 1;
        }
    }

    for log in snapshot.logs.iter().filter_map(|log| log.namespace.as_ref()) {
        let entry = summary
            .entry(log.clone())
            .or_insert_with(|| NamespaceActivity {
                namespace: log.clone(),
                resource_count: 0,
                pod_count: 0,
                log_files: 0,
            });
        entry.log_files += 1;
    }

    let mut values = summary.into_values().collect::<Vec<_>>();
    values.sort_by(|left, right| {
        right
            .pod_count
            .cmp(&left.pod_count)
            .then_with(|| right.log_files.cmp(&left.log_files))
            .then_with(|| right.resource_count.cmp(&left.resource_count))
            .then_with(|| left.namespace.cmp(&right.namespace))
    });
    values
}

fn append_omitted(out: &mut String, total: usize, limit: usize) {
    if total > limit {
        out.push_str(&format!("- ... {} more omitted\n", total - limit));
    }
}

#[cfg(test)]
mod tests {
    use crate::analysis::{snapshot::Snapshot, test_support::sample_snapshot};

    use super::{build, render_markdown};

    #[test]
    fn summary_collects_hotspots_and_exposure() {
        let fixture = sample_snapshot("summary-report").expect("fixture");
        let snapshot = Snapshot::open(fixture.root()).expect("snapshot");
        let report = build(&snapshot).expect("report");

        assert!(!report.non_ready_nodes.is_empty());
        assert!(report.pod_hotspots.iter().any(|pod| pod.object.name == "web-abc"));
        assert!(report.orphan_services.iter().any(|service| service.object.name == "orphan"));
        assert!(
            report
                .exposure_surfaces
                .iter()
                .any(|surface| surface.exposure_type == "LoadBalancer")
        );
    }

    #[test]
    fn summary_markdown_mentions_key_findings() {
        let fixture = sample_snapshot("summary-markdown").expect("fixture");
        let snapshot = Snapshot::open(fixture.root()).expect("snapshot");
        let markdown = render_markdown(&build(&snapshot).expect("report"));

        assert!(markdown.contains("web-abc"));
        assert!(markdown.contains("orphan service"));
        assert!(markdown.contains("LoadBalancer"));
    }
}
