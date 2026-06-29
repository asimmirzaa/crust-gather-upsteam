use std::collections::BTreeMap;

use clap::Args;
use serde::Serialize;
use serde_json::Value;

use super::{
    cli::{AnalysisFormat, OutputOptions, SnapshotInput, emit_json, emit_text},
    queries::{
        ExposureSurface, ObjectRef, exposure_surfaces, missing_resource_gaps, node_health,
        object_ref, service_targets, workload_risks,
    },
    snapshot::Snapshot,
};

const MARKDOWN_FINDING_LIMIT: usize = 120;

#[derive(Clone, Debug, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    Critical,
    High,
    Medium,
    Low,
}

#[derive(Clone, Debug, Serialize)]
pub struct AuditFinding {
    pub rule_id: String,
    pub severity: Severity,
    pub title: String,
    pub object: Option<ObjectRef>,
    pub evidence: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct AuditReport {
    pub success: bool,
    pub totals: BTreeMap<String, usize>,
    pub findings: Vec<AuditFinding>,
}

#[derive(Clone, Debug, Args)]
pub struct AuditCommand {
    #[command(flatten)]
    pub input: SnapshotInput,

    #[command(flatten)]
    pub output: OutputOptions,
}

pub fn build(snapshot: &Snapshot) -> anyhow::Result<AuditReport> {
    let mut findings = vec![];
    if !snapshot.report.success || !snapshot.failures.is_empty() {
        findings.push(AuditFinding {
            rule_id: "snapshot.partial-run".into(),
            severity: Severity::High,
            title: "Snapshot completed with partial or failed collectors".into(),
            object: None,
            evidence: snapshot
                .failures
                .iter()
                .take(5)
                .map(|failure| format!("{}: {}", failure.collector, failure.message))
                .collect(),
        });
    }

    findings.extend(rbac_findings(snapshot)?);
    findings.extend(workload_findings(snapshot)?);
    findings.extend(exposure_findings(snapshot)?);
    findings.extend(api_version_findings(snapshot)?);
    findings.extend(node_findings(snapshot)?);

    findings.sort_by(|left, right| {
        left.severity
            .cmp(&right.severity)
            .then_with(|| left.rule_id.cmp(&right.rule_id))
            .then_with(|| {
                left.object
                    .as_ref()
                    .map(|object| &object.id)
                    .cmp(&right.object.as_ref().map(|object| &object.id))
            })
    });

    let mut totals = BTreeMap::new();
    for finding in &findings {
        *totals
            .entry(severity_key(&finding.severity).to_string())
            .or_insert(0) += 1;
    }

    Ok(AuditReport {
        success: snapshot.report.success,
        totals,
        findings,
    })
}

pub fn render_markdown(report: &AuditReport) -> String {
    let mut out = String::new();
    out.push_str("# Snapshot Audit\n\n");
    out.push_str(&format!("- success: {}\n", report.success));
    for (severity, count) in &report.totals {
        out.push_str(&format!("- {severity}: {count}\n"));
    }
    out.push('\n');

    if report.findings.is_empty() {
        out.push_str("No findings.\n");
        return out;
    }

    for finding in report.findings.iter().take(MARKDOWN_FINDING_LIMIT) {
        let object = finding
            .object
            .as_ref()
            .map(|object| {
                format!(
                    " `{}/{}`",
                    object.namespace.as_deref().unwrap_or("_cluster"),
                    object.name
                )
            })
            .unwrap_or_default();
        out.push_str(&format!(
            "## [{}] {}{}\n\n",
            severity_key(&finding.severity).to_uppercase(),
            finding.title,
            object
        ));
        out.push_str(&format!("- rule_id: `{}`\n", finding.rule_id));
        for evidence in &finding.evidence {
            out.push_str(&format!("- {evidence}\n"));
        }
        out.push('\n');
    }
    if report.findings.len() > MARKDOWN_FINDING_LIMIT {
        out.push_str(&format!(
            "- ... {} more findings omitted\n",
            report.findings.len() - MARKDOWN_FINDING_LIMIT
        ));
    }

    out
}

pub fn run(command: AuditCommand) -> anyhow::Result<()> {
    let snapshot = Snapshot::open(command.input.snapshot)?;
    let report = build(&snapshot)?;

    match command.output.format {
        AnalysisFormat::Markdown => emit_text(
            command.output.output.as_ref(),
            render_markdown(&report).as_str(),
        ),
        AnalysisFormat::Json => emit_json(command.output.output.as_ref(), &report),
    }
}

fn rbac_findings(snapshot: &Snapshot) -> anyhow::Result<Vec<AuditFinding>> {
    let mut findings = vec![];

    for resource in snapshot
        .resources
        .iter()
        .filter(|resource| matches!(resource.kind.as_str(), "ClusterRoleBinding" | "RoleBinding"))
    {
        let value = snapshot.load_resource_value(resource)?;
        if value.pointer("/roleRef/kind").and_then(Value::as_str) == Some("ClusterRole")
            && value.pointer("/roleRef/name").and_then(Value::as_str) == Some("cluster-admin")
        {
            let subjects = value
                .pointer("/subjects")
                .and_then(Value::as_array)
                .map(|subjects| {
                    subjects
                        .iter()
                        .filter_map(|subject| {
                            Some(format!(
                                "{}:{}:{}",
                                subject.get("kind")?.as_str()?,
                                subject
                                    .get("namespace")
                                    .and_then(Value::as_str)
                                    .unwrap_or("_cluster"),
                                subject.get("name")?.as_str()?
                            ))
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            findings.push(AuditFinding {
                rule_id: "rbac.cluster-admin-binding".into(),
                severity: Severity::Critical,
                title: "Cluster-admin binding detected".into(),
                object: Some(object_ref(resource)),
                evidence: if subjects.is_empty() {
                    vec!["roleRef points to cluster-admin".into()]
                } else {
                    subjects
                },
            });
        }
    }

    for resource in snapshot
        .resources
        .iter()
        .filter(|resource| matches!(resource.kind.as_str(), "ClusterRole" | "Role"))
    {
        let value = snapshot.load_resource_value(resource)?;
        let Some(rules) = value.pointer("/rules").and_then(Value::as_array) else {
            continue;
        };
        let mut evidence = vec![];
        for (index, rule) in rules.iter().enumerate() {
            let wildcard_verbs = array_contains(rule.get("verbs"), "*");
            let wildcard_resources = array_contains(rule.get("resources"), "*");
            let wildcard_groups = array_contains(rule.get("apiGroups"), "*");
            let wildcard_urls = array_contains(rule.get("nonResourceURLs"), "*");
            if wildcard_verbs || wildcard_resources || wildcard_groups || wildcard_urls {
                evidence.push(format!(
                    "rule[{index}] wildcards verbs={} resources={} apiGroups={} nonResourceURLs={}",
                    wildcard_verbs, wildcard_resources, wildcard_groups, wildcard_urls
                ));
            }
        }
        if !evidence.is_empty() {
            findings.push(AuditFinding {
                rule_id: "rbac.wildcard-rules".into(),
                severity: Severity::High,
                title: "RBAC wildcard permissions detected".into(),
                object: Some(object_ref(resource)),
                evidence,
            });
        }
    }

    Ok(findings)
}

fn workload_findings(snapshot: &Snapshot) -> anyhow::Result<Vec<AuditFinding>> {
    let mut findings = vec![];
    for risk in workload_risks(snapshot)? {
        findings.push(AuditFinding {
            rule_id: "workload.security-risk".into(),
            severity: Severity::High,
            title: "Risky workload security posture".into(),
            object: Some(risk.object),
            evidence: vec![risk.issue],
        });
    }

    for gap in missing_resource_gaps(snapshot)? {
        findings.push(AuditFinding {
            rule_id: "workload.missing-resources".into(),
            severity: Severity::Medium,
            title: "Container missing resource settings".into(),
            object: Some(gap.object),
            evidence: vec![format!("container {}: {}", gap.container, gap.issue)],
        });
    }

    Ok(findings)
}

fn exposure_findings(snapshot: &Snapshot) -> anyhow::Result<Vec<AuditFinding>> {
    let services = service_targets(snapshot)?;
    let exposures = exposure_surfaces(snapshot)?;
    let service_ids = snapshot
        .resources
        .iter()
        .filter(|resource| resource.kind == "Service")
        .map(|resource| resource.id.clone())
        .collect::<std::collections::HashSet<_>>();
    let mut findings = vec![];

    for service in services
        .iter()
        .filter(|service| !service.selectors.is_empty() && service.ready_pods.is_empty())
    {
        findings.push(AuditFinding {
            rule_id: "exposure.orphan-service".into(),
            severity: Severity::High,
            title: "Service exposes no ready backend pods".into(),
            object: Some(service.object.clone()),
            evidence: vec![
                format!("selector {:?}", service.selectors),
                format!("matched pods {}", service.matched_pods.len()),
            ],
        });
    }

    for exposure in exposures {
        if exposure.kind == "Service"
            && matches!(exposure.exposure_type.as_str(), "LoadBalancer" | "NodePort")
        {
            findings.push(AuditFinding {
                rule_id: "exposure.external-service".into(),
                severity: Severity::Medium,
                title: "Externally exposed service".into(),
                object: object_for_surface(snapshot, &exposure),
                evidence: vec![exposure.detail],
            });
        }
    }

    for ingress in super::queries::ingress_targets(snapshot)? {
        let missing = ingress
            .service_names
            .iter()
            .filter(|name| {
                let id = format!(
                    "v1/Service/{}/{}",
                    ingress.object.namespace.as_deref().unwrap_or("_cluster"),
                    name
                );
                !service_ids.contains(&id)
            })
            .cloned()
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            findings.push(AuditFinding {
                rule_id: "exposure.ingress-missing-backend".into(),
                severity: Severity::Medium,
                title: "Ingress references missing services".into(),
                object: Some(ingress.object),
                evidence: missing,
            });
        }
    }

    for route in super::queries::gateway_route_targets(snapshot)? {
        let namespace = route.namespace.as_deref().unwrap_or("_cluster");
        let missing = route
            .service_names
            .iter()
            .filter(|name| {
                let id = format!("v1/Service/{namespace}/{name}");
                !service_ids.contains(&id)
            })
            .cloned()
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            findings.push(AuditFinding {
                rule_id: "exposure.route-missing-backend".into(),
                severity: Severity::Medium,
                title: "Gateway route references missing services".into(),
                object: None,
                evidence: missing,
            });
        }
    }

    Ok(findings)
}

fn api_version_findings(snapshot: &Snapshot) -> anyhow::Result<Vec<AuditFinding>> {
    let mut findings = vec![];
    for resource in &snapshot.resources {
        let api_version = resource.api_version.to_ascii_lowercase();
        if api_version.contains("alpha")
            || api_version.contains("beta")
            || api_version.starts_with("extensions/")
        {
            findings.push(AuditFinding {
                rule_id: "api.preview-or-legacy-version".into(),
                severity: Severity::Low,
                title: "Preview or legacy API version detected".into(),
                object: Some(object_ref(resource)),
                evidence: vec![format!("apiVersion {}", resource.api_version)],
            });
        }
    }
    Ok(findings)
}

fn node_findings(snapshot: &Snapshot) -> anyhow::Result<Vec<AuditFinding>> {
    let mut findings = vec![];
    for node in node_health(snapshot)?
        .into_iter()
        .filter(|node| !node.ready)
    {
        findings.push(AuditFinding {
            rule_id: "node.not-ready".into(),
            severity: Severity::Medium,
            title: "Node is not ready".into(),
            object: Some(node.object),
            evidence: vec![node.reason.unwrap_or_else(|| "unknown reason".into())],
        });
    }
    Ok(findings)
}

fn object_for_surface(snapshot: &Snapshot, surface: &ExposureSurface) -> Option<ObjectRef> {
    let namespace = surface.namespace.as_deref().unwrap_or("_cluster");
    let id = match surface.namespace {
        Some(_) => format!("v1/{}/{namespace}/{}", surface.kind, surface.name),
        None => format!("v1/{}/_cluster/{}", surface.kind, surface.name),
    };
    snapshot.resource_by_id(&id).map(object_ref)
}

fn array_contains(value: Option<&Value>, needle: &str) -> bool {
    value
        .and_then(Value::as_array)
        .is_some_and(|values| values.iter().any(|value| value.as_str() == Some(needle)))
}

fn severity_key(severity: &Severity) -> &'static str {
    match severity {
        Severity::Critical => "critical",
        Severity::High => "high",
        Severity::Medium => "medium",
        Severity::Low => "low",
    }
}

#[cfg(test)]
mod tests {
    use crate::analysis::{snapshot::Snapshot, test_support::sample_snapshot};

    use super::{Severity, build, render_markdown};

    #[test]
    fn audit_finds_rbac_and_workload_risks() {
        let fixture = sample_snapshot("audit-report").expect("fixture");
        let snapshot = Snapshot::open(fixture.root()).expect("snapshot");
        let report = build(&snapshot).expect("report");

        assert!(
            report
                .findings
                .iter()
                .any(|finding| finding.rule_id == "rbac.cluster-admin-binding")
        );
        assert!(
            report
                .findings
                .iter()
                .any(|finding| finding.rule_id == "rbac.wildcard-rules")
        );
        assert!(report.findings.iter().any(|finding| {
            finding.rule_id == "workload.security-risk"
                && finding
                    .object
                    .as_ref()
                    .is_some_and(|object| object.name == "debug-tool")
        }));
        assert_eq!(
            report.findings.first().expect("finding").severity,
            Severity::Critical
        );
    }

    #[test]
    fn audit_markdown_mentions_findings() {
        let fixture = sample_snapshot("audit-markdown").expect("fixture");
        let snapshot = Snapshot::open(fixture.root()).expect("snapshot");
        let markdown = render_markdown(&build(&snapshot).expect("report"));

        assert!(markdown.contains("Cluster-admin binding"));
        assert!(markdown.contains("debug-tool"));
        assert!(markdown.contains("Node is not ready"));
    }
}
