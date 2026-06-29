use std::collections::{BTreeMap, BTreeSet};

use clap::{Args, ValueEnum};
use serde::Serialize;

use super::{
    cli::{SnapshotInput, emit_json, emit_text},
    queries::{
        GatewayRouteTarget, IngressTarget, ServiceTarget, gateway_route_targets, ingress_targets,
        service_targets,
    },
    snapshot::Snapshot,
};

#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, ValueEnum)]
pub enum GraphFormat {
    Markdown,
    Json,
    #[default]
    Mermaid,
}

#[derive(Clone, Debug, Args)]
pub struct GraphOutputOptions {
    #[arg(long, default_value_t, value_enum)]
    pub format: GraphFormat,

    #[arg(long, value_name = "PATH")]
    pub output: Option<std::path::PathBuf>,
}

#[derive(Clone, Debug, Serialize)]
pub struct GraphNode {
    pub id: String,
    pub label: String,
    pub kind: String,
    pub namespace: Option<String>,
    pub name: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct GraphEdge {
    pub from: String,
    pub to: String,
    pub relation: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct GraphReport {
    pub namespace: Option<String>,
    pub nodes: Vec<GraphNode>,
    pub edges: Vec<GraphEdge>,
}

#[derive(Clone, Debug, Args)]
pub struct GraphCommand {
    #[command(flatten)]
    pub input: SnapshotInput,

    #[command(flatten)]
    pub output: GraphOutputOptions,

    #[arg(long)]
    pub namespace: Option<String>,
}

pub fn build(snapshot: &Snapshot, namespace: Option<&str>) -> anyhow::Result<GraphReport> {
    let mut nodes = BTreeMap::<String, GraphNode>::new();
    let mut edges = BTreeSet::<(String, String, String)>::new();
    let namespace = namespace.map(ToString::to_string);

    for relation in snapshot
        .relations
        .iter()
        .filter(|relation| relation.relation == "owned-by")
    {
        let Some(source) = snapshot.resource_by_id(&relation.source_id) else {
            continue;
        };
        let Some(target) = snapshot.resource_by_id(&relation.target_id) else {
            continue;
        };
        if !is_topology_kind(source.kind.as_str()) || !is_topology_kind(target.kind.as_str()) {
            continue;
        }
        if !allow_pair(
            namespace.as_deref(),
            source.namespace.as_deref(),
            target.namespace.as_deref(),
        ) {
            continue;
        }
        insert_resource(&mut nodes, target);
        insert_resource(&mut nodes, source);
        edges.insert((target.id.clone(), source.id.clone(), "owns".into()));
    }

    for service in service_targets(snapshot)? {
        if !allow_namespace(namespace.as_deref(), service.object.namespace.as_deref()) {
            continue;
        }
        add_service_edges(snapshot, &mut nodes, &mut edges, &service);
    }

    for ingress in ingress_targets(snapshot)? {
        if !allow_namespace(namespace.as_deref(), ingress.object.namespace.as_deref()) {
            continue;
        }
        add_ingress_edges(snapshot, &mut nodes, &mut edges, &ingress);
    }

    for route in gateway_route_targets(snapshot)? {
        if !allow_namespace(namespace.as_deref(), route.namespace.as_deref()) {
            continue;
        }
        add_gateway_route_edges(snapshot, &mut nodes, &mut edges, &route);
    }

    for relation in snapshot
        .relations
        .iter()
        .filter(|relation| relation.relation == "scheduled-on")
    {
        let Some(source) = snapshot.resource_by_id(&relation.source_id) else {
            continue;
        };
        let Some(target) = snapshot.resource_by_id(&relation.target_id) else {
            continue;
        };
        if !allow_pair(
            namespace.as_deref(),
            source.namespace.as_deref(),
            target.namespace.as_deref(),
        ) {
            continue;
        }
        insert_resource(&mut nodes, source);
        insert_resource(&mut nodes, target);
        edges.insert((source.id.clone(), target.id.clone(), "scheduled-on".into()));
    }

    Ok(GraphReport {
        namespace,
        nodes: nodes.into_values().collect(),
        edges: edges
            .into_iter()
            .map(|(from, to, relation)| GraphEdge { from, to, relation })
            .collect(),
    })
}

pub fn render_mermaid(report: &GraphReport) -> String {
    let mut out = String::from("graph TD\n");
    for node in &report.nodes {
        out.push_str(&format!(
            "    {}[\"{}\"]\n",
            mermaid_id(node.id.as_str()),
            escape_label(node.label.as_str())
        ));
    }
    for edge in &report.edges {
        out.push_str(&format!(
            "    {} -->|{}| {}\n",
            mermaid_id(edge.from.as_str()),
            escape_label(edge.relation.as_str()),
            mermaid_id(edge.to.as_str())
        ));
    }
    out
}

pub fn render_markdown(report: &GraphReport) -> String {
    format!(
        "# Snapshot Graph\n\n- namespace: {}\n- nodes: {}\n- edges: {}\n\n```mermaid\n{}\n```\n",
        report.namespace.as_deref().unwrap_or("<all>"),
        report.nodes.len(),
        report.edges.len(),
        render_mermaid(report)
    )
}

pub fn run(command: GraphCommand) -> anyhow::Result<()> {
    let snapshot = Snapshot::open(command.input.snapshot)?;
    let report = build(&snapshot, command.namespace.as_deref())?;

    match command.output.format {
        GraphFormat::Markdown => emit_text(
            command.output.output.as_ref(),
            render_markdown(&report).as_str(),
        ),
        GraphFormat::Json => emit_json(command.output.output.as_ref(), &report),
        GraphFormat::Mermaid => emit_text(
            command.output.output.as_ref(),
            render_mermaid(&report).as_str(),
        ),
    }
}

fn add_service_edges(
    snapshot: &Snapshot,
    nodes: &mut BTreeMap<String, GraphNode>,
    edges: &mut BTreeSet<(String, String, String)>,
    service: &ServiceTarget,
) {
    let Some(service_resource) = snapshot.resource_by_id(&service.object.id) else {
        return;
    };
    insert_resource(nodes, service_resource);
    for pod in &service.matched_pods {
        if let Some(pod_resource) = snapshot.resource_by_id(&pod.id) {
            insert_resource(nodes, pod_resource);
            edges.insert((service.object.id.clone(), pod.id.clone(), "selects".into()));
        }
    }
}

fn add_ingress_edges(
    snapshot: &Snapshot,
    nodes: &mut BTreeMap<String, GraphNode>,
    edges: &mut BTreeSet<(String, String, String)>,
    ingress: &IngressTarget,
) {
    let Some(ingress_resource) = snapshot.resource_by_id(&ingress.object.id) else {
        return;
    };
    insert_resource(nodes, ingress_resource);
    let namespace = ingress.object.namespace.as_deref().unwrap_or("_cluster");
    for service_name in &ingress.service_names {
        let id = format!("v1/Service/{namespace}/{service_name}");
        if let Some(service_resource) = snapshot.resource_by_id(&id) {
            insert_resource(nodes, service_resource);
            edges.insert((ingress.object.id.clone(), id, "routes-to".into()));
        }
    }
}

fn add_gateway_route_edges(
    snapshot: &Snapshot,
    nodes: &mut BTreeMap<String, GraphNode>,
    edges: &mut BTreeSet<(String, String, String)>,
    route: &GatewayRouteTarget,
) {
    let namespace = route.namespace.as_deref().unwrap_or("_cluster");
    if let Some(route_resource) = snapshot.resource_by_id(&route.route_id) {
        insert_resource(nodes, route_resource);
        for gateway_ref in &route.gateway_refs {
            let (gateway_ns, gateway_name) = gateway_ref
                .split_once('/')
                .map(|(ns, name)| (Some(ns), name))
                .unwrap_or((route.namespace.as_deref(), gateway_ref.as_str()));
            if let Some(gateway_resource) =
                find_resource(snapshot, "Gateway", gateway_ns, gateway_name)
            {
                insert_resource(nodes, gateway_resource);
                edges.insert((
                    gateway_resource.id.clone(),
                    route.route_id.clone(),
                    "accepts".into(),
                ));
            }
        }
        for service_name in &route.service_names {
            let service_id = format!("v1/Service/{namespace}/{service_name}");
            if let Some(service_resource) = snapshot.resource_by_id(&service_id) {
                insert_resource(nodes, service_resource);
                edges.insert((route.route_id.clone(), service_id, "routes-to".into()));
            }
        }
    }
}

fn insert_resource(
    nodes: &mut BTreeMap<String, GraphNode>,
    resource: &crate::gather::agent_artifacts::ResourceIndexEntry,
) {
    nodes
        .entry(resource.id.clone())
        .or_insert_with(|| GraphNode {
            id: resource.id.clone(),
            label: format!(
                "{} {}",
                resource.kind,
                qualified_name(resource.namespace.as_deref(), resource.name.as_str())
            ),
            kind: resource.kind.clone(),
            namespace: resource.namespace.clone(),
            name: resource.name.clone(),
        });
}

fn allow_namespace(filter: Option<&str>, namespace: Option<&str>) -> bool {
    match filter {
        None => true,
        Some(filter) => namespace == Some(filter),
    }
}

fn allow_pair(
    filter: Option<&str>,
    source_namespace: Option<&str>,
    target_namespace: Option<&str>,
) -> bool {
    allow_namespace(filter, source_namespace) || allow_namespace(filter, target_namespace)
}

fn mermaid_id(id: &str) -> String {
    id.chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect()
}

fn escape_label(label: &str) -> String {
    label.replace('"', "\\\"")
}

fn qualified_name(namespace: Option<&str>, name: &str) -> String {
    match namespace {
        Some(namespace) => format!("{namespace}/{name}"),
        None => name.to_string(),
    }
}

fn is_topology_kind(kind: &str) -> bool {
    matches!(
        kind,
        "Pod"
            | "Deployment"
            | "ReplicaSet"
            | "StatefulSet"
            | "DaemonSet"
            | "Job"
            | "CronJob"
            | "Service"
            | "Ingress"
            | "Gateway"
            | "HTTPRoute"
            | "TLSRoute"
            | "TCPRoute"
            | "UDPRoute"
            | "GRPCRoute"
            | "Node"
    )
}

fn find_resource<'a>(
    snapshot: &'a Snapshot,
    kind: &str,
    namespace: Option<&str>,
    name: &str,
) -> Option<&'a crate::gather::agent_artifacts::ResourceIndexEntry> {
    snapshot.resources.iter().find(|resource| {
        resource.kind == kind && resource.namespace.as_deref() == namespace && resource.name == name
    })
}

#[cfg(test)]
mod tests {
    use crate::analysis::{snapshot::Snapshot, test_support::sample_snapshot};

    use super::{build, render_mermaid};

    #[test]
    fn graph_builds_expected_edges() {
        let fixture = sample_snapshot("graph-report").expect("fixture");
        let snapshot = Snapshot::open(fixture.root()).expect("snapshot");
        let report = build(&snapshot, Some("default")).expect("report");

        assert!(
            report
                .edges
                .iter()
                .any(|edge| edge.relation == "owns" && edge.to == "v1/Pod/default/web-abc")
        );
        assert!(
            report
                .edges
                .iter()
                .any(|edge| edge.relation == "selects" && edge.from == "v1/Service/default/web")
        );
        assert!(
            report.edges.iter().any(
                |edge| edge.relation == "scheduled-on" && edge.to == "v1/Node/_cluster/worker1"
            )
        );
    }

    #[test]
    fn graph_mermaid_output_is_stable() {
        let fixture = sample_snapshot("graph-mermaid").expect("fixture");
        let snapshot = Snapshot::open(fixture.root()).expect("snapshot");
        let mermaid = render_mermaid(&build(&snapshot, Some("default")).expect("report"));

        assert!(mermaid.contains("graph TD"));
        assert!(mermaid.contains("routes-to"));
        assert!(mermaid.contains("selects"));
    }
}
