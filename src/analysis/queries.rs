use std::collections::BTreeMap;

use anyhow::Context;
use serde::Serialize;
use serde_json::Value;

use super::snapshot::Snapshot;

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct ObjectRef {
    pub id: String,
    pub kind: String,
    pub namespace: Option<String>,
    pub name: String,
    pub path: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct PodHealth {
    pub object: ObjectRef,
    pub phase: Option<String>,
    pub reason: Option<String>,
    pub node_name: Option<String>,
    pub ready_containers: usize,
    pub total_containers: usize,
    pub restart_count: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct NodeHealth {
    pub object: ObjectRef,
    pub ready: bool,
    pub reason: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ServiceTarget {
    pub object: ObjectRef,
    pub service_type: String,
    pub selectors: BTreeMap<String, String>,
    pub matched_pods: Vec<ObjectRef>,
    pub ready_pods: Vec<ObjectRef>,
}

#[derive(Clone, Debug, Serialize)]
pub struct IngressTarget {
    pub object: ObjectRef,
    pub hosts: Vec<String>,
    pub service_names: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct GatewayRouteTarget {
    pub route_kind: String,
    pub route_id: String,
    pub route_api_version: String,
    pub namespace: Option<String>,
    pub name: String,
    pub gateway_refs: Vec<String>,
    pub service_names: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ExposureSurface {
    pub kind: String,
    pub namespace: Option<String>,
    pub name: String,
    pub exposure_type: String,
    pub detail: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct ContainerResourceGap {
    pub object: ObjectRef,
    pub container: String,
    pub issue: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct WorkloadRisk {
    pub object: ObjectRef,
    pub issue: String,
}

pub fn pod_health(snapshot: &Snapshot) -> anyhow::Result<Vec<PodHealth>> {
    let mut pods = vec![];
    for resource in snapshot
        .resources
        .iter()
        .filter(|resource| resource.kind == "Pod")
    {
        let value = snapshot.load_resource_value(resource)?;
        let phase = value
            .pointer("/status/phase")
            .and_then(Value::as_str)
            .map(ToString::to_string);
        let reason = value
            .pointer("/status/reason")
            .and_then(Value::as_str)
            .map(ToString::to_string)
            .or_else(|| first_waiting_reason(&value));
        let ready_containers = value
            .pointer("/status/containerStatuses")
            .and_then(Value::as_array)
            .map(|statuses| {
                statuses
                    .iter()
                    .filter(|status| status.get("ready").and_then(Value::as_bool) == Some(true))
                    .count()
            })
            .unwrap_or_default();
        let total_containers = value
            .pointer("/spec/containers")
            .and_then(Value::as_array)
            .map(Vec::len)
            .unwrap_or_default();
        let restart_count = value
            .pointer("/status/containerStatuses")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|status| status.get("restartCount").and_then(Value::as_u64))
            .sum();

        pods.push(PodHealth {
            object: object_ref(resource),
            phase,
            reason,
            node_name: value
                .pointer("/spec/nodeName")
                .and_then(Value::as_str)
                .map(ToString::to_string),
            ready_containers,
            total_containers,
            restart_count,
        });
    }

    pods.sort_by(|left, right| {
        right
            .restart_count
            .cmp(&left.restart_count)
            .then_with(|| left.object.name.cmp(&right.object.name))
    });
    Ok(pods)
}

pub fn node_health(snapshot: &Snapshot) -> anyhow::Result<Vec<NodeHealth>> {
    let mut nodes = vec![];
    for resource in snapshot
        .resources
        .iter()
        .filter(|resource| resource.kind == "Node")
    {
        let value = snapshot.load_resource_value(resource)?;
        let ready_condition = value
            .pointer("/status/conditions")
            .and_then(Value::as_array)
            .and_then(|conditions| {
                conditions.iter().find(|condition| {
                    condition.get("type").and_then(Value::as_str) == Some("Ready")
                })
            });
        let ready = ready_condition
            .and_then(|condition| condition.get("status").and_then(Value::as_str))
            == Some("True");
        let reason = ready_condition
            .and_then(|condition| condition.get("reason").and_then(Value::as_str))
            .map(ToString::to_string);

        nodes.push(NodeHealth {
            object: object_ref(resource),
            ready,
            reason,
        });
    }

    nodes.sort_by(|left, right| left.object.name.cmp(&right.object.name));
    Ok(nodes)
}

pub fn service_targets(snapshot: &Snapshot) -> anyhow::Result<Vec<ServiceTarget>> {
    let pods = snapshot
        .resources
        .iter()
        .filter(|resource| resource.kind == "Pod")
        .map(|resource| {
            Ok((
                object_ref(resource),
                resource.labels.clone(),
                pod_ready(snapshot, resource)?,
            ))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    let mut services = vec![];
    for resource in snapshot
        .resources
        .iter()
        .filter(|resource| resource.kind == "Service")
    {
        let value = snapshot.load_resource_value(resource)?;
        let selectors = value
            .pointer("/spec/selector")
            .and_then(Value::as_object)
            .map(object_string_map)
            .unwrap_or_default();
        let service_type = value
            .pointer("/spec/type")
            .and_then(Value::as_str)
            .unwrap_or("ClusterIP")
            .to_string();

        let matched_pods = pods
            .iter()
            .filter(|pod| labels_match(&selectors, &pod.1))
            .map(|pod| pod.0.clone())
            .collect::<Vec<_>>();
        let ready_pods = pods
            .iter()
            .filter(|pod| labels_match(&selectors, &pod.1) && pod.2)
            .map(|pod| pod.0.clone())
            .collect::<Vec<_>>();

        services.push(ServiceTarget {
            object: object_ref(resource),
            service_type,
            selectors,
            matched_pods,
            ready_pods,
        });
    }

    services.sort_by(|left, right| left.object.name.cmp(&right.object.name));
    Ok(services)
}

pub fn ingress_targets(snapshot: &Snapshot) -> anyhow::Result<Vec<IngressTarget>> {
    let mut ingresses = vec![];
    for resource in snapshot
        .resources
        .iter()
        .filter(|resource| resource.kind == "Ingress")
    {
        let value = snapshot.load_resource_value(resource)?;
        let hosts = value
            .pointer("/spec/rules")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|rule| rule.get("host").and_then(Value::as_str))
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        let mut services = value
            .pointer("/spec/rules")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .flat_map(|rule| {
                rule.pointer("/http/paths")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(|path| {
                        path.pointer("/backend/service/name")
                            .and_then(Value::as_str)
                            .map(ToString::to_string)
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        if let Some(default_backend) = value
            .pointer("/spec/defaultBackend/service/name")
            .and_then(Value::as_str)
        {
            services.push(default_backend.to_string());
        }
        services.sort();
        services.dedup();

        ingresses.push(IngressTarget {
            object: object_ref(resource),
            hosts,
            service_names: services,
        });
    }

    ingresses.sort_by(|left, right| left.object.name.cmp(&right.object.name));
    Ok(ingresses)
}

pub fn gateway_route_targets(snapshot: &Snapshot) -> anyhow::Result<Vec<GatewayRouteTarget>> {
    let mut routes = vec![];
    for resource in snapshot.resources.iter().filter(|resource| {
        matches!(
            resource.kind.as_str(),
            "HTTPRoute" | "TLSRoute" | "TCPRoute" | "UDPRoute" | "GRPCRoute"
        )
    }) {
        let value = snapshot.load_resource_value(resource)?;
        let gateway_refs = value
            .pointer("/spec/parentRefs")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|parent| {
                let name = parent.get("name").and_then(Value::as_str)?;
                let namespace = parent
                    .get("namespace")
                    .and_then(Value::as_str)
                    .map(ToString::to_string)
                    .or_else(|| resource.namespace.clone());
                Some(match namespace {
                    Some(namespace) => format!("{namespace}/{name}"),
                    None => name.to_string(),
                })
            })
            .collect::<Vec<_>>();
        let service_names = route_backend_service_names(&value);

        routes.push(GatewayRouteTarget {
            route_kind: resource.kind.clone(),
            route_id: resource.id.clone(),
            route_api_version: resource.api_version.clone(),
            namespace: resource.namespace.clone(),
            name: resource.name.clone(),
            gateway_refs,
            service_names,
        });
    }

    routes.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(routes)
}

pub fn exposure_surfaces(snapshot: &Snapshot) -> anyhow::Result<Vec<ExposureSurface>> {
    let mut exposures = vec![];

    for service in service_targets(snapshot)? {
        match service.service_type.as_str() {
            "LoadBalancer" | "NodePort" => exposures.push(ExposureSurface {
                kind: service.object.kind.clone(),
                namespace: service.object.namespace.clone(),
                name: service.object.name.clone(),
                exposure_type: service.service_type.clone(),
                detail: format!("service type {}", service.service_type),
            }),
            _ => {}
        }
    }

    for ingress in ingress_targets(snapshot)? {
        exposures.push(ExposureSurface {
            kind: ingress.object.kind.clone(),
            namespace: ingress.object.namespace.clone(),
            name: ingress.object.name.clone(),
            exposure_type: "Ingress".into(),
            detail: if ingress.hosts.is_empty() {
                "ingress routes".into()
            } else {
                format!("hosts {}", ingress.hosts.join(", "))
            },
        });
    }

    for resource in snapshot
        .resources
        .iter()
        .filter(|resource| resource.kind == "Gateway")
    {
        let value = snapshot.load_resource_value(resource)?;
        let listeners = value
            .pointer("/spec/listeners")
            .and_then(Value::as_array)
            .map(|listeners| listeners.len())
            .unwrap_or_default();
        exposures.push(ExposureSurface {
            kind: resource.kind.clone(),
            namespace: resource.namespace.clone(),
            name: resource.name.clone(),
            exposure_type: "Gateway".into(),
            detail: format!("{listeners} listener(s)"),
        });
    }

    exposures.sort_by(|left, right| {
        left.namespace
            .cmp(&right.namespace)
            .then_with(|| left.name.cmp(&right.name))
    });
    Ok(exposures)
}

pub fn missing_resource_gaps(snapshot: &Snapshot) -> anyhow::Result<Vec<ContainerResourceGap>> {
    let mut gaps = vec![];
    for resource in snapshot
        .resources
        .iter()
        .filter(|resource| resource.kind == "Pod")
    {
        let value = snapshot.load_resource_value(resource)?;
        for (container_type, container) in pod_containers(&value) {
            let name = container
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or("<unknown>")
                .to_string();
            let requests = container.pointer("/resources/requests");
            let limits = container.pointer("/resources/limits");
            if requests.is_none() {
                gaps.push(ContainerResourceGap {
                    object: object_ref(resource),
                    container: name.clone(),
                    issue: format!("{container_type} missing resource requests"),
                });
            }
            if limits.is_none() {
                gaps.push(ContainerResourceGap {
                    object: object_ref(resource),
                    container: name,
                    issue: format!("{container_type} missing resource limits"),
                });
            }
        }
    }

    Ok(gaps)
}

pub fn workload_risks(snapshot: &Snapshot) -> anyhow::Result<Vec<WorkloadRisk>> {
    let mut risks = vec![];
    for resource in snapshot
        .resources
        .iter()
        .filter(|resource| resource.kind == "Pod")
    {
        let value = snapshot.load_resource_value(resource)?;
        if value.pointer("/spec/hostNetwork").and_then(Value::as_bool) == Some(true) {
            risks.push(WorkloadRisk {
                object: object_ref(resource),
                issue: "hostNetwork enabled".into(),
            });
        }
        if value.pointer("/spec/hostPID").and_then(Value::as_bool) == Some(true) {
            risks.push(WorkloadRisk {
                object: object_ref(resource),
                issue: "hostPID enabled".into(),
            });
        }
        if value
            .pointer("/spec/volumes")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .any(|volume| volume.get("hostPath").is_some())
        {
            risks.push(WorkloadRisk {
                object: object_ref(resource),
                issue: "hostPath mount present".into(),
            });
        }
        for (container_type, container) in pod_containers(&value) {
            let privileged = container
                .pointer("/securityContext/privileged")
                .and_then(Value::as_bool)
                == Some(true);
            let allow_escalation = container
                .pointer("/securityContext/allowPrivilegeEscalation")
                .and_then(Value::as_bool)
                == Some(true);
            let run_as_root = container
                .pointer("/securityContext/runAsUser")
                .and_then(Value::as_u64)
                == Some(0);
            let container_name = container
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or("<unknown>");
            if privileged {
                risks.push(WorkloadRisk {
                    object: object_ref(resource),
                    issue: format!("{container_type} {container_name} is privileged"),
                });
            }
            if allow_escalation {
                risks.push(WorkloadRisk {
                    object: object_ref(resource),
                    issue: format!("{container_type} {container_name} allows privilege escalation"),
                });
            }
            if run_as_root {
                risks.push(WorkloadRisk {
                    object: object_ref(resource),
                    issue: format!("{container_type} {container_name} runs as uid 0"),
                });
            }
        }
    }

    Ok(risks)
}

pub fn object_ref(resource: &crate::gather::agent_artifacts::ResourceIndexEntry) -> ObjectRef {
    ObjectRef {
        id: resource.id.clone(),
        kind: resource.kind.clone(),
        namespace: resource.namespace.clone(),
        name: resource.name.clone(),
        path: resource.path.clone(),
    }
}

fn first_waiting_reason(value: &Value) -> Option<String> {
    value
        .pointer("/status/containerStatuses")
        .and_then(Value::as_array)
        .and_then(|statuses| {
            statuses.iter().find_map(|status| {
                status
                    .pointer("/state/waiting/reason")
                    .and_then(Value::as_str)
                    .map(ToString::to_string)
            })
        })
}

fn route_backend_service_names(value: &Value) -> Vec<String> {
    let mut services = vec![];
    for section in ["/spec/rules", "/spec/defaults"] {
        for rule in value
            .pointer(section)
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            collect_route_backend_refs(rule, &mut services);
        }
    }
    services.sort();
    services.dedup();
    services
}

fn collect_route_backend_refs(value: &Value, services: &mut Vec<String>) {
    if let Some(backends) = value.get("backendRefs").and_then(Value::as_array) {
        services.extend(
            backends
                .iter()
                .filter(|backend| {
                    backend
                        .get("kind")
                        .and_then(Value::as_str)
                        .unwrap_or("Service")
                        == "Service"
                })
                .filter_map(|backend| backend.get("name").and_then(Value::as_str))
                .map(ToString::to_string),
        );
    }
    if let Some(rules) = value.get("rules").and_then(Value::as_array) {
        for rule in rules {
            collect_route_backend_refs(rule, services);
        }
    }
}

fn labels_match(selectors: &BTreeMap<String, String>, labels: &BTreeMap<String, String>) -> bool {
    !selectors.is_empty()
        && selectors
            .iter()
            .all(|(key, value)| labels.get(key) == Some(value))
}

fn pod_ready(
    snapshot: &Snapshot,
    resource: &crate::gather::agent_artifacts::ResourceIndexEntry,
) -> anyhow::Result<bool> {
    let value = snapshot.load_resource_value(resource)?;
    Ok(value
        .pointer("/status/conditions")
        .and_then(Value::as_array)
        .and_then(|conditions| {
            conditions
                .iter()
                .find(|condition| condition.get("type").and_then(Value::as_str) == Some("Ready"))
        })
        .and_then(|condition| condition.get("status").and_then(Value::as_str))
        == Some("True"))
}

fn pod_containers<'a>(value: &'a Value) -> Vec<(&'static str, &'a Value)> {
    let mut containers = vec![];
    for (container_type, path) in [
        ("container", "/spec/containers"),
        ("init container", "/spec/initContainers"),
        ("ephemeral container", "/spec/ephemeralContainers"),
    ] {
        containers.extend(
            value
                .pointer(path)
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .map(|container| (container_type, container)),
        );
    }
    containers
}

fn object_string_map(value: &serde_json::Map<String, Value>) -> BTreeMap<String, String> {
    value
        .iter()
        .filter_map(|(key, value)| value.as_str().map(|value| (key.clone(), value.to_string())))
        .collect()
}

pub fn yaml_digest(snapshot: &Snapshot, path: &str) -> anyhow::Result<String> {
    let value = snapshot
        .load_resource_value_by_path(path)
        .with_context(|| format!("failed to load {path} for digest"))?;
    let normalized = normalize_value(value);
    Ok(serde_json::to_string(&normalized)?)
}

fn normalize_value(mut value: Value) -> Value {
    if let Some(metadata) = value.get_mut("metadata").and_then(Value::as_object_mut) {
        metadata.remove("resourceVersion");
        metadata.remove("managedFields");
        metadata.remove("uid");
        metadata.remove("generation");
        if let Some(annotations) = metadata
            .get_mut("annotations")
            .and_then(Value::as_object_mut)
        {
            annotations.remove("crust-gather.io/added");
            annotations.remove("crust-gather.io/updated");
            annotations.remove("crust-gather.io/deleted");
            if annotations.is_empty() {
                metadata.remove("annotations");
            }
        }
    }
    value
}

#[cfg(test)]
mod tests {
    use crate::analysis::{snapshot::Snapshot, test_support::sample_snapshot};

    use super::{
        exposure_surfaces, ingress_targets, missing_resource_gaps, node_health, pod_health,
        service_targets, workload_risks, yaml_digest,
    };

    #[test]
    fn pod_and_node_queries_extract_health_signals() {
        let fixture = sample_snapshot("queries-health").expect("fixture");
        let snapshot = Snapshot::open(fixture.root()).expect("snapshot");

        let pods = pod_health(&snapshot).expect("pods");
        let nodes = node_health(&snapshot).expect("nodes");

        assert_eq!(pods.first().expect("pod").object.name, "web-abc");
        assert_eq!(pods.first().expect("pod").restart_count, 7);
        assert!(
            !nodes
                .iter()
                .find(|node| node.object.name == "control-plane")
                .expect("node")
                .ready
        );
    }

    #[test]
    fn topology_queries_match_services_and_ingresses() {
        let fixture = sample_snapshot("queries-topology").expect("fixture");
        let snapshot = Snapshot::open(fixture.root()).expect("snapshot");

        let services = service_targets(&snapshot).expect("services");
        let ingresses = ingress_targets(&snapshot).expect("ingresses");

        assert_eq!(
            services
                .iter()
                .find(|service| service.object.name == "web")
                .expect("web")
                .matched_pods
                .len(),
            1
        );
        assert!(
            services
                .iter()
                .find(|service| service.object.name == "orphan")
                .expect("orphan")
                .matched_pods
                .is_empty()
        );
        assert_eq!(
            ingresses.first().expect("ingress").service_names,
            vec!["web".to_string()]
        );
    }

    #[test]
    fn audit_queries_extract_exposure_and_risks() {
        let fixture = sample_snapshot("queries-audit").expect("fixture");
        let snapshot = Snapshot::open(fixture.root()).expect("snapshot");

        let exposures = exposure_surfaces(&snapshot).expect("exposures");
        let gaps = missing_resource_gaps(&snapshot).expect("gaps");
        let risks = workload_risks(&snapshot).expect("risks");

        assert!(
            exposures
                .iter()
                .any(|surface| surface.name == "web" && surface.exposure_type == "LoadBalancer")
        );
        assert!(gaps.iter().any(|gap| gap.object.name == "debug-tool"));
        assert!(risks.iter().any(|risk| risk.object.name == "debug-tool"));
    }

    #[test]
    fn yaml_digest_ignores_volatile_metadata() {
        let fixture = sample_snapshot("queries-digest").expect("fixture");
        let snapshot = Snapshot::open(fixture.root()).expect("snapshot");
        let digest =
            yaml_digest(&snapshot, "namespaces/default/v1/pod/web-abc.yaml").expect("digest");

        assert!(!digest.contains("resourceVersion"));
        assert!(!digest.contains("managedFields"));
    }
}
