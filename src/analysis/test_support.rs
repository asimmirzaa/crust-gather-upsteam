use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use chrono::Utc;
use serde::Serialize;
use tempfile::TempDir;

use crate::gather::{
    agent_artifacts::{
        LogIndexEntry, OwnerRefEntry, RelationIndexEntry, ResourceIndexEntry, ResourceReference,
    },
    analysis_schema::AnalysisSchema,
    report::{CollectorStats, InputLog, RunIdentity, RunInputs, RunMessage, RunReport, RunTotals},
};

pub struct SampleSnapshot {
    _tempdir: TempDir,
    root: PathBuf,
}

impl SampleSnapshot {
    pub fn root(&self) -> &Path {
        &self.root
    }
}

#[derive(Serialize)]
struct AppVersionEntry {
    name: String,
    namespace: String,
    container: String,
    container_type: String,
    version: String,
}

pub fn sample_snapshot(name: &str) -> anyhow::Result<SampleSnapshot> {
    let tempdir = TempDir::new()?;
    let root = tempdir.path().join(name).join("snapshot");
    fs::create_dir_all(&root)?;

    let started_at = Utc::now();
    let finished_at = started_at + chrono::Duration::seconds(42);
    let report = RunReport {
        identity: RunIdentity {
            collector_name: "crust-gather".into(),
            collector_version: "1.0.1".into(),
            collector_revision: "test-revision".into(),
        },
        inputs: RunInputs {
            mode: "collect".into(),
            source: "test".into(),
            context: Some("unit-test".into()),
            output_path: "/tmp/snapshot".into(),
            output_encoding: "path".into(),
            oci_reference: None,
            clean_output: true,
            duration: "5m".into(),
            list_page_limit: 100,
            collect_concurrency: 32,
            log_collect_concurrency: 8,
            node_log_mode: "deep".into(),
            debug_pod_image: None,
            systemd_units: vec!["kubelet".into()],
            additional_logs: Vec::<InputLog>::new(),
            filters: vec![],
            secret_env_names: vec![],
            secrets_file: None,
        },
        started_at,
        finished_at: Some(finished_at),
        duration_ms: Some(42_000),
        success: true,
        totals: RunTotals {
            collectors: 10,
            listed_objects: 10,
            collected_objects: 10,
            written_files: 12,
            failed_objects: 0,
            warnings: 1,
        },
        stats: BTreeMap::from([
            (
                "node-kubelet-logs".into(),
                CollectorStats {
                    listed_objects: 2,
                    collected_objects: 2,
                    written_files: 2,
                    failed_objects: 0,
                    warnings: 0,
                },
            ),
            (
                "v1/Pod/current-logs".into(),
                CollectorStats {
                    listed_objects: 2,
                    collected_objects: 2,
                    written_files: 2,
                    failed_objects: 0,
                    warnings: 1,
                },
            ),
        ]),
        warnings: vec![RunMessage {
            timestamp: finished_at,
            phase: "logs".into(),
            collector: "v1/Pod/current-logs".into(),
            object: Some("default/web-abc:app".into()),
            message: "No previous logs found".into(),
        }],
        failures: vec![],
    };

    let resources = vec![
        ResourceIndexEntry {
            id: "v1/Node/_cluster/control-plane".into(),
            api_version: "v1".into(),
            kind: "Node".into(),
            namespace: None,
            name: "control-plane".into(),
            path: "cluster/v1/node/control-plane.yaml".into(),
            uid: Some("node-control-plane".into()),
            resource_version: Some("1".into()),
            created_at: Some(started_at.to_rfc3339()),
            phase: Some("Ready".into()),
            node_name: None,
            service_account: None,
            labels: BTreeMap::from([(
                "node-role.kubernetes.io/control-plane".into(),
                "true".into(),
            )]),
            annotation_keys: vec![],
            owner_refs: vec![],
            containers: vec![],
            references: vec![],
        },
        ResourceIndexEntry {
            id: "v1/Node/_cluster/worker1".into(),
            api_version: "v1".into(),
            kind: "Node".into(),
            namespace: None,
            name: "worker1".into(),
            path: "cluster/v1/node/worker1.yaml".into(),
            uid: Some("node-worker1".into()),
            resource_version: Some("1".into()),
            created_at: Some(started_at.to_rfc3339()),
            phase: Some("Ready".into()),
            node_name: None,
            service_account: None,
            labels: BTreeMap::from([("kubernetes.io/os".into(), "linux".into())]),
            annotation_keys: vec![],
            owner_refs: vec![],
            containers: vec![],
            references: vec![],
        },
        ResourceIndexEntry {
            id: "apps/v1/Deployment/default/web".into(),
            api_version: "apps/v1".into(),
            kind: "Deployment".into(),
            namespace: Some("default".into()),
            name: "web".into(),
            path: "namespaces/default/apps-v1/deployment/web.yaml".into(),
            uid: Some("deploy-web".into()),
            resource_version: Some("1".into()),
            created_at: Some(started_at.to_rfc3339()),
            phase: None,
            node_name: None,
            service_account: None,
            labels: BTreeMap::from([("app".into(), "web".into())]),
            annotation_keys: vec![],
            owner_refs: vec![],
            containers: vec![],
            references: vec![],
        },
        ResourceIndexEntry {
            id: "apps/v1/ReplicaSet/default/web-rs".into(),
            api_version: "apps/v1".into(),
            kind: "ReplicaSet".into(),
            namespace: Some("default".into()),
            name: "web-rs".into(),
            path: "namespaces/default/apps-v1/replicaset/web-rs.yaml".into(),
            uid: Some("rs-web".into()),
            resource_version: Some("1".into()),
            created_at: Some(started_at.to_rfc3339()),
            phase: None,
            node_name: None,
            service_account: None,
            labels: BTreeMap::from([("app".into(), "web".into())]),
            annotation_keys: vec![],
            owner_refs: vec![OwnerRefEntry {
                api_version: "apps/v1".into(),
                kind: "Deployment".into(),
                name: "web".into(),
                uid: Some("deploy-web".into()),
            }],
            containers: vec![],
            references: vec![],
        },
        ResourceIndexEntry {
            id: "v1/Pod/default/web-abc".into(),
            api_version: "v1".into(),
            kind: "Pod".into(),
            namespace: Some("default".into()),
            name: "web-abc".into(),
            path: "namespaces/default/v1/pod/web-abc.yaml".into(),
            uid: Some("pod-web".into()),
            resource_version: Some("1".into()),
            created_at: Some(started_at.to_rfc3339()),
            phase: Some("Running".into()),
            node_name: Some("worker1".into()),
            service_account: Some("default".into()),
            labels: BTreeMap::from([("app".into(), "web".into())]),
            annotation_keys: vec![],
            owner_refs: vec![OwnerRefEntry {
                api_version: "apps/v1".into(),
                kind: "ReplicaSet".into(),
                name: "web-rs".into(),
                uid: Some("rs-web".into()),
            }],
            containers: vec![],
            references: vec![
                ResourceReference {
                    relation: "uses-service-account".into(),
                    target_kind: "ServiceAccount".into(),
                    target_namespace: Some("default".into()),
                    target_name: "default".into(),
                },
                ResourceReference {
                    relation: "scheduled-on".into(),
                    target_kind: "Node".into(),
                    target_namespace: None,
                    target_name: "worker1".into(),
                },
            ],
        },
        ResourceIndexEntry {
            id: "v1/Pod/default/debug-tool".into(),
            api_version: "v1".into(),
            kind: "Pod".into(),
            namespace: Some("default".into()),
            name: "debug-tool".into(),
            path: "namespaces/default/v1/pod/debug-tool.yaml".into(),
            uid: Some("pod-debug".into()),
            resource_version: Some("1".into()),
            created_at: Some(started_at.to_rfc3339()),
            phase: Some("Running".into()),
            node_name: Some("control-plane".into()),
            service_account: Some("default".into()),
            labels: BTreeMap::from([("app".into(), "debug".into())]),
            annotation_keys: vec![],
            owner_refs: vec![],
            containers: vec![],
            references: vec![ResourceReference {
                relation: "scheduled-on".into(),
                target_kind: "Node".into(),
                target_namespace: None,
                target_name: "control-plane".into(),
            }],
        },
        ResourceIndexEntry {
            id: "v1/Service/default/web".into(),
            api_version: "v1".into(),
            kind: "Service".into(),
            namespace: Some("default".into()),
            name: "web".into(),
            path: "namespaces/default/v1/service/web.yaml".into(),
            uid: Some("svc-web".into()),
            resource_version: Some("1".into()),
            created_at: Some(started_at.to_rfc3339()),
            phase: None,
            node_name: None,
            service_account: None,
            labels: BTreeMap::new(),
            annotation_keys: vec![],
            owner_refs: vec![],
            containers: vec![],
            references: vec![],
        },
        ResourceIndexEntry {
            id: "v1/Service/default/orphan".into(),
            api_version: "v1".into(),
            kind: "Service".into(),
            namespace: Some("default".into()),
            name: "orphan".into(),
            path: "namespaces/default/v1/service/orphan.yaml".into(),
            uid: Some("svc-orphan".into()),
            resource_version: Some("1".into()),
            created_at: Some(started_at.to_rfc3339()),
            phase: None,
            node_name: None,
            service_account: None,
            labels: BTreeMap::new(),
            annotation_keys: vec![],
            owner_refs: vec![],
            containers: vec![],
            references: vec![],
        },
        ResourceIndexEntry {
            id: "networking.k8s.io/v1/Ingress/default/web".into(),
            api_version: "networking.k8s.io/v1".into(),
            kind: "Ingress".into(),
            namespace: Some("default".into()),
            name: "web".into(),
            path: "namespaces/default/networking.k8s.io-v1/ingress/web.yaml".into(),
            uid: Some("ing-web".into()),
            resource_version: Some("1".into()),
            created_at: Some(started_at.to_rfc3339()),
            phase: None,
            node_name: None,
            service_account: None,
            labels: BTreeMap::new(),
            annotation_keys: vec![],
            owner_refs: vec![],
            containers: vec![],
            references: vec![],
        },
        ResourceIndexEntry {
            id: "rbac.authorization.k8s.io/v1/ClusterRoleBinding/_cluster/admin-binding".into(),
            api_version: "rbac.authorization.k8s.io/v1".into(),
            kind: "ClusterRoleBinding".into(),
            namespace: None,
            name: "admin-binding".into(),
            path: "cluster/rbac.authorization.k8s.io-v1/clusterrolebinding/admin-binding.yaml"
                .into(),
            uid: Some("crb-admin".into()),
            resource_version: Some("1".into()),
            created_at: Some(started_at.to_rfc3339()),
            phase: None,
            node_name: None,
            service_account: None,
            labels: BTreeMap::new(),
            annotation_keys: vec![],
            owner_refs: vec![],
            containers: vec![],
            references: vec![],
        },
        ResourceIndexEntry {
            id: "rbac.authorization.k8s.io/v1/ClusterRole/_cluster/wild-reader".into(),
            api_version: "rbac.authorization.k8s.io/v1".into(),
            kind: "ClusterRole".into(),
            namespace: None,
            name: "wild-reader".into(),
            path: "cluster/rbac.authorization.k8s.io-v1/clusterrole/wild-reader.yaml".into(),
            uid: Some("cr-wild".into()),
            resource_version: Some("1".into()),
            created_at: Some(started_at.to_rfc3339()),
            phase: None,
            node_name: None,
            service_account: None,
            labels: BTreeMap::new(),
            annotation_keys: vec![],
            owner_refs: vec![],
            containers: vec![],
            references: vec![],
        },
    ];

    let relations = vec![
        RelationIndexEntry {
            source_id: "apps/v1/ReplicaSet/default/web-rs".into(),
            source_path: "namespaces/default/apps-v1/replicaset/web-rs.yaml".into(),
            relation: "owned-by".into(),
            target_id: "apps/v1/Deployment/default/web".into(),
            target_kind: "Deployment".into(),
            target_namespace: Some("default".into()),
            target_name: "web".into(),
            target_path: Some("namespaces/default/apps-v1/deployment/web.yaml".into()),
        },
        RelationIndexEntry {
            source_id: "v1/Pod/default/web-abc".into(),
            source_path: "namespaces/default/v1/pod/web-abc.yaml".into(),
            relation: "owned-by".into(),
            target_id: "apps/v1/ReplicaSet/default/web-rs".into(),
            target_kind: "ReplicaSet".into(),
            target_namespace: Some("default".into()),
            target_name: "web-rs".into(),
            target_path: Some("namespaces/default/apps-v1/replicaset/web-rs.yaml".into()),
        },
        RelationIndexEntry {
            source_id: "v1/Pod/default/web-abc".into(),
            source_path: "namespaces/default/v1/pod/web-abc.yaml".into(),
            relation: "scheduled-on".into(),
            target_id: "v1/Node/_cluster/worker1".into(),
            target_kind: "Node".into(),
            target_namespace: None,
            target_name: "worker1".into(),
            target_path: Some("cluster/v1/node/worker1.yaml".into()),
        },
        RelationIndexEntry {
            source_id: "v1/Pod/default/debug-tool".into(),
            source_path: "namespaces/default/v1/pod/debug-tool.yaml".into(),
            relation: "scheduled-on".into(),
            target_id: "v1/Node/_cluster/control-plane".into(),
            target_kind: "Node".into(),
            target_namespace: None,
            target_name: "control-plane".into(),
            target_path: Some("cluster/v1/node/control-plane.yaml".into()),
        },
    ];

    let logs = vec![
        LogIndexEntry {
            path: "namespaces/default/v1/pod/web-abc/app/current.log".into(),
            resource_path: "namespaces/default/v1/pod/web-abc.yaml".into(),
            resource_id: Some("v1/Pod/default/web-abc".into()),
            namespace: Some("default".into()),
            name: "web-abc".into(),
            kind: "Pod".into(),
            container: Some("app".into()),
            log_kind: "current".into(),
            source: "pod".into(),
            size_bytes: 128,
            line_count: 4,
            warn_count: 1,
            error_count: 2,
            first_line: Some("2026-01-01T00:00:00Z INFO starting".into()),
            last_line: Some("2026-01-01T00:00:03Z ERROR timeout".into()),
        },
        LogIndexEntry {
            path: "namespaces/default/v1/pod/debug-tool/debug/current.log".into(),
            resource_path: "namespaces/default/v1/pod/debug-tool.yaml".into(),
            resource_id: Some("v1/Pod/default/debug-tool".into()),
            namespace: Some("default".into()),
            name: "debug-tool".into(),
            kind: "Pod".into(),
            container: Some("debug".into()),
            log_kind: "current".into(),
            source: "pod".into(),
            size_bytes: 64,
            line_count: 2,
            warn_count: 0,
            error_count: 0,
            first_line: Some("starting debug".into()),
            last_line: Some("running".into()),
        },
        LogIndexEntry {
            path: "cluster/v1/node/worker1/kubelet.log".into(),
            resource_path: "cluster/v1/node/worker1.yaml".into(),
            resource_id: Some("v1/Node/_cluster/worker1".into()),
            namespace: None,
            name: "worker1".into(),
            kind: "Node".into(),
            container: None,
            log_kind: "node-kubelet".into(),
            source: "systemd-unit:kubelet".into(),
            size_bytes: 512,
            line_count: 8,
            warn_count: 1,
            error_count: 1,
            first_line: Some("kubelet boot".into()),
            last_line: Some("pod sandbox changed".into()),
        },
    ];

    let app_versions = vec![
        AppVersionEntry {
            name: "web-abc".into(),
            namespace: "default".into(),
            container: "app".into(),
            container_type: "app".into(),
            version: "nginx:1.27.0".into(),
        },
        AppVersionEntry {
            name: "debug-tool".into(),
            namespace: "default".into(),
            container: "debug".into(),
            container_type: "app".into(),
            version: "busybox:1.36".into(),
        },
    ];

    write_yaml(
        root.join("analysis-schema.yaml"),
        &AnalysisSchema::current(&report.identity),
    )?;
    write_yaml(root.join("run-report.yaml"), &report)?;
    write_yaml(root.join("run-stats.yaml"), &report.stats)?;
    write_yaml(root.join("run-failures.yaml"), &report.failures)?;
    write_yaml(root.join("run-warnings.yaml"), &report.warnings)?;
    write_yaml(root.join("app-versions.yaml"), &app_versions)?;
    write_text(
        root.join("AGENT-START.md"),
        "# AGENT START\n\nSynthetic snapshot fixture.\n",
    )?;
    write_jsonl(root.join("resource-index.jsonl"), &resources)?;
    write_jsonl(root.join("relation-index.jsonl"), &relations)?;
    write_jsonl(root.join("log-index.jsonl"), &logs)?;

    write_text(
        root.join("cluster/v1/node/control-plane.yaml"),
        r#"apiVersion: v1
kind: Node
metadata:
  name: control-plane
  labels:
    node-role.kubernetes.io/control-plane: "true"
status:
  conditions:
    - type: Ready
      status: "False"
      reason: KubeletNotReady
"#,
    )?;
    write_text(
        root.join("cluster/v1/node/worker1.yaml"),
        r#"apiVersion: v1
kind: Node
metadata:
  name: worker1
  labels:
    kubernetes.io/os: linux
status:
  conditions:
    - type: Ready
      status: "True"
"#,
    )?;
    write_text(
        root.join("namespaces/default/apps-v1/deployment/web.yaml"),
        r#"apiVersion: apps/v1
kind: Deployment
metadata:
  name: web
  namespace: default
spec:
  replicas: 2
"#,
    )?;
    write_text(
        root.join("namespaces/default/apps-v1/replicaset/web-rs.yaml"),
        r#"apiVersion: apps/v1
kind: ReplicaSet
metadata:
  name: web-rs
  namespace: default
  ownerReferences:
    - apiVersion: apps/v1
      kind: Deployment
      name: web
spec: {}
"#,
    )?;
    write_text(
        root.join("namespaces/default/v1/pod/web-abc.yaml"),
        r#"apiVersion: v1
kind: Pod
metadata:
  name: web-abc
  namespace: default
  labels:
    app: web
  ownerReferences:
    - apiVersion: apps/v1
      kind: ReplicaSet
      name: web-rs
spec:
  serviceAccountName: default
  nodeName: worker1
  containers:
    - name: app
      image: nginx:1.27.0
      resources:
        requests:
          cpu: 100m
          memory: 128Mi
        limits:
          cpu: 250m
          memory: 256Mi
status:
  phase: Running
  containerStatuses:
    - name: app
      ready: false
      restartCount: 7
      state:
        waiting:
          reason: CrashLoopBackOff
"#,
    )?;
    write_text(
        root.join("namespaces/default/v1/pod/debug-tool.yaml"),
        r#"apiVersion: v1
kind: Pod
metadata:
  name: debug-tool
  namespace: default
  labels:
    app: debug
spec:
  serviceAccountName: default
  nodeName: control-plane
  hostNetwork: true
  hostPID: true
  volumes:
    - name: host-root
      hostPath:
        path: /
  containers:
    - name: debug
      image: busybox:1.36
      securityContext:
        privileged: true
        allowPrivilegeEscalation: true
        runAsUser: 0
status:
  phase: Running
  containerStatuses:
    - name: debug
      ready: true
      restartCount: 0
"#,
    )?;
    write_text(
        root.join("namespaces/default/v1/service/web.yaml"),
        r#"apiVersion: v1
kind: Service
metadata:
  name: web
  namespace: default
spec:
  type: LoadBalancer
  selector:
    app: web
  ports:
    - port: 80
      targetPort: 8080
"#,
    )?;
    write_text(
        root.join("namespaces/default/v1/service/orphan.yaml"),
        r#"apiVersion: v1
kind: Service
metadata:
  name: orphan
  namespace: default
spec:
  type: NodePort
  selector:
    app: ghost
  ports:
    - port: 8080
      nodePort: 30080
"#,
    )?;
    write_text(
        root.join("namespaces/default/networking.k8s.io-v1/ingress/web.yaml"),
        r#"apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: web
  namespace: default
spec:
  rules:
    - host: web.example.internal
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service:
                name: web
                port:
                  number: 80
"#,
    )?;
    write_text(
        root.join("cluster/rbac.authorization.k8s.io-v1/clusterrolebinding/admin-binding.yaml"),
        r#"apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: admin-binding
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: cluster-admin
subjects:
  - kind: ServiceAccount
    name: admin
    namespace: default
"#,
    )?;
    write_text(
        root.join("cluster/rbac.authorization.k8s.io-v1/clusterrole/wild-reader.yaml"),
        r#"apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: wild-reader
rules:
  - apiGroups: ["*"]
    resources: ["*"]
    verbs: ["get", "list", "watch"]
"#,
    )?;
    write_text(
        root.join("namespaces/default/v1/pod/web-abc/app/current.log"),
        "2026-01-01T00:00:00Z INFO starting\n2026-01-01T00:00:01Z WARN slow boot\n2026-01-01T00:00:02Z ERROR upstream refused\n2026-01-01T00:00:03Z ERROR timeout\n",
    )?;
    write_text(
        root.join("namespaces/default/v1/pod/debug-tool/debug/current.log"),
        "starting debug\nrunning\n",
    )?;
    write_text(
        root.join("cluster/v1/node/worker1/kubelet.log"),
        "kubelet boot\nwarning: cpu manager slow path\nerror: pod sandbox changed\n",
    )?;

    Ok(SampleSnapshot {
        _tempdir: tempdir,
        root,
    })
}

fn write_yaml(path: PathBuf, value: &impl Serialize) -> anyhow::Result<()> {
    let payload = serde_yaml::to_string(value)?;
    write_text(path, payload.as_str())
}

fn write_jsonl<T: Serialize>(path: PathBuf, values: &[T]) -> anyhow::Result<()> {
    let mut payload = values
        .iter()
        .map(serde_json::to_string)
        .collect::<Result<Vec<_>, _>>()?
        .join("\n");
    if !payload.is_empty() {
        payload.push('\n');
    }
    write_text(path, payload.as_str())
}

fn write_text(path: PathBuf, payload: &str) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, payload)?;
    Ok(())
}
