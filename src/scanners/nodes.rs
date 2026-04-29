use std::{
    fmt::{self, Debug},
    sync::Arc,
};

use async_trait::async_trait;
use k8s_openapi::api::core::v1::{
    Container, HostPathVolumeSource, Node, Pod, PodSpec, Toleration, Volume, VolumeMount,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Status;
use kube::Api;
use kube::{
    api::TypeMeta,
    core::{ApiResource, ObjectMeta, ResourceExt, params::DeleteParams, subresource::AttachParams},
};
use thiserror::Error;
use tokio::{
    io::AsyncReadExt,
    sync::Mutex,
    time::{Duration, Instant, sleep},
};
use tracing::instrument;

use crate::{
    cli::DebugPod,
    gather::{
        config::{CollectionTuning, Config, Secrets},
        report::RunReportState,
        representation::{ArchivePath, LogGroup, Representation},
        writer::Writer,
    },
};

use super::{
    interface::{Collect, CollectError},
    objects::Objects,
};

/// Failure of debug pod
#[derive(Debug, Error)]
pub enum DebugPodError {
    #[error("Failed to create pod: {0:?}")]
    Create(kube::Error),

    #[error("Failed to get pod: {0:?}")]
    Get(kube::Error),
}

struct NodeLogCommand {
    source: String,
    args: Vec<String>,
    path: ArchivePath,
}

struct NodeLogCapture {
    representation: Option<Representation>,
    warning: Option<String>,
    warn_always: bool,
}

struct CommandOutput {
    stdout: String,
    stderr: String,
    status: Option<Status>,
}

#[derive(Clone)]
pub struct Nodes {
    pub collectable: Objects<Node>,
    systemd_units: Vec<String>,
    pub debug_pod: DebugPod,
}

impl Debug for Nodes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Nodes").finish()
    }
}

impl From<Config> for Nodes {
    fn from(value: Config) -> Self {
        Self {
            systemd_units: value.systemd_units.clone(),
            debug_pod: value.debug_pod.clone(),
            collectable: Objects::new_typed(value),
        }
    }
}

#[async_trait]
impl Collect<Node> for Nodes {
    fn get_secrets(&self) -> Secrets {
        self.collectable.get_secrets()
    }

    fn get_writer(&self) -> Arc<Mutex<Writer>> {
        self.collectable.get_writer()
    }

    fn get_report(&self) -> Arc<Mutex<RunReportState>> {
        self.collectable.get_report()
    }

    fn get_tuning(&self) -> CollectionTuning {
        self.collectable.get_tuning()
    }

    fn filter(&self, obj: &Node) -> Result<bool, CollectError> {
        self.collectable.filter(obj)
    }

    fn collector_name(&self) -> String {
        "node-kubelet-logs".to_string()
    }

    /// Collects container logs representations.
    #[instrument(skip_all, fields(node = node.name_any()), err)]
    async fn representations(&self, node: &Node) -> anyhow::Result<Vec<Representation>> {
        tracing::info!("Collecting node logs");

        let node_name = node.name_any();
        let pod = Self::get_template_pod(&self.debug_pod, "node-debug".into(), node_name);
        let pod_name = pod.name_any();
        self.get_or_create(pod).await?;

        self.collect_logs(node, pod_name).await
    }

    fn get_api(&self) -> Api<Node> {
        self.collectable.get_api()
    }

    #[allow(refining_impl_trait)]
    fn resource(&self) -> ApiResource {
        self.collectable.resource()
    }
}

impl Nodes {
    #[instrument(skip_all, fields(pod_name = pod.name_any()), err)]
    async fn get_or_create(&self, pod: Pod) -> anyhow::Result<()> {
        let api = Api::default_namespaced(self.get_api().into());
        let pod_name = pod.name_any();

        let found = api
            .get_opt(pod_name.as_str())
            .await
            .map_err(DebugPodError::Get)?;

        if found.is_some() {
            tracing::info!("Refreshing existing debug pod");
            api.delete(&pod_name, &DeleteParams::default().grace_period(0))
                .await?;
            self.wait_for_pod_deleted(&api, &pod_name).await?;
        }

        tracing::info!("Creating debug pod");
        api.create(&Default::default(), &pod)
            .await
            .map_err(DebugPodError::Create)?;

        Ok(())
    }

    #[instrument(skip_all, fields(pod_name = pod_name), err)]
    async fn collect_logs(
        &self,
        node: &Node,
        pod_name: String,
    ) -> anyhow::Result<Vec<Representation>> {
        let api = Api::default_namespaced(self.get_api().into());
        let collection_result = async {
            tracing::info!("Waiting for pod to be running");
            self.wait_for_pod_running(&api, &pod_name).await?;

            let mut representations = vec![];
            let mut captures = vec![];

            for command in self.node_log_commands(node) {
                let capture = self
                    .get_representation(pod_name.as_str(), &command.args, command.path)
                    .await?;
                captures.push((command.source, capture));
            }

            let collected_any = captures
                .iter()
                .any(|(_, capture)| capture.representation.is_some());

            for (source, capture) in captures {
                self.extend_with_node_log(
                    &mut representations,
                    node,
                    &source,
                    capture,
                    collected_any,
                )
                .await;
            }

            if !collected_any {
                anyhow::bail!("no node logs were collected from any configured source");
            }

            Ok(representations)
        }
        .await;

        if let Err(error) = api
            .delete(&pod_name, &DeleteParams::default().grace_period(0))
            .await
        {
            self.get_report().lock().await.record_warning(
                "node-logs",
                self.collector_name(),
                Some(node.name_any()),
                format!("Failed to delete debug pod {pod_name}: {error}"),
            );
        }

        collection_result
    }

    #[instrument(skip_all, fields(node = path.to_string()))]
    async fn get_representation(
        &self,
        pod_name: &str,
        args: &[String],
        path: ArchivePath,
    ) -> anyhow::Result<NodeLogCapture> {
        let api: Api<Pod> = Api::default_namespaced(self.get_api().into());

        let mut attached = api.exec(pod_name, args, &AttachParams::default()).await?;

        let stdout = attached.stdout().expect("stdout should be attached");
        let stderr = attached.stderr().expect("stderr should be attached");
        let status = attached.take_status();

        let stdout_fut = Self::read_stream_to_string(stdout);
        let stderr_fut = Self::read_stream_to_string(stderr);
        let status_fut = async {
            match status {
                Some(status) => status.await,
                None => None,
            }
        };

        let (stdout, stderr, status, join_result) =
            tokio::join!(stdout_fut, stderr_fut, status_fut, attached.join());

        join_result?;

        Ok(Self::capture_node_log(
            path,
            CommandOutput {
                stdout: stdout?,
                stderr: stderr?,
                status,
            },
        ))
    }

    async fn extend_with_node_log(
        &self,
        representations: &mut Vec<Representation>,
        node: &Node,
        source: &str,
        capture: NodeLogCapture,
        collected_any: bool,
    ) {
        if let Some(representation) = capture.representation {
            representations.push(representation);
        }

        if let Some(warning) = capture
            .warning
            .filter(|_| !collected_any || capture.warn_always)
        {
            self.get_report().lock().await.record_warning(
                "node-logs",
                self.collector_name(),
                Some(node.name_any()),
                format!("{warning} from {source}"),
            );
        }
    }

    async fn wait_for_pod_running(&self, api: &Api<Pod>, pod_name: &str) -> anyhow::Result<()> {
        let deadline = Instant::now() + Duration::from_secs(30);

        loop {
            let pod = api.get(pod_name).await?;
            let phase = pod
                .status
                .as_ref()
                .and_then(|status| status.phase.clone())
                .unwrap_or_default();

            match phase.as_str() {
                "Running" => {
                    tracing::info!("Attaching to pod");
                    return Ok(());
                }
                "Failed" | "Succeeded" => {
                    anyhow::bail!(
                        "debug pod {pod_name} entered terminal phase {phase} before collection"
                    );
                }
                _ if Instant::now() >= deadline => {
                    anyhow::bail!("timed out waiting for debug pod {pod_name} to be running");
                }
                _ => sleep(Duration::from_millis(500)).await,
            }
        }
    }

    async fn wait_for_pod_deleted(&self, api: &Api<Pod>, pod_name: &str) -> anyhow::Result<()> {
        let deadline = Instant::now() + Duration::from_secs(30);

        loop {
            if api.get_opt(pod_name).await?.is_none() {
                return Ok(());
            }

            if Instant::now() >= deadline {
                anyhow::bail!("timed out waiting for debug pod {pod_name} to be deleted");
            }

            sleep(Duration::from_millis(500)).await;
        }
    }

    fn node_log_commands(&self, node: &Node) -> Vec<NodeLogCommand> {
        let mut commands = vec![Self::legacy_log_command(node)];
        commands.extend(
            self.systemd_units
                .iter()
                .map(|systemd_unit| Self::systemd_log_command(node, systemd_unit)),
        );
        commands
    }

    fn legacy_log_command(node: &Node) -> NodeLogCommand {
        NodeLogCommand {
            source: "legacy-kubelet-log".into(),
            args: vec![
                "sh".into(),
                "-c".into(),
                "if [ -r /host/var/log/kubelet.log ]; then cat /host/var/log/kubelet.log; fi"
                    .into(),
            ],
            path: ArchivePath::logs_path(
                node,
                TypeMeta::resource::<Node>(),
                LogGroup::KubeletLegacy,
            ),
        }
    }

    fn systemd_log_command(node: &Node, systemd_unit: &str) -> NodeLogCommand {
        NodeLogCommand {
            source: format!("systemd-unit:{systemd_unit}"),
            args: vec![
                "chroot".into(),
                "/host".into(),
                "/bin/sh".into(),
                "-lc".into(),
                format!("journalctl -u {systemd_unit} --no-pager"),
            ],
            path: ArchivePath::logs_path(
                node,
                TypeMeta::resource::<Node>(),
                LogGroup::Kubelet(systemd_unit.into()),
            ),
        }
    }

    async fn read_stream_to_string(
        mut reader: impl tokio::io::AsyncRead + Unpin,
    ) -> anyhow::Result<String> {
        let mut buffer = vec![];
        reader.read_to_end(&mut buffer).await?;
        Ok(String::from_utf8_lossy(&buffer).into_owned())
    }

    fn capture_node_log(path: ArchivePath, output: CommandOutput) -> NodeLogCapture {
        let has_output = !output.stdout.trim().is_empty();
        let warning_detail = Self::command_warning_detail(output.status.as_ref(), &output.stderr);

        match (has_output, warning_detail) {
            (true, Some(warning)) => NodeLogCapture {
                representation: Some(
                    Representation::new()
                        .with_path(path)
                        .with_data(&output.stdout),
                ),
                warning: Some(format!(
                    "Collected node log with command warnings: {warning}"
                )),
                warn_always: true,
            },
            (true, None) => NodeLogCapture {
                representation: Some(
                    Representation::new()
                        .with_path(path)
                        .with_data(&output.stdout),
                ),
                warning: None,
                warn_always: false,
            },
            (false, Some(warning)) => NodeLogCapture {
                representation: None,
                warning: Some(format!("No output collected: {warning}")),
                warn_always: true,
            },
            (false, None) => {
                tracing::debug!("Node debug output is unavailable.");
                NodeLogCapture {
                    representation: None,
                    warning: Some("No output collected".into()),
                    warn_always: false,
                }
            }
        }
    }

    fn command_warning_detail(status: Option<&Status>, stderr: &str) -> Option<String> {
        let stderr = stderr.trim();
        let status = Self::status_warning_detail(status);
        match (status, stderr.is_empty()) {
            (Some(status), false) => Some(format!("{status} | {stderr}")),
            (Some(status), true) => Some(status),
            (None, false) => Some(stderr.into()),
            (None, true) => None,
        }
    }

    fn status_warning_detail(status: Option<&Status>) -> Option<String> {
        let status = status?;
        let mut parts = vec![];

        if let Some(state) = status.status.as_ref() {
            if state != "Success" {
                parts.push(state.clone());
            }
        }

        if let Some(reason) = status.reason.as_ref() {
            parts.push(reason.clone());
        }

        if let Some(code) = status.code {
            parts.push(format!("code={code}"));
        }

        if let Some(message) = status.message.as_ref() {
            parts.push(message.clone());
        }

        if let Some(causes) = status
            .details
            .as_ref()
            .and_then(|details| details.causes.as_ref())
        {
            for cause in causes {
                if let Some(reason) = cause.reason.as_ref() {
                    parts.push(reason.clone());
                }
                if let Some(message) = cause.message.as_ref() {
                    parts.push(message.clone());
                }
            }
        }

        if parts.is_empty() {
            None
        } else {
            Some(parts.join(" | "))
        }
    }

    pub fn get_template_pod(debug_pod: &DebugPod, log_path: String, node_name: String) -> Pod {
        Pod {
            metadata: ObjectMeta {
                name: Some(format!("{log_path}-{node_name}")),
                ..Default::default()
            },
            spec: Some(PodSpec {
                node_name: Some(node_name),
                restart_policy: Some("Never".into()),
                dns_policy: Some("ClusterFirst".into()),
                enable_service_links: Some(true),
                host_ipc: Some(true),
                host_network: Some(true),
                host_pid: Some(true),
                tolerations: Some(vec![
                    Toleration {
                        operator: Some("Exists".into()),
                        ..Default::default()
                    },
                    Toleration {
                        operator: Some("Exists".into()),
                        key: Some("NoSchedule".into()),
                        ..Default::default()
                    },
                ]),
                containers: vec![Container {
                    name: "debug".into(),
                    stdin: Some(true),
                    tty: Some(true),
                    command: Some(vec![
                        "sh".into(),
                        "-c".into(),
                        "while true; do sleep 3600; done".into(),
                    ]),
                    image: debug_pod.image.clone().or(Some("busybox".into())),
                    image_pull_policy: Some("IfNotPresent".into()),
                    volume_mounts: Some(vec![VolumeMount {
                        name: "host-root".into(),
                        mount_path: "/host".into(),
                        ..Default::default()
                    }]),
                    ..Default::default()
                }],
                volumes: Some(vec![Volume {
                    name: "host-root".into(),
                    host_path: Some(HostPathVolumeSource {
                        path: "/".into(),
                        type_: Some(String::new()),
                    }),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::{StatusCause, StatusDetails};

    fn test_node() -> Node {
        Node {
            metadata: ObjectMeta {
                name: Some("control-plane".into()),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    #[test]
    fn systemd_log_command_uses_direct_chroot_exec() {
        let command = Nodes::systemd_log_command(&test_node(), "kubelet");
        assert_eq!(
            command.args,
            vec![
                "chroot",
                "/host",
                "/bin/sh",
                "-lc",
                "journalctl -u kubelet --no-pager",
            ]
        );
    }

    #[test]
    fn legacy_log_command_checks_for_a_readable_file() {
        let command = Nodes::legacy_log_command(&test_node());
        assert_eq!(
            command.args,
            vec![
                "sh",
                "-c",
                "if [ -r /host/var/log/kubelet.log ]; then cat /host/var/log/kubelet.log; fi",
            ]
        );
    }

    #[test]
    fn template_pod_keeps_debug_container_running() {
        let pod =
            Nodes::get_template_pod(&DebugPod::default(), "node-debug".into(), "worker1".into());
        let command = pod
            .spec
            .as_ref()
            .and_then(|spec| spec.containers.first())
            .and_then(|container| container.command.clone())
            .expect("debug container command should be present");
        assert_eq!(command, vec!["sh", "-c", "while true; do sleep 3600; done"]);
    }

    #[test]
    fn capture_node_log_reports_status_and_stderr_details() {
        let capture = Nodes::capture_node_log(
            ArchivePath::logs_path(
                &test_node(),
                TypeMeta::resource::<Node>(),
                LogGroup::Kubelet("kubelet".into()),
            ),
            CommandOutput {
                stdout: String::new(),
                stderr: "cat: can't open '/host/var/log/kubelet.log': No such file or directory"
                    .into(),
                status: Some(Status {
                    status: Some("Failure".into()),
                    reason: Some("NonZeroExitCode".into()),
                    code: Some(137),
                    message: Some("command terminated with exit code 1".into()),
                    details: Some(StatusDetails {
                        causes: Some(vec![StatusCause {
                            reason: Some("ExitCode".into()),
                            message: Some("1".into()),
                            ..Default::default()
                        }]),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
            },
        );

        assert!(capture.representation.is_none());
        assert_eq!(
            capture.warning,
            Some(
                "No output collected: Failure | NonZeroExitCode | code=137 | command terminated with exit code 1 | ExitCode | 1 | cat: can't open '/host/var/log/kubelet.log': No such file or directory".into()
            )
        );
        assert!(capture.warn_always);
    }

    #[test]
    fn capture_node_log_marks_empty_fallback_as_nonfatal() {
        let capture = Nodes::capture_node_log(
            ArchivePath::logs_path(
                &test_node(),
                TypeMeta::resource::<Node>(),
                LogGroup::KubeletLegacy,
            ),
            CommandOutput {
                stdout: String::new(),
                stderr: String::new(),
                status: None,
            },
        );

        assert!(capture.representation.is_none());
        assert_eq!(capture.warning, Some("No output collected".into()));
        assert!(!capture.warn_always);
    }
}
