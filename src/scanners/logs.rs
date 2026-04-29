use std::{
    fmt::{self, Debug, Display},
    sync::Arc,
};

use async_trait::async_trait;
use k8s_openapi::api::core::v1::Pod;
use kube::Api;
use kube::{
    api::TypeMeta,
    core::{ApiResource, ResourceExt, subresource::LogParams},
};
use thiserror::Error;
use tokio::sync::Mutex;
use tracing::instrument;

use crate::gather::{
    config::{CollectionTuning, Config, Secrets},
    report::RunReportState,
    representation::{ArchivePath, Container, LogGroup, Representation},
    writer::Writer,
};

use super::{
    interface::{Collect, CollectError},
    objects::Objects,
    pod_support::pod_container_refs,
};

/// Failure to collect logs
#[derive(Debug, Error)]
#[error("Failed to collect logs: {0:?}")]
pub struct LogsError(kube::Error);

#[derive(Clone, PartialEq)]
pub enum LogSelection {
    Current,
    Previous,
}

impl Display for LogSelection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogSelection::Current => write!(f, "current.log"),
            LogSelection::Previous => write!(f, "previous.log"),
        }
    }
}

impl From<LogSelection> for LogParams {
    fn from(val: LogSelection) -> Self {
        Self {
            previous: val == LogSelection::Previous,
            ..Default::default()
        }
    }
}

/// Logs collects container logs for pods. It contains a Collectable for
/// querying pods and a `LogGroup` to specify whether to collect current or
/// previous logs.
#[derive(Clone)]
pub struct Logs {
    pub collectable: Objects<Pod>,
    pub group: LogSelection,
}

impl Debug for Logs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.group.fmt(f)
    }
}

impl Logs {
    pub fn new(config: Config, group: LogSelection) -> Self {
        Self {
            collectable: Objects::new_typed(config),
            group,
        }
    }
}

fn normalize_logs_result(result: Result<String, kube::Error>) -> Result<Option<String>, LogsError> {
    match result {
        Ok(logs) => Ok(Some(logs)),
        Err(kube::Error::Api(status)) if matches!(status.code, 400 | 404) => Ok(None),
        Err(err) => Err(LogsError(err)),
    }
}

#[async_trait]
impl Collect<Pod> for Logs {
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

    fn collect_concurrency(&self) -> usize {
        self.get_tuning().log_collect_concurrency.max(1)
    }

    fn filter(&self, obj: &Pod) -> Result<bool, CollectError> {
        self.collectable.filter(obj)
    }

    fn collector_name(&self) -> String {
        match self.group {
            LogSelection::Current => "v1/Pod/current-logs".to_string(),
            LogSelection::Previous => "v1/Pod/previous-logs".to_string(),
        }
    }

    /// Collects container logs representations.
    #[instrument(skip_all, fields(name = pod.name_any(), namespace = pod.namespace(), group=self.group.to_string()), err)]
    async fn representations(&self, pod: &Pod) -> anyhow::Result<Vec<Representation>> {
        tracing::debug!("Collecting logs");

        let mut representations = vec![];
        let collector = self.collector_name();
        let pod_ref = pod
            .namespace()
            .map(|namespace| format!("{namespace}/{}", pod.name_any()))
            .unwrap_or_else(|| pod.name_any());

        for container in pod_container_refs(pod) {
            let Some(logs) = normalize_logs_result(
                Api::<Pod>::namespaced(
                    self.get_api().into(),
                    pod.namespace().unwrap_or_default().as_ref(),
                )
                .logs(
                    pod.name_any().as_str(),
                    &LogParams {
                        container: Some(container.name.clone()),
                        since_time: Some(Default::default()),
                        ..self.group.clone().into()
                    },
                )
                .await,
            )?
            else {
                if matches!(self.group, LogSelection::Current) {
                    self.get_report().lock().await.record_warning(
                        "logs",
                        collector.clone(),
                        Some(format!("{pod_ref}:{}", container.name)),
                        format!("No {} logs found for {}", self.group, container.kind),
                    );
                }
                tracing::debug!(
                    container = container.name.as_str(),
                    container_kind = %container.kind,
                    "No logs found"
                );
                continue;
            };

            representations.push(
                Representation::new()
                    .with_path(ArchivePath::logs_path(
                        pod,
                        TypeMeta::resource::<Pod>(),
                        match self.group {
                            LogSelection::Current => {
                                LogGroup::Current(Container(container.name.clone()))
                            }
                            LogSelection::Previous => {
                                LogGroup::Previous(Container(container.name.clone()))
                            }
                        },
                    ))
                    .with_data(logs.as_str()),
            );
        }

        Ok(representations)
    }

    fn get_api(&self) -> Api<Pod> {
        self.collectable.get_api()
    }

    #[allow(refining_impl_trait)]
    fn resource(&self) -> ApiResource {
        self.collectable.resource()
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;
    use std::time::Duration;

    use backon::{ConstantBuilder, Retryable};
    use k8s_openapi::{
        api::core::v1::Pod, apimachinery::pkg::apis::meta::v1::ListMeta, serde_json,
    };
    use kube::Api;
    use kube::core::{Status, params::PostParams};
    use tempfile::TempDir;
    use tokio::time::timeout;

    use crate::cli::DEFAULT_OCI_BUFFER_SIZE;
    use crate::filters::filter::Include;
    use crate::gather::config::GatherMode;
    use crate::{
        filters::{
            filter::{FilterGroup, FilterList},
            namespace::Namespace,
        },
        gather::{
            config::Config,
            writer::{Archive, Encoding, Writer},
        },
        scanners::{interface::Collect, logs::LogSelection, objects::Objects},
    };

    use super::{Logs, LogsError, normalize_logs_result};

    #[test]
    fn normalize_logs_result_treats_400_as_missing_logs() {
        let status = Status {
            code: 400,
            metadata: Some(ListMeta::default()),
            ..Default::default()
        };

        assert_eq!(
            normalize_logs_result(Err(kube::Error::Api(Box::new(status))))
                .expect("missing logs to be ignored"),
            None
        );
    }

    #[test]
    fn normalize_logs_result_preserves_other_api_errors() {
        let status = Status {
            code: 500,
            metadata: Some(ListMeta::default()),
            ..Default::default()
        };

        let err = normalize_logs_result(Err(kube::Error::Api(Box::new(status))))
            .expect_err("500 should bubble");
        let LogsError(kube::Error::Api(status)) = err else {
            panic!("unexpected error variant");
        };

        assert_eq!(status.code, 500);
    }

    #[test]
    fn normalize_logs_result_treats_404_as_missing_logs() {
        let status = Status {
            code: 404,
            metadata: Some(ListMeta::default()),
            ..Default::default()
        };

        assert_eq!(
            normalize_logs_result(Err(kube::Error::Api(Box::new(status))))
                .expect("deleted pods to be ignored"),
            None
        );
    }

    #[tokio::test]
    async fn collect_logs() {
        let test_env = envtest::Environment::default()
            .create()
            .await
            .expect("cluster");
        let filter = Namespace::<Include>::try_from("default").unwrap();

        let pod_api: Api<Pod> = Api::default_namespaced(test_env.client().expect("client"));

        let pod = timeout(
            Duration::new(10, 0),
            (|| async {
                pod_api
                    .create(
                        &PostParams::default(),
                        &serde_json::from_value(serde_json::json!({
                            "apiVersion": "v1",
                            "kind": "Pod",
                            "metadata": {
                                "name": "test",
                            },
                            "spec": {
                                "containers": [{
                                  "name": "test",
                                  "image": "test",
                                }],
                            }
                        }))
                        .expect("Serialize"),
                    )
                    .await
            })
            .retry(ConstantBuilder::default().with_delay(Duration::from_secs(1))),
        )
        .await
        .expect("Timeout")
        .expect("Pod to be created");

        let tmp_dir = TempDir::new().expect("failed to create temp dir");
        let file_path = tmp_dir.path().join("crust-gather-test");
        let repr = Logs {
            collectable: Objects::new_typed(Config {
                client: test_env.client().expect("client"),
                filter: Arc::new(FilterGroup(vec![FilterList(vec![vec![filter].into()])])),
                writer: Writer::new(
                    &Archive::new(file_path),
                    &Encoding::Path,
                    None,
                    None,
                    DEFAULT_OCI_BUFFER_SIZE,
                )
                .await
                .expect("failed to create builder")
                .into(),
                secrets: Default::default(),
                mode: GatherMode::Collect,
                additional_logs: Default::default(),
                duration: "1m".try_into().unwrap(),
                systemd_units: Default::default(),
                debug_pod: Default::default(),
                node_log_mode: crate::cli::NodeLogMode::Deep,
                tuning: Default::default(),
                report: std::sync::Arc::new(tokio::sync::Mutex::new(
                    crate::gather::report::RunReportState::default(),
                )),
            }),
            group: LogSelection::Current,
        }
        .representations(&pod)
        .await
        .expect("Succeed");

        let repr = repr[0].clone();
        assert_eq!(repr.data(), "");
    }
}
