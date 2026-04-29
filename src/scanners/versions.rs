use std::{fmt::Debug, sync::Arc};

use async_trait::async_trait;
use k8s_openapi::api::core::v1::Pod;
use kube::core::ApiResource;
use kube::{Api, Resource};
use serde::Serialize;
use tokio::sync::Mutex;
use tracing::instrument;

use crate::gather::{
    config::{CollectionTuning, Config, Secrets},
    report::RunReportState,
    representation::{ArchivePath, Representation},
    writer::Writer,
};

use super::{
    interface::{Collect, CollectError},
    objects::Objects,
    pod_support::pod_container_refs,
};

#[derive(Clone, Debug, Serialize)]
struct Version {
    name: String,
    namespace: String,
    container: String,
    container_type: String,
    version: String,
}

#[derive(Clone, Debug)]
pub struct Versions {
    pub collectable: Objects<Pod>,
}

impl Versions {
    pub fn new(config: Config) -> Self {
        Self {
            collectable: Objects::new_typed(config),
        }
    }
}

#[async_trait]
impl Collect<Pod> for Versions {
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

    fn filter(&self, _: &Pod) -> Result<bool, CollectError> {
        Ok(true)
    }

    fn collector_name(&self) -> String {
        "app-versions".to_string()
    }

    #[instrument(skip_all, err)]
    async fn collect(&self) -> anyhow::Result<()> {
        let collector = self.collector_name();
        let pods = self.list().await?;
        let pod_count = pods.len();

        let data = pods
            .iter()
            .flat_map(|pod| {
                let meta = pod.meta().clone();
                pod_container_refs(pod)
                    .into_iter()
                    .map(move |container| Version {
                        name: meta.name.clone().unwrap_or_default(),
                        namespace: meta.namespace.clone().unwrap_or_default(),
                        container: container.name.clone(),
                        container_type: container.kind.to_string(),
                        version: container.image.unwrap_or_default(),
                    })
            })
            .collect::<Vec<_>>();

        let payload = serde_saphyr::to_string(&data)?;
        if let Err(error) = self
            .get_writer()
            .lock()
            .await
            .store(
                &Representation::new()
                    .with_path(ArchivePath::Custom("app-versions.yaml".into()))
                    .with_data(payload.as_str()),
            )
            .await
        {
            self.get_report().lock().await.record_failure(
                "store",
                collector.clone(),
                None,
                error.to_string(),
            );
            return Err(error);
        }

        self.get_report()
            .lock()
            .await
            .record_success(&collector, pod_count, 1);

        Ok(())
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
mod tests {

    use std::{env, fs::File, io::Write, path::PathBuf, time::Duration};

    use backon::{ConstantBuilder, Retryable};
    use k8s_openapi::{api::core::v1::Pod, serde_json};
    use kube::{Api, api::PostParams};
    use tempfile::TempDir;
    use tokio::{fs, time::timeout};

    use crate::cli::GatherCommands;

    fn temp_kubeconfig() -> PathBuf {
        let mut dir = env::temp_dir();
        dir.push(xid::new().to_string());
        dir
    }

    #[tokio::test]
    async fn test_collect_versions() {
        let test_env = envtest::Environment::default()
            .create()
            .await
            .expect("cluster");
        let kubeconfig = test_env.kubeconfig().expect("kubeconfig");

        let kubeconfig = serde_saphyr::to_string(&kubeconfig).unwrap();
        let path = temp_kubeconfig();
        fs::write(path.clone(), kubeconfig).await.unwrap();

        let tmp_dir = TempDir::new().expect("failed to create temp dir");

        let mut valid = File::create(tmp_dir.path().join("valid.yaml")).unwrap();
        let valid_config = format!(
            r"
            settings:
              file: {}
              kubeconfig: {}
            ",
            tmp_dir.path().join("collect").to_str().unwrap(),
            path.clone().to_str().unwrap(),
        );
        valid.write_all(valid_config.as_bytes()).unwrap();

        let pod_api: Api<Pod> = Api::default_namespaced(test_env.client().expect("client"));
        timeout(
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
        .unwrap();

        let commands =
            GatherCommands::try_from(tmp_dir.path().join("valid.yaml").to_str().unwrap()).unwrap();

        let config = commands.load().await.unwrap();

        config.collect().await.unwrap();
        assert!(
            tmp_dir
                .path()
                .join("collect")
                .join("app-versions.yaml")
                .is_file()
        );
        assert_eq!(
            fs::read_to_string(tmp_dir.path().join("collect").join("app-versions.yaml"))
                .await
                .unwrap(),
            r"- name: test
  namespace: default
  container: test
  container_type: app
  version: test
"
            .to_string()
        )
    }
}
