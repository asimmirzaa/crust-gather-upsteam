use crate::{
    filters::filter::Filter,
    gather::{
        config::{CollectionTuning, Config, Secrets},
        report::RunReportState,
        representation::TypeMetaGetter,
        writer::Writer,
    },
};
use async_trait::async_trait;

use kube::Api;
use kube::core::{ApiResource, GroupVersionKind, Resource};
use tokio::sync::Mutex;
use tracing::instrument;

use std::{fmt::Debug, sync::Arc};

use super::interface::{Collect, CollectError, ResourceReq, ResourceThreadSafe};

#[derive(Clone)]
pub struct Objects<R: Resource> {
    pub api: Api<R>,
    pub filter: Arc<dyn Filter<R>>,
    pub resource: ApiResource,
    secrets: Secrets,
    writer: Arc<Mutex<Writer>>,
    report: Arc<Mutex<RunReportState>>,
    tuning: CollectionTuning,
}

impl<R: ResourceThreadSafe> Debug for Objects<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Object")
            .field("resource", &self.resource.kind)
            .finish_non_exhaustive()
    }
}

impl<R> Objects<R>
where
    R: Resource<DynamicType = ApiResource> + ResourceReq,
{
    pub fn new(config: Config, resource: ApiResource) -> Self {
        Self {
            api: Api::all_with(config.client, &resource),
            filter: config.filter,
            writer: config.writer,
            secrets: config.secrets,
            report: config.report,
            tuning: config.tuning,
            resource,
        }
    }
}

impl<R> Objects<R>
where
    R: ResourceThreadSafe,
    R::DynamicType: Default,
{
    pub fn new_typed(config: Config) -> Self {
        Self {
            api: Api::all(config.client),
            filter: config.filter,
            writer: config.writer,
            secrets: config.secrets,
            report: config.report,
            tuning: config.tuning,
            resource: ApiResource::erase::<R>(&Default::default()),
        }
    }
}

#[async_trait]
/// Collects default representations for Kubernetes API objects of any type.
impl<R: ResourceThreadSafe> Collect<R> for Objects<R> {
    fn get_secrets(&self) -> Secrets {
        self.secrets.clone()
    }

    fn get_writer(&self) -> Arc<Mutex<Writer>> {
        self.writer.clone()
    }

    fn get_report(&self) -> Arc<Mutex<RunReportState>> {
        self.report.clone()
    }

    fn get_tuning(&self) -> CollectionTuning {
        self.tuning
    }

    #[instrument(skip_all, fields(kind = self.resource().to_type_meta().kind, apiVersion = self.resource().to_type_meta().api_version), err)]
    fn filter(&self, obj: &R) -> Result<bool, CollectError> {
        Ok(self.filter.filter(
            &GroupVersionKind::try_from(self.resource().to_type_meta())
                .map_err(CollectError::GroupVersion)?,
            obj,
        ))
    }

    #[instrument(skip_all, fields(
        kind = self.resource().to_type_meta().kind,
        apiVersion = self.resource().to_type_meta().api_version,
    ))]
    fn get_api(&self) -> Api<R> {
        tracing::debug!("Collecting resources");
        self.api.clone()
    }

    #[allow(refining_impl_trait)]
    fn resource(&self) -> ApiResource {
        self.resource.clone()
    }
}

#[cfg(test)]
mod test {
    use backon::{ConstantBuilder, Retryable};

    use k8s_openapi::{
        api::core::v1::{self, Pod},
        serde_json,
    };
    use kube::core::{
        ApiResource, DynamicObject,
        params::{ListParams, PostParams},
    };
    use kube::{Api, ResourceExt};

    use crate::{
        cli::DEFAULT_OCI_BUFFER_SIZE,
        filters::{
            filter::{FilterGroup, FilterList, Include},
            namespace::Namespace,
        },
        gather::{
            config::{CollectionTuning, Config, GatherMode, Secrets},
            report::RunReportState,
            representation::ArchivePath,
            writer::{Archive, Encoding, Writer},
        },
        scanners::{
            interface::{Collect, CollectError},
            objects::Objects,
        },
    };
    use tokio::sync::Mutex;
    use tokio::time::timeout;

    use std::{collections::BTreeSet, sync::Arc, time::Duration};

    use async_trait::async_trait;

    #[derive(Clone, Debug)]
    struct PaginatedNamespaces {
        collectable: Objects<v1::Namespace>,
    }

    #[async_trait]
    impl Collect<v1::Namespace> for PaginatedNamespaces {
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

        fn filter(&self, obj: &v1::Namespace) -> Result<bool, CollectError> {
            self.collectable.filter(obj)
        }

        fn get_api(&self) -> Api<v1::Namespace> {
            self.collectable.get_api()
        }

        fn list_params(&self) -> ListParams {
            ListParams::default().limit(1)
        }

        #[allow(refining_impl_trait)]
        fn resource(&self) -> ApiResource {
            self.collectable.resource()
        }
    }

    #[tokio::test]
    async fn collect_pod() {
        let test_env = envtest::Environment::default()
            .create()
            .await
            .expect("cluster");
        let client = test_env.client().expect("client");

        let filter = Namespace::<Include>::try_from("default").unwrap();

        let pod_api: Api<Pod> = Api::default_namespaced(client.clone());
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

        let api: Api<DynamicObject> =
            Api::default_namespaced_with(client.clone(), &ApiResource::erase::<Pod>(&()));
        let pod = api.get("test").await.unwrap();
        let repr = Objects::new(
            Config {
                client,
                filter: Arc::new(FilterGroup(vec![FilterList(vec![vec![filter].into()])])),
                writer: Writer::new(
                    &Archive::new("crust-gather".into()),
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
            },
            ApiResource::erase::<Pod>(&()),
        )
        .representations(&pod)
        .await
        .expect("Succeed");

        let repr = &repr[0];

        let existing_pod: Pod = serde_yaml::from_str(repr.data()).unwrap();
        assert_eq!(existing_pod.spec.unwrap().containers[0].name, "test");
    }

    #[tokio::test]
    async fn test_path_cluster_scoped() {
        let test_env = envtest::Environment::default()
            .create()
            .await
            .expect("cluster");
        let client = test_env.client().expect("client");

        let obj = DynamicObject::new("test", &ApiResource::erase::<v1::Namespace>(&()));

        let collectable = Objects::new(
            Config {
                client,
                filter: Arc::new(FilterGroup(vec![FilterList(vec![])])),
                writer: Writer::new(
                    &Archive::new("crust-gather".into()),
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
            },
            ApiResource::erase::<v1::Namespace>(&()),
        );

        let expected = ArchivePath::Cluster("cluster/v1/namespace/test.yaml".into());
        let actual = collectable.path(&obj);

        assert_eq!(expected, actual);
    }

    #[tokio::test]
    async fn list_fetches_all_server_pages() {
        let test_env = envtest::Environment::default()
            .create()
            .await
            .expect("cluster");
        let client = test_env.client().expect("client");

        let namespace_api: Api<v1::Namespace> = Api::all(client.clone());
        let created_names = [
            "pagination-test-a",
            "pagination-test-b",
            "pagination-test-c",
        ];

        for name in created_names {
            timeout(
                Duration::new(10, 0),
                (|| async {
                    namespace_api
                        .create(
                            &PostParams::default(),
                            &serde_json::from_value(serde_json::json!({
                                "apiVersion": "v1",
                                "kind": "Namespace",
                                "metadata": { "name": name }
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
        }

        let collectable = PaginatedNamespaces {
            collectable: Objects::new_typed(Config {
                client,
                filter: Arc::new(FilterGroup(vec![FilterList(vec![])])),
                writer: Writer::new(
                    &Archive::new("crust-gather".into()),
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
        };

        let listed_names = collectable
            .list()
            .await
            .expect("list")
            .into_iter()
            .map(|namespace| namespace.name_any())
            .collect::<BTreeSet<_>>();

        assert!(
            created_names
                .iter()
                .all(|name| listed_names.contains(*name)),
            "expected paginated list to include all created namespaces, got {listed_names:?}"
        );
    }

    #[tokio::test]
    async fn test_path_namespaced() {
        let test_env = envtest::Environment::default()
            .create()
            .await
            .expect("cluster");
        let client = test_env.client().expect("client");

        let obj = DynamicObject::new("test", &ApiResource::erase::<Pod>(&())).within("default");

        let collectable = Objects::new(
            Config {
                client,
                filter: Arc::new(FilterGroup(vec![FilterList(vec![])])),
                writer: Writer::new(
                    &Archive::new("crust-gather".into()),
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
            },
            ApiResource::erase::<Pod>(&()),
        );

        let expected = ArchivePath::Namespaced("namespaces/default/v1/pod/test.yaml".into());
        let actual = collectable.path(&obj);

        assert_eq!(expected, actual);
    }
}
