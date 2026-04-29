use anyhow::{self, Context};
use async_trait::async_trait;
use backon::{ExponentialBuilder, Retryable};
use chrono::Utc;
use futures::{StreamExt, TryStreamExt as _, stream};
use k8s_openapi::serde_json;
use kube::Api;
use kube::api::WatchEvent;
use kube::core::gvk::ParseGroupVersionError;
use kube::core::params::ListParams;
use kube::core::{DynamicObject, ResourceExt, Status};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::fmt::Debug;
use std::future::Future;
use thiserror::Error;
use tokio::sync::Mutex;
use tracing::instrument;

use std::sync::Arc;
use std::time::Duration;
use trait_set::trait_set;

use crate::gather::config::{CollectionTuning, Secrets};
use crate::gather::report::RunReportState;
use crate::gather::representation::{ArchivePath, Representation, TypeMetaGetter};
use crate::gather::writer::Writer;

trait_set! {
    pub trait Base = Clone + Debug;
    pub trait ThreadSafe = Send + Sync;
    pub trait SerDe = Serialize + DeserializeOwned;
    pub trait ResourceReq = Base + ThreadSafe + SerDe;
    pub trait ResourceThreadSafe = ResourceReq + ResourceExt;
}

/// Indicates failure of conversion to Expression
#[derive(Debug, Error)]
pub enum CollectError {
    #[error("Failed to list resources: {0}")]
    List(kube::Error),

    #[error("Unable to parse froup versoin for object: {0}")]
    GroupVersion(ParseGroupVersionError),
}

#[derive(Debug, Error)]
pub enum WatchError {
    #[error("Failed to watch object: {0}")]
    Watch(#[from] kube::Error),

    #[error("Failed to sync object: {0}")]
    Sync(#[from] anyhow::Error),

    #[error("Failed to stream object events: {0}")]
    Stream(#[from] Box<Status>),

    #[error("Unable to parse froup versoin for object: {0}")]
    GroupVersion(#[from] ParseGroupVersionError),
}

pub const ADDED_ANNOTATION: &str = "crust-gather.io/added";
pub const UPDATED_ANNOTATION: &str = "crust-gather.io/updated";
pub const DELETED_ANNOTATION: &str = "crust-gather.io/deleted";

#[async_trait]
/// Collect defines a trait for collecting Kubernetes object representations.
pub trait Collect<R: ResourceThreadSafe>: Send {
    /// Default retry policy - exponential backoff.
    /// Starts at 10ms, doubles each iteration, up to max of 60s.
    fn retry_policy() -> ExponentialBuilder {
        ExponentialBuilder::default()
            .with_min_delay(Duration::from_millis(10))
            .with_max_delay(Duration::from_secs(60))
            .with_max_times(8)
    }

    fn watch_retry_policy() -> ExponentialBuilder {
        ExponentialBuilder::default()
            .with_min_delay(Duration::from_millis(10))
            .with_max_delay(Duration::from_secs(60))
            .without_max_times()
    }

    async fn retry<T, Fut, F>(&self, action: F) -> anyhow::Result<T>
    where
        T: Send,
        Fut: Future<Output = anyhow::Result<T>> + Send,
        F: FnMut() -> Fut + Send,
    {
        action.retry(Self::retry_policy()).await
    }

    /// Returns the Secrets instance to filter any secrets in the representation
    fn get_secrets(&self) -> Secrets;

    /// Returns the Writer instance for this scanner to write object
    /// representations to.
    fn get_writer(&self) -> Arc<Mutex<Writer>>;

    /// Returns the run report state for recording collection statistics.
    fn get_report(&self) -> Arc<Mutex<RunReportState>>;

    /// Returns collection tuning values.
    fn get_tuning(&self) -> CollectionTuning;

    /// Constructs the path for storing the collected Kubernetes object.
    ///
    /// The path is constructed differently for cluster-scoped vs namespaced objects.
    /// Cluster-scoped objects are stored under `cluster/{api_version}/{kind}/{name}.yaml`.
    /// Namespaced objects are stored under `namespaces/{namespace}/{api_version}/{kind}/{name}.yaml`.
    ///
    /// Example output: `crust-gather/namespaces/default/pod/nginx-deployment-549849849849849849849
    fn path(&self, obj: &R) -> ArchivePath {
        ArchivePath::to_path(obj, self.resource().to_type_meta())
    }

    /// Filters objects based on their GroupVersionKind and the object itself.
    /// Returns true if the object should be included, false otherwise.
    fn filter(&self, object: &R) -> Result<bool, CollectError>;

    /// Converts the provided DynamicObject into a vector of Representation
    /// with YAML object data and output path for the object.
    #[instrument(skip_all, fields(
        kind = self.resource().to_type_meta().kind,
        apiVersion = self.resource().to_type_meta().api_version,
        name = object.name_any(),
        namespace = object.namespace(),
    ), err)]
    async fn representations(&self, object: &R) -> anyhow::Result<Vec<Representation>> {
        tracing::debug!("Collecting representation");

        let obj = serde_json::to_value(object)?;
        let mut data: DynamicObject = serde_json::from_value(obj)?;
        data.types = Some(data.types.unwrap_or(self.resource().to_type_meta()));

        Ok(vec![
            Representation::new()
                .with_path(self.path(object))
                .with_data(serde_saphyr::to_string(&data)?.as_str()),
        ])
    }

    /// Returns the Kubernetes API client for the resource type this scanner handles.
    fn get_api(&self) -> Api<R>;

    /// Returns the list parameters for this scanner.
    ///
    /// Scanners can override this to tune list behavior, such as forcing smaller
    /// server-side pages when a resource kind produces very large responses.
    fn list_params(&self) -> ListParams {
        ListParams::default()
    }

    fn collector_name(&self) -> String {
        let type_meta = self.resource().to_type_meta();
        format!("{}/{}", type_meta.api_version, type_meta.kind)
    }

    /// Returns the TypeMetaGetter for the API resource type this scanner handles.
    /// Used to set the TypeMeta on the returned objects in the list,
    /// as the API server does not provide this data in the response.
    fn resource(&self) -> impl TypeMetaGetter;

    /// Returns the maximum number of object collection tasks to run at once.
    ///
    /// Scanners can override this to reduce pressure on expensive API paths such
    /// as pod log streaming.
    fn collect_concurrency(&self) -> usize {
        self.get_tuning().collect_concurrency
    }

    /// Lists Kubernetes objects of the type handled by this scanner, and set
    /// the get_type_meta() information on the objects. Objects are filtered
    /// before getting added to the result.
    #[instrument(skip_all, fields(kind = self.resource().to_type_meta().kind, apiVersion = self.resource().to_type_meta().api_version), err)]
    async fn list(&self) -> anyhow::Result<Vec<R>> {
        let mut params = self.list_params();
        params
            .limit
            .get_or_insert(self.get_tuning().list_page_limit);

        let mut objects = vec![];
        let collector = self.collector_name();

        loop {
            let data = match self
                .retry(|| async {
                    self.get_api()
                        .list(&params)
                        .await
                        .map_err(CollectError::List)
                        .map_err(anyhow::Error::from)
                })
                .await
            {
                Ok(data) => data,
                Err(error) => {
                    self.get_report().lock().await.record_failure(
                        "list",
                        collector.clone(),
                        None,
                        error.to_string(),
                    );
                    return Err(error);
                }
            };

            objects.extend(
                data.items
                    .into_iter()
                    .filter_map(|o| self.filter(&o).ok()?.then_some(o)),
            );

            match data.metadata.continue_.filter(|token| !token.is_empty()) {
                Some(token) => params.continue_token = Some(token),
                None => break,
            }
        }

        self.get_report()
            .lock()
            .await
            .record_listed(&collector, objects.len());

        Ok(objects)
    }

    /// Lists all object and collects representations for them.
    #[instrument(skip_all, err)]
    async fn collect(&self) -> anyhow::Result<()> {
        let objects = self.list().await?;
        let kind = self.resource().to_type_meta().kind;
        let collector = self.collector_name();
        let collect_tasks = objects.into_iter().map(|object| {
            let kind = kind.clone();
            let object_name = object.name_any();
            let object_ref = object
                .namespace()
                .map(|namespace| format!("{namespace}/{object_name}"))
                .unwrap_or(object_name);

            async move {
                self.write_with_retry(&object)
                    .await
                    .map(|written_files| (object_ref.clone(), written_files))
                    .with_context(|| format!("failed to collect {kind} {object_ref}"))
            }
        });

        let mut collect_tasks = stream::iter(collect_tasks)
            .buffer_unordered(self.collect_concurrency())
            .boxed();
        let mut errors = vec![];

        while let Some(result) = collect_tasks.next().await {
            match result {
                Ok((object_ref, written_files)) => {
                    self.get_report()
                        .lock()
                        .await
                        .record_success(&collector, 1, written_files);
                    tracing::debug!(object = object_ref, written_files, "Collected object");
                }
                Err(err) => {
                    self.get_report().lock().await.record_failure(
                        "collect",
                        collector.clone(),
                        None,
                        err.to_string(),
                    );
                    errors.push(err);
                }
            }
        }

        if !errors.is_empty() {
            let sample_errors = errors
                .iter()
                .take(3)
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join("; ");

            return Err(anyhow::anyhow!(
                "failed to collect {} object(s): {}",
                errors.len(),
                sample_errors,
            ));
        }

        Ok(())
    }

    /// Retries collecting representations using an exponential backoff with jitter.
    /// This helps handle transient errors and spreading load.
    async fn watch_retry(&self) -> anyhow::Result<()> {
        (|| async { self.watch_collect().await })
            .retry(Self::watch_retry_policy())
            .await
            .map_err(anyhow::Error::from)
    }

    /// Retries collecting representations using an exponential backoff with jitter.
    /// This helps handle transient errors and spreading load.
    async fn write_with_retry(&self, object: &R) -> anyhow::Result<usize> {
        let representations = self
            .retry(|| async { self.representations(object).await })
            .await?;

        let writer = self.get_writer();
        let mut written_files = 0;
        for repr in representations {
            writer
                .lock()
                .await
                .store(&self.get_secrets().strip(&repr))
                .await?;
            written_files += 1;
        }

        Ok(written_files)
    }

    /// Collect objects from watch events, storing difference from original as a series of json pathes
    #[instrument(skip_all, err)]
    async fn watch_collect(&self) -> Result<(), WatchError> {
        self.collect().await?;

        let mut stream = self
            .get_api()
            .watch(&Default::default(), "0")
            .await?
            .boxed();

        while let Some(e) = stream.try_next().await? {
            let now = Utc::now().to_string();
            match e {
                WatchEvent::Added(obj) => {
                    let mut obj = obj.clone();
                    obj.annotations_mut()
                        .insert(ADDED_ANNOTATION.to_string(), now);
                    self.sync_with_retry(&obj).await?
                }
                WatchEvent::Modified(obj) => {
                    let mut obj = obj.clone();
                    obj.annotations_mut()
                        .insert(UPDATED_ANNOTATION.to_string(), now);
                    self.sync_with_retry(&obj).await?
                }
                WatchEvent::Deleted(obj) => {
                    let mut obj = obj.clone();
                    obj.annotations_mut()
                        .insert(DELETED_ANNOTATION.to_string(), now);
                    self.sync_with_retry(&obj).await?
                }
                WatchEvent::Error(e) => Err(WatchError::Stream(e))?,
                WatchEvent::Bookmark(_) => (),
            }
        }

        Ok(())
    }

    /// Retries collecting representations using an exponential backoff with jitter.
    /// This helps handle transient errors and spreading load.
    #[instrument(skip_all, err, fields(name = obj.name_any(), namespace = obj.namespace(), gvk))]
    async fn sync_with_retry(&self, obj: &R) -> anyhow::Result<()> {
        let representations = self
            .retry(|| async { self.representations(obj).await })
            .await?;

        let writer = self.get_writer();
        for repr in representations {
            writer
                .lock()
                .await
                .sync(&self.get_secrets().strip(&repr))
                .await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::time::Duration;

    use anyhow::Result;
    use async_trait::async_trait;
    use k8s_openapi::api::core::v1::Pod;
    use kube::Api;
    use kube::ResourceExt;
    use kube::api::TypeMeta;
    use tempfile::TempDir;
    use tokio::sync::Mutex;
    use tokio::time::sleep;

    use crate::cli::DEFAULT_OCI_BUFFER_SIZE;
    use crate::gather::config::{CollectionTuning, Secrets};
    use crate::gather::report::RunReportState;
    use crate::gather::representation::{ArchivePath, Representation};
    use crate::gather::writer::{Archive, Encoding, Writer};

    use super::{Collect, CollectError};

    async fn test_writer(name: &str) -> Arc<Mutex<Writer>> {
        let tmp_dir = TempDir::new().expect("failed to create temp dir");
        let file_path = tmp_dir.path().join(name);
        Writer::new(
            &Archive::new(file_path),
            &Encoding::Path,
            None,
            None,
            DEFAULT_OCI_BUFFER_SIZE,
        )
        .await
        .expect("failed to create writer")
        .into()
    }

    struct ConcurrencyCollector {
        objects: Vec<Pod>,
        writer: Arc<Mutex<Writer>>,
        report: Arc<Mutex<RunReportState>>,
        current_in_flight: Arc<AtomicUsize>,
        max_in_flight: Arc<AtomicUsize>,
        concurrency: usize,
    }

    struct StoreFailingCollector {
        objects: Vec<Pod>,
        writer: Arc<Mutex<Writer>>,
        report: Arc<Mutex<RunReportState>>,
    }

    #[async_trait]
    impl Collect<Pod> for ConcurrencyCollector {
        fn get_secrets(&self) -> Secrets {
            Default::default()
        }

        fn get_writer(&self) -> Arc<Mutex<Writer>> {
            self.writer.clone()
        }

        fn get_report(&self) -> Arc<Mutex<RunReportState>> {
            self.report.clone()
        }

        fn get_tuning(&self) -> CollectionTuning {
            CollectionTuning::default()
        }

        fn filter(&self, _object: &Pod) -> Result<bool, CollectError> {
            Ok(true)
        }

        async fn representations(&self, object: &Pod) -> Result<Vec<Representation>> {
            let in_flight = self.current_in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_in_flight.fetch_max(in_flight, Ordering::SeqCst);

            sleep(Duration::from_millis(25)).await;

            self.current_in_flight.fetch_sub(1, Ordering::SeqCst);

            Ok(vec![
                Representation::new()
                    .with_path(ArchivePath::Custom(
                        format!("{}.log", object.name_any()).into(),
                    ))
                    .with_data("ok"),
            ])
        }

        fn get_api(&self) -> Api<Pod> {
            unreachable!("list is overridden in this test")
        }

        fn resource(&self) -> impl crate::gather::representation::TypeMetaGetter {
            TypeMeta::resource::<Pod>()
        }

        fn collect_concurrency(&self) -> usize {
            self.concurrency
        }

        async fn list(&self) -> Result<Vec<Pod>> {
            Ok(self.objects.clone())
        }
    }

    #[async_trait]
    impl Collect<Pod> for StoreFailingCollector {
        fn get_secrets(&self) -> Secrets {
            Default::default()
        }

        fn get_writer(&self) -> Arc<Mutex<Writer>> {
            self.writer.clone()
        }

        fn get_report(&self) -> Arc<Mutex<RunReportState>> {
            self.report.clone()
        }

        fn get_tuning(&self) -> CollectionTuning {
            CollectionTuning::default()
        }

        fn filter(&self, _object: &Pod) -> Result<bool, CollectError> {
            Ok(true)
        }

        async fn representations(&self, object: &Pod) -> Result<Vec<Representation>> {
            if object.name_any() == "pod-fail" {
                return Ok(vec![Representation::new().with_data("will fail to store")]);
            }

            Ok(vec![
                Representation::new()
                    .with_path(ArchivePath::Custom(
                        format!("{}.log", object.name_any()).into(),
                    ))
                    .with_data("ok"),
            ])
        }

        fn get_api(&self) -> Api<Pod> {
            unreachable!("list is overridden in this test")
        }

        fn resource(&self) -> impl crate::gather::representation::TypeMetaGetter {
            TypeMeta::resource::<Pod>()
        }

        async fn list(&self) -> Result<Vec<Pod>> {
            Ok(self.objects.clone())
        }
    }

    #[tokio::test]
    async fn collect_respects_collect_concurrency() {
        let collector = ConcurrencyCollector {
            objects: (0..6)
                .map(|idx| Pod {
                    metadata: kube::core::ObjectMeta {
                        name: Some(format!("pod-{idx}")),
                        namespace: Some("default".into()),
                        ..Default::default()
                    },
                    ..Default::default()
                })
                .collect(),
            writer: test_writer("collect-concurrency-test").await,
            report: Arc::new(Mutex::new(RunReportState::default())),
            current_in_flight: Arc::new(AtomicUsize::new(0)),
            max_in_flight: Arc::new(AtomicUsize::new(0)),
            concurrency: 2,
        };

        collector.collect().await.expect("collection to succeed");

        assert_eq!(collector.max_in_flight.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn collect_returns_error_when_any_object_collection_fails() {
        let collector = StoreFailingCollector {
            objects: ["pod-ok", "pod-fail"]
                .into_iter()
                .map(|name| Pod {
                    metadata: kube::core::ObjectMeta {
                        name: Some(name.to_string()),
                        namespace: Some("default".into()),
                        ..Default::default()
                    },
                    ..Default::default()
                })
                .collect(),
            writer: test_writer("collect-error-test").await,
            report: Arc::new(Mutex::new(RunReportState::default())),
        };

        let err = collector
            .collect()
            .await
            .expect_err("collection should fail when any object fails");

        assert!(err.to_string().contains("pod-fail"));
    }
}
