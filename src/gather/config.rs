use std::fmt::Display;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use std::{env, fs};

use anyhow::{self, bail};
use base64::prelude::*;
use duration_string::DurationString;
use futures::future::join_all;
use k8s_openapi::api::core::v1::{ConfigMap, Event, Node, Pod, Secret};
use kube::api::ListParams;
use kube::config::Kubeconfig;
use kube::core::ApiResource;
use kube::core::discovery::verbs::{LIST, WATCH};
use kube::{Api, Client, ResourceExt, discovery};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_saphyr::ser_options;
use tokio::sync::Mutex;
use tokio::time::timeout;
use tracing::instrument;

use crate::cli::{DebugPod, NodeLogMode};
use crate::filters::filter::FilterGroup;
use crate::scanners::dynamic::Dynamic;
use crate::scanners::events::Events;
use crate::scanners::host_logs::HostLogs;
use crate::scanners::info::Info;
use crate::scanners::interface::Collect;
use crate::scanners::logs::{LogSelection, Logs};
use crate::scanners::versions::Versions;

use super::report::RunReportState;
use super::representation::{ArchivePath, CustomLog, NamespaceName, Representation};
use super::writer::Writer;

#[derive(Default, Clone, Debug)]
pub struct Secrets(pub Vec<String>);

#[derive(Default, Clone, Deserialize)]
pub struct SecretsFile(pub PathBuf);

impl Secrets {
    /// Replaces any secrets in representation data with xxx.
    pub fn strip(&self, repr: &Representation) -> Representation {
        let mut data = repr.data().to_string();
        for secret in &self.0 {
            data = data.replace(secret.as_str(), "xxx");
            let b64 = BASE64_STANDARD.encode(secret);
            data = data.replace(b64.as_str(), "xxx");
            data = data.replace(BASE64_STANDARD.encode(b64).as_str(), "xxx");
        }

        repr.clone().with_data(data.as_str())
    }
}

impl From<Vec<String>> for Secrets {
    /// Gets a list of secret environment variable values to exclude from the collected artifacts.
    fn from(val: Vec<String>) -> Self {
        Self(
            val.iter()
                .map(|s| env::var(s).unwrap_or_default())
                .filter(|s| !s.is_empty())
                .collect(),
        )
    }
}

impl TryFrom<SecretsFile> for Secrets {
    type Error = anyhow::Error;

    fn try_from(file: SecretsFile) -> Result<Self, Self::Error> {
        let file = file.0;
        Ok(Self(
            fs::read_to_string(file.as_path())?
                .lines()
                .map(Into::into)
                .collect(),
        ))
    }
}

impl TryFrom<&str> for SecretsFile {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match File::open(value) {
            Ok(_) => Ok(Self(Path::new(value).into())),
            Err(e) => Err(e.into()),
        }
    }
}

impl Display for SecretsFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

#[derive(Clone, Deserialize)]
pub struct ConfigFromConfigMap(pub String);

impl ConfigFromConfigMap {
    pub async fn get_config<D: DeserializeOwned>(&self, client: Client) -> anyhow::Result<D> {
        let api: Api<ConfigMap> = Api::all(client);
        api.list(&ListParams::default())
            .await?
            .iter()
            .filter(|cm| cm.name_any() == self.0)
            .find_map(|cm| self.config_from_cm(cm))
            .ok_or_else(|| anyhow::anyhow!("No configuration map found"))
    }

    fn config_from_cm<D: DeserializeOwned>(&self, cm: &ConfigMap) -> Option<D> {
        // Retrieve the deserialized configuration from the ConfigMap data key
        cm.data
            .clone()?
            .values()
            .find_map(|v| serde_saphyr::from_str(v).ok())
    }
}

impl From<String> for ConfigFromConfigMap {
    fn from(val: String) -> Self {
        Self(val)
    }
}

#[derive(Default, Clone)]
/// `KubeconfigFile` wraps a Kubeconfig struct used to instantiate a Kubernetes client.
pub struct KubeconfigFile(pub Kubeconfig);

impl KubeconfigFile {
    pub fn with_context(mut self, context: Option<&str>) -> anyhow::Result<Self> {
        let Some(context) = context else {
            return Ok(self);
        };

        if !self
            .0
            .contexts
            .iter()
            .any(|candidate| candidate.name == context)
        {
            bail!("context not found in kubeconfig: {context}");
        }

        self.0.current_context = Some(context.to_string());
        Ok(self)
    }

    pub fn infer_file() -> anyhow::Result<Self> {
        Ok(Self(Kubeconfig::read()?))
    }

    pub fn write_to_path(&self, path: &Path) -> anyhow::Result<()> {
        serde_saphyr::to_io_writer(&mut File::create(path)?, &self.0)?;
        Ok(())
    }

    /// Creates a new Kubernetes client from the `KubeconfigFile`.
    pub async fn client(&self, insecure: bool) -> Result<Client, kube::Error> {
        let kubeconfig = match insecure {
            true => KubeconfigFile::insecure(self.into()),
            false => self.into(),
        };

        kubeconfig.try_into()
    }

    /// Creates a new Kubernetes client from the inferred config.
    pub async fn infer(insecure: bool) -> Result<Client, kube::Error> {
        let kubeconfig = match insecure {
            true => KubeconfigFile::insecure(Kubeconfig::read()?),
            false => Kubeconfig::read()?,
        };

        kubeconfig.try_into()
    }

    fn insecure(config: kube::config::Kubeconfig) -> kube::config::Kubeconfig {
        let mut config = config.clone();
        Kubeconfig {
            clusters: config
                .clusters
                .iter_mut()
                .map(|c| {
                    match c.cluster.as_mut() {
                        Some(cluster) => {
                            cluster.insecure_skip_tls_verify = Some(true);
                            c
                        }
                        _ => c,
                    }
                    .clone()
                })
                .collect(),
            ..config
        }
    }
}

impl<'de> Deserialize<'de> for KubeconfigFile {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let path = String::deserialize(deserializer)?;
        path.as_str().try_into().map_err(serde::de::Error::custom)
    }
}

impl TryFrom<&str> for KubeconfigFile {
    type Error = anyhow::Error;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        Ok(Self(serde_saphyr::from_reader(File::open(s)?)?))
    }
}

impl From<&KubeconfigFile> for Kubeconfig {
    fn from(val: &KubeconfigFile) -> Self {
        val.0.clone()
    }
}

#[derive(Default, Clone, Deserialize, Debug)]
/// `KubeconfigSecretLabel` wraps a Kubeconfig secret label used to search a secret to instantiate a Kubernetes client.
pub struct KubeconfigSecretLabel(pub String);

impl KubeconfigSecretLabel {
    pub async fn get_config<D: DeserializeOwned>(&self, client: Client) -> anyhow::Result<Vec<D>> {
        let api: Api<Secret> = Api::all(client);
        Ok(SecretSearch(
            api.list(&ListParams {
                label_selector: Some(self.0.clone()),
                ..Default::default()
            })
            .await?
            .items,
        )
        .lookup())
    }
}

impl From<String> for KubeconfigSecretLabel {
    fn from(value: String) -> Self {
        Self(value)
    }
}

#[derive(Default, Clone, Deserialize, Debug)]
/// `KubeconfigSecretNamespaceName` wraps a Kubeconfig secret namespace/name used to instantiate a Kubernetes client.
pub struct KubeconfigSecretNamespaceName(pub NamespaceName);

impl KubeconfigSecretNamespaceName {
    pub async fn get_config<D: DeserializeOwned>(&self, client: Client) -> anyhow::Result<Vec<D>> {
        let search = match self.0.clone() {
            NamespaceName {
                name: Some(name),
                namespace: Some(namespace),
            } => {
                let api: Api<Secret> = Api::namespaced(client, &namespace);
                SecretSearch(vec![api.get(&name).await?])
            }
            NamespaceName {
                name: Some(name), ..
            } => {
                let api: Api<Secret> = Api::all(client);
                SecretSearch(
                    api.list(&ListParams {
                        ..Default::default()
                    })
                    .await?
                    .items
                    .into_iter()
                    .filter(|s| s.name_any() == name)
                    .collect(),
                )
            }
            NamespaceName { .. } => SecretSearch(vec![]),
        };

        Ok(search.lookup())
    }
}

impl From<String> for KubeconfigSecretNamespaceName {
    fn from(value: String) -> Self {
        Self(value.into())
    }
}

pub struct SecretSearch(Vec<Secret>);

impl SecretSearch {
    pub fn lookup<D: DeserializeOwned>(&self) -> Vec<D> {
        self.0
            .iter()
            .filter_map(|s| self.config_from_secret(s))
            .collect()
    }

    fn config_from_secret<D: DeserializeOwned>(&self, s: &Secret) -> Option<D> {
        // Retrieve the deserialized configuration from the Secret data key
        s.data
            .clone()?
            .values()
            .filter_map(|v| {
                serde_saphyr::to_string_with_options(
                    v,
                    ser_options! {
                        quote_all: true,
                    },
                )
                .ok()
            })
            .filter_map(|v| BASE64_STANDARD.decode(v.replace("'", "").trim_end()).ok())
            .filter_map(|v| String::from_utf8(v).ok())
            .find_map(|v| serde_saphyr::from_str(&v).ok())
    }
}

#[derive(Clone, Deserialize, Copy)]
pub struct RunDuration(DurationString);

impl TryFrom<&str> for RunDuration {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Ok(Self(match DurationString::try_from(value.to_string()) {
            Ok(duration) => duration,
            Err(error) => bail!(error),
        }))
    }
}

impl Default for RunDuration {
    fn default() -> Self {
        Self(DurationString::from(Duration::new(60, 0)))
    }
}

impl Display for RunDuration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, Default, Deserialize)]
pub enum GatherMode {
    #[default]
    Collect,
    Record,
}

#[derive(Clone, Copy, Debug)]
pub struct CollectionTuning {
    pub list_page_limit: u32,
    pub collect_concurrency: usize,
    pub log_collect_concurrency: usize,
}

impl Default for CollectionTuning {
    fn default() -> Self {
        Self {
            list_page_limit: 100,
            collect_concurrency: 32,
            log_collect_concurrency: 8,
        }
    }
}

#[derive(Clone)]
pub struct Config {
    pub client: Client,
    pub filter: Arc<FilterGroup>,
    pub writer: Arc<Mutex<Writer>>,
    pub secrets: Secrets,
    pub mode: GatherMode,
    pub additional_logs: Vec<CustomLog>,
    pub duration: RunDuration,
    pub systemd_units: Vec<String>,
    pub debug_pod: DebugPod,
    pub disable_additional_logs: bool,
    pub skip_logs_collection: bool,
    pub skip_events_collection: bool,
    pub node_log_mode: NodeLogMode,
    pub tuning: CollectionTuning,
    pub report: Arc<Mutex<RunReportState>>,
}

impl Config {
    /// Collect representations for resources from discovery to the specified archive file.
    #[instrument(skip_all, err)]
    pub async fn collect(&self) -> anyhow::Result<()> {
        let collection_result = async {
            let discovery = match discovery::Discovery::new(self.client.clone())
                .run_aggregated()
                .await
            {
                Ok(discovery) => discovery,
                Err(error) => {
                    tracing::warn!(
                        %error,
                        "Aggregated discovery failed, falling back to standard discovery"
                    );
                    discovery::Discovery::new(self.client.clone()).run().await?
                }
            };

            let mode = match self.mode {
                GatherMode::Collect => LIST,
                GatherMode::Record => WATCH,
            };
            let collectables = discovery
                .groups()
                .flat_map(|r| r.resources_by_stability())
                .filter_map(|r| r.1.supports_operation(mode).then_some(r.0.into()))
                .flat_map(|group: Group| group.into_collectable(self.clone()));

            match self.mode {
                GatherMode::Collect => {
                    tracing::info!("Collecting resources...");
                    timeout(
                        self.duration.0.into(),
                        self.iterate_until_completion(collectables),
                    )
                    .await?
                }
                GatherMode::Record => {
                    tracing::info!("Recording resources...");
                    self.iterate_until_completion(collectables).await
                }
            }
        }
        .await;

        if let Err(error) = &collection_result {
            self.report
                .lock()
                .await
                .record_failure("collection", "run", None, error.to_string());
        }

        self.write_run_artifacts(collection_result.is_ok()).await?;
        let finish_result = self.finish().await;

        match (collection_result, finish_result) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(error), Ok(())) => Err(error),
            (Ok(()), Err(error)) => Err(error),
            (Err(collection_error), Err(finish_error)) => Err(anyhow::anyhow!(
                "collection failed: {collection_error}; archive finalization failed: {finish_error}"
            )),
        }
    }

    async fn finish(&self) -> anyhow::Result<()> {
        let writer = &self.writer.clone();
        writer.lock().await.finish_oci().await?;
        writer.lock().await.finish_gzip()?;
        drop(writer.lock().await);
        Ok(())
    }

    async fn iterate_until_completion(
        &self,
        collectables: impl Iterator<Item = Collectable>,
    ) -> anyhow::Result<()> {
        let results = join_all(collectables.map(|c| async move { c.collect().await })).await;
        let errors = results
            .into_iter()
            .filter_map(Result::err)
            .map(|error| error.to_string())
            .collect::<Vec<_>>();

        if errors.is_empty() {
            return Ok(());
        }

        Err(anyhow::anyhow!(
            "collection failed for {} collector(s): {}",
            errors.len(),
            errors.into_iter().take(3).collect::<Vec<_>>().join("; "),
        ))
    }

    async fn write_run_artifacts(&self, success: bool) -> anyhow::Result<()> {
        let (report, stats, failures, warnings) = {
            let mut report = self.report.lock().await;
            report.finalize(success);
            (
                serde_saphyr::to_string(report.report())?,
                serde_saphyr::to_string(report.stats())?,
                serde_saphyr::to_string(report.failures())?,
                serde_saphyr::to_string(report.warnings())?,
            )
        };

        let artifacts = [
            ("run-report.yaml", report),
            ("run-stats.yaml", stats),
            ("run-failures.yaml", failures),
            ("run-warnings.yaml", warnings),
        ];

        let writer = self.writer.clone();
        let mut writer = writer.lock().await;
        for (path, data) in artifacts {
            writer
                .store(
                    &Representation::new()
                        .with_path(ArchivePath::Custom(path.into()))
                        .with_data(data.as_str()),
                )
                .await?;
        }

        Ok(())
    }
}

#[derive(Clone)]
enum Group {
    Nodes(ApiResource),
    Pods(ApiResource),
    Events(ApiResource),
    Dynamic(ApiResource),
}

impl From<ApiResource> for Group {
    fn from(val: ApiResource) -> Self {
        match val {
            r if r == ApiResource::erase::<Event>(&()) => Self::Events(r),
            r if r == ApiResource::erase::<Pod>(&()) => Self::Pods(r),
            r if r == ApiResource::erase::<Node>(&()) => Self::Nodes(r),
            r => Self::Dynamic(r),
        }
    }
}

#[derive(Debug, Clone)]
enum Collectable {
    WatchDynamic(Dynamic),
    Dynamic(Dynamic),
    Pods(Logs),
    Events(Events),
    HostLogs(HostLogs),
    Info(Info),
    Versions(Versions),
}

impl Collectable {
    async fn collect(&self) -> anyhow::Result<()> {
        match self {
            Self::WatchDynamic(o) => o.watch_retry(),
            Self::Dynamic(o) => o.collect_retry(),
            Self::Pods(l) => l.collect_retry(),
            Self::Events(e) => e.collect_retry(),
            Self::HostLogs(u) => u.collect_retry(),
            Self::Info(i) => i.collect_retry(),
            Self::Versions(v) => v.collect_retry(),
        }
        .await
    }
}

impl Group {
    fn into_collectable(self, gather: Config) -> Vec<Collectable> {
        match gather.mode {
            GatherMode::Collect => match self {
                Self::Nodes(resource) => vec![
                    Collectable::Info(Info::new(gather.clone())),
                    Collectable::Dynamic(Dynamic::new(gather.clone(), resource)),
                    Collectable::HostLogs(HostLogs::from(gather)),
                ],
                Self::Pods(resource) => vec![
                    Collectable::Pods(Logs::new(gather.clone(), LogSelection::Current)),
                    Collectable::Pods(Logs::new(gather.clone(), LogSelection::Previous)),
                    Collectable::Versions(Versions::new(gather.clone())),
                    Collectable::Dynamic(Dynamic::new(gather, resource)),
                ],
                Self::Events(resource) => vec![
                    Collectable::Events(Events::from(gather.clone())),
                    Collectable::Dynamic(Dynamic::new(gather, resource)),
                ],
                Self::Dynamic(resource) => {
                    vec![Collectable::Dynamic(Dynamic::new(gather, resource))]
                }
            },
            GatherMode::Record => match self {
                Group::Nodes(resource)
                | Group::Pods(resource)
                | Group::Events(resource)
                | Group::Dynamic(resource) => {
                    vec![
                        Collectable::Info(Info::new(gather.clone())),
                        Collectable::WatchDynamic(Dynamic::new(gather.clone(), resource)),
                    ]
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {

    use tempfile::TempDir;

    use crate::{
        cli::DEFAULT_OCI_BUFFER_SIZE,
        filters::filter::{FilterList, Include},
        gather::writer::{Archive, Encoding},
    };

    use crate::filters::namespace::Namespace;

    use super::*;

    #[test]
    fn test_secrets_empty() {
        let secrets: Secrets = vec![].into();

        assert!(secrets.0.is_empty());
    }

    #[test]
    fn test_secrets_populated() {
        unsafe { env::set_var("FOO", "foo") };
        unsafe { env::set_var("BAR", "bar") };

        let secrets: Secrets = vec!["FOO".into(), "BAR".into(), "OTHER".into()].into();

        assert_eq!(secrets.0, vec!["foo", "bar"]);
    }

    #[test]
    fn test_strip_secrets() {
        unsafe { env::set_var("KEY", "password") };

        let data = "omit password string".to_string();
        let secrets: Secrets = vec!["KEY".to_string()].into();
        let result = secrets.strip(&Representation::new().with_data(data.as_str()));

        assert_eq!(result.data(), "omit xxx string");
    }

    #[test]
    fn test_strip_b64_secrets() {
        unsafe { env::set_var("KEY", "password") };

        let data = "omit cGFzc3dvcmQ= string".to_string();
        let secrets: Secrets = vec!["KEY".to_string()].into();
        let result = secrets.strip(&Representation::new().with_data(data.as_str()));

        assert_eq!(result.data(), "omit xxx string");

        // Double encoded
        let data = "omit Y0dGemMzZHZjbVE9 string".to_string();
        let secrets: Secrets = vec!["KEY".to_string()].into();
        let result = secrets.strip(&Representation::new().with_data(data.as_str()));

        assert_eq!(result.data(), "omit xxx string");
    }

    #[test]
    fn test_strip_secrets_from_file() {
        let data = "omit password string with ip 10.10.10.10".to_string();

        let tmp_dir = TempDir::new().expect("failed to create temp dir");
        let file_path = tmp_dir.path().join("secrets");
        fs::write(file_path.clone(), "password\n10.10.10.10").unwrap();
        let secrets = SecretsFile(file_path);
        let secrets: Secrets = secrets.try_into().unwrap();
        let result = secrets.strip(&Representation::new().with_data(data.as_str()));

        assert_eq!(result.data(), "omit xxx string with ip xxx");
    }

    #[tokio::test]
    async fn test_gzip_collect() {
        let test_env = envtest::Environment::default()
            .create()
            .await
            .expect("cluster");
        let client = test_env.client().expect("client");
        let tmp_dir = TempDir::new().expect("failed to create temp dir");
        let file_path = tmp_dir.path().join("crust-gather-test.zip");
        let f = Namespace::<Include>::try_from("default").unwrap();
        let config = Config {
            client,
            filter: Arc::new(FilterGroup(vec![FilterList(vec![vec![f].into()])])),
            writer: Writer::new(
                &Archive::new(file_path),
                &Encoding::Zip,
                None,
                None,
                DEFAULT_OCI_BUFFER_SIZE,
            )
            .await
            .expect("failed to create builder")
            .into(),
            secrets: Default::default(),
            mode: GatherMode::Collect,
            duration: "10s".try_into().unwrap(),
            additional_logs: Default::default(),
            systemd_units: Default::default(),
            debug_pod: Default::default(),
            disable_additional_logs: false,
            skip_logs_collection: false,
            skip_events_collection: false,
            node_log_mode: crate::cli::NodeLogMode::Deep,
            tuning: Default::default(),
            report: std::sync::Arc::new(tokio::sync::Mutex::new(
                crate::gather::report::RunReportState::default(),
            )),
        };

        // Gzip archive is failing due to timeout.
        // As the archive can't be consumed, it can't be closed other way... (TODO)
        let result = config.collect().await;
        assert!(result.is_err())
    }

    #[tokio::test]
    async fn test_zip_collect() {
        let test_env = envtest::Environment::default()
            .create()
            .await
            .expect("cluster");
        let client = test_env.client().expect("client");

        let tmp_dir = TempDir::new().expect("failed to create temp dir");
        let file_path = tmp_dir.path().join("crust-gather-test.tar.gz");
        let config = Config {
            client,
            filter: Arc::new(FilterGroup(vec![FilterList(vec![])])),
            writer: Writer::new(
                &Archive::new(file_path),
                &Encoding::Gzip,
                None,
                None,
                DEFAULT_OCI_BUFFER_SIZE,
            )
            .await
            .expect("failed to create builder")
            .into(),
            duration: "1m".try_into().unwrap(),
            mode: GatherMode::Collect,
            secrets: Default::default(),
            additional_logs: Default::default(),
            systemd_units: Default::default(),
            debug_pod: Default::default(),
            disable_additional_logs: false,
            skip_logs_collection: false,
            skip_events_collection: false,
            node_log_mode: crate::cli::NodeLogMode::Deep,
            tuning: Default::default(),
            report: std::sync::Arc::new(tokio::sync::Mutex::new(
                crate::gather::report::RunReportState::default(),
            )),
        };

        let result = config.collect().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_path_collect() {
        let test_env = envtest::Environment::default()
            .create()
            .await
            .expect("cluster");
        let client = test_env.client().expect("client");

        let tmp_dir = TempDir::new().expect("failed to create temp dir");
        let file_path = tmp_dir.path().join("crust-gather-test");
        let config = Config {
            client,
            filter: Arc::new(FilterGroup(vec![FilterList(vec![])])),
            writer: Writer::new(
                &Archive::new(file_path.clone()),
                &Encoding::Path,
                None,
                None,
                DEFAULT_OCI_BUFFER_SIZE,
            )
            .await
            .expect("failed to create builder")
            .into(),
            duration: "1m".try_into().unwrap(),
            mode: GatherMode::Collect,
            secrets: Default::default(),
            additional_logs: Default::default(),
            systemd_units: Default::default(),
            debug_pod: Default::default(),
            disable_additional_logs: false,
            skip_logs_collection: false,
            skip_events_collection: false,
            node_log_mode: crate::cli::NodeLogMode::Deep,
            tuning: Default::default(),
            report: std::sync::Arc::new(tokio::sync::Mutex::new(
                crate::gather::report::RunReportState::default(),
            )),
        };

        let result = config.collect().await;
        assert!(result.is_ok());
        assert!(file_path.join("run-report.yaml").is_file());
        assert!(file_path.join("run-stats.yaml").is_file());
        assert!(file_path.join("run-failures.yaml").is_file());
        assert!(file_path.join("run-warnings.yaml").is_file());
    }
}
