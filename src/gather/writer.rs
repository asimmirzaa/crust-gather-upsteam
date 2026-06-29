use anyhow::Context;
use backon::{ExponentialBuilder, Retryable};
use base64::{Engine as _, prelude::BASE64_STANDARD};
use flate2::{Compression, write::GzEncoder};

use chrono::Utc;
use futures::{
    StreamExt as _, TryStreamExt as _,
    future::{self},
    stream, try_join,
};
use json_patch::diff;
use k8s_openapi::serde_json;
use oci_client::{
    Client, Reference,
    client::{ClientConfig, Config},
    errors::OciDistributionError,
    manifest::{
        IMAGE_LAYER_MEDIA_TYPE, OCI_IMAGE_MEDIA_TYPE, OciDescriptor, OciImageManifest, OciManifest,
    },
    secrets::RegistryAuth,
};
use serde::{Deserialize, Serialize};
use sha2::Digest as _;
use std::{
    borrow::Cow,
    collections::BTreeMap,
    ffi::OsStr,
    fmt::Display,
    fs::{DirBuilder, File},
    io::{Read as _, Write as _},
    ops::Deref,
    path::PathBuf,
    sync::Arc,
    time::Duration,
};
use tar::{Builder, Header};
use tokio::sync::Mutex;
use tokio_util::bytes;
use tracing::{debug, info, instrument};
use walkdir::WalkDir;
use zip::{ZipWriter, result::ZipError, write::SimpleFileOptions};

use crate::cli::DEFAULT_OCI_BUFFER_SIZE;
use crate::gather::{
    agent_artifacts::AgentArtifactsState,
    reader::{ArchiveReader, Reader},
    report::{CollectorStats, RunMessage, RunReport},
    storage::Storage,
};

use super::representation::{ArchivePath, Representation};

#[derive(Clone, Deserialize)]
pub struct ArchiveSearch(PathBuf);

impl ArchiveSearch {
    pub fn path(&self) -> PathBuf {
        self.0.clone()
    }
}

impl Default for ArchiveSearch {
    fn default() -> Self {
        Self("crust-gather".into())
    }
}

impl From<ArchiveSearch> for PathBuf {
    fn from(val: ArchiveSearch) -> Self {
        val.0
    }
}

impl Display for ArchiveSearch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.display())
    }
}

impl From<&str> for ArchiveSearch {
    fn from(value: &str) -> Self {
        Self(PathBuf::from(value))
    }
}

impl From<ArchiveSearch> for Vec<Archive> {
    fn from(value: ArchiveSearch) -> Self {
        WalkDir::new(value.0)
            .max_depth(5)
            .same_file_system(true)
            .into_iter()
            .filter_map(Result::ok)
            .filter(|f| f.file_name() == "version.yaml")
            .map(|f| {
                f.path()
                    .parent()
                    .unwrap_or(PathBuf::from("snapshot").as_path())
                    .to_path_buf()
            })
            .map(Into::into)
            .collect()
    }
}

#[derive(Clone, Deserialize)]
pub struct Archive(PathBuf);

/// Creates a new Archive instance with the given path.
impl Archive {
    pub fn new(path: PathBuf) -> Self {
        Self(path)
    }

    pub fn path(&self) -> PathBuf {
        self.0.clone()
    }

    pub fn name(&self) -> &OsStr {
        self.0
            .components()
            .next_back()
            .map(|c| c.as_os_str())
            .unwrap_or(OsStr::new("snapshot"))
    }

    pub fn join(&self, path: ArchivePath) -> PathBuf {
        match path {
            ArchivePath::Empty => self.path(),
            ArchivePath::Cluster(path) => self.path().join(path),
            ArchivePath::Namespaced(path) => self.path().join(path),
            ArchivePath::NamespacedList(path) => self.path().join(path),
            ArchivePath::ClusterList(path) => self.path().join(path),
            ArchivePath::Logs(path) => self.path().join(path),
            ArchivePath::Custom(path) => self.path().join(path),
        }
    }
}

impl Default for Archive {
    fn default() -> Self {
        Self("crust-gather".into())
    }
}

impl From<Archive> for PathBuf {
    fn from(val: Archive) -> Self {
        val.0
    }
}

impl From<PathBuf> for Archive {
    fn from(val: PathBuf) -> Archive {
        Archive(val)
    }
}

impl Display for Archive {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.display())
    }
}

impl From<&str> for Archive {
    fn from(value: &str) -> Self {
        Self(PathBuf::from(value))
    }
}

#[derive(Clone, Default, Deserialize)]
/// The Encoding enum represents the supported archive encoding formats.
/// - Path indicates no encoding.
/// - Gzip indicates gzip compression should be used.
/// - Zip indicates zip compression should be used.
pub enum Encoding {
    #[default]
    Path,
    Gzip,
    Zip,
    Oci(Reference),
}

impl From<&str> for Encoding {
    fn from(value: &str) -> Self {
        match value {
            "zip" => Self::Zip,
            "gzip" => Self::Gzip,
            _ => Self::Path,
        }
    }
}

/// The WriterSink enum represents the different archive writer implementations.
/// Gzip uses the gzip compression format.
/// Zip uses the zip compression format.
/// Oci uses the remote image reference as a destination.
enum WriterSink {
    Path(Archive),
    Gzip(Archive, Box<Builder<GzEncoder<File>>>),
    Zip(Archive, Box<ZipWriter<File>>),
    Oci(OCIState),
}

// OCIState holds current OCI writer destination state
pub struct OCIState {
    archive: Archive,
    client: Client,
    config: ManifestConfig,
    image_ref: Box<Reference>,
    auth: RegistryAuth,
    buffer_size: usize,
}

// YamlPath contains a full path in the yaml file in archive
// and a range of bytes to extract yaml from a list
#[derive(Serialize, Deserialize)]
pub struct YamlPath {
    pub path: PathBuf,
    pub from: usize,
    pub to: usize,
}

#[derive(Serialize, Deserialize, Default, Clone)]
pub struct ManifestConfig {
    #[serde(default)]
    pub compressed: bool,
}

pub struct Writer {
    sink: WriterSink,
    agent_artifacts: AgentArtifactsState,
}

impl From<Writer> for Arc<Mutex<Writer>> {
    fn from(val: Writer) -> Self {
        Self::new(Mutex::new(val))
    }
}

impl Writer {
    /// Finish zip archive
    pub fn finish_zip(self) -> anyhow::Result<()> {
        let Writer {
            sink: WriterSink::Zip(_, builder),
            ..
        } = self
        else {
            return anyhow::Result::Ok(());
        };

        builder.finish()?;
        Ok(())
    }

    /// Finish gzip archive
    pub fn finish_gzip(&mut self) -> anyhow::Result<()> {
        let WriterSink::Gzip(archive, _) = &self.sink else {
            return anyhow::Result::Ok(());
        };

        let archive = archive.clone();
        let sink = std::mem::replace(&mut self.sink, WriterSink::Path(archive));
        let WriterSink::Gzip(_, builder) = sink else {
            unreachable!("writer sink variant changed during finish_gzip");
        };

        let encoder = (*builder).into_inner()?;
        encoder.finish()?;
        Ok(())
    }

    /// Finish writing the archive, finalizing any compression and flushing buffers.
    pub async fn finish_oci(&self) -> anyhow::Result<()> {
        if let WriterSink::Oci(ocistate) = &self.sink {
            return ocistate.publish_image().await;
        }

        Ok(())
    }

    /// Adds a representation data to the archive under the representation path
    #[instrument(skip_all, fields(repr = repr.path().to_string()))]
    pub async fn store(&mut self, repr: &Representation) -> anyhow::Result<()> {
        self.store_bytes(repr.path(), repr.data().as_bytes())
            .await?;
        self.agent_artifacts.observe(repr)?;
        Ok(())
    }

    pub async fn store_bytes(
        &mut self,
        path: ArchivePath,
        data: impl AsRef<[u8]>,
    ) -> anyhow::Result<()> {
        tracing::debug!("Writing...");

        let archive_path: String = path.clone().try_into()?;
        let data = data.as_ref();

        match &mut self.sink {
            WriterSink::Path(Archive(archive))
            | WriterSink::Oci(OCIState {
                archive: Archive(archive),
                ..
            }) => {
                let file = archive.join(archive_path);
                DirBuilder::new()
                    .recursive(true)
                    .create(file.parent().unwrap())?;
                let mut file = File::create(file)?;
                file.write_all(data)?;
            }
            WriterSink::Gzip(Archive(archive), builder) => {
                let mut header = Header::new_gnu();
                header.set_size(data.len() as u64);
                header.set_cksum();
                header.set_mode(0o644);

                let root_prefix = archive.file_stem().unwrap();
                let file = PathBuf::from(root_prefix).join(archive_path);
                builder.append_data(&mut header, file, data)?;
            }
            WriterSink::Zip(Archive(archive), writer) => {
                let path = path.parent().unwrap().to_str().unwrap();
                writer
                    .add_directory(path, SimpleFileOptions::default())
                    .or_else(|err| match err {
                        ZipError::InvalidArchive(Cow::Borrowed("Duplicate filename")) => Ok(()),
                        other => Err(other),
                    })?;

                let root_prefix = archive.file_stem().unwrap();
                let file = PathBuf::from(root_prefix).join(archive_path);
                let file = file.to_str().unwrap();
                writer
                    .start_file(file, SimpleFileOptions::default())
                    .or_else(|err| match err {
                        ZipError::InvalidArchive(Cow::Borrowed("Duplicate filename")) => Ok(()),
                        other => Err(other),
                    })?;
                writer.write_all(data)?;
            }
        }
        Ok(())
    }

    pub async fn write_agent_artifacts(
        &mut self,
        report: &RunReport,
        stats: &BTreeMap<String, CollectorStats>,
        failures: &[RunMessage],
        warnings: &[RunMessage],
    ) -> anyhow::Result<()> {
        let artifacts = self
            .agent_artifacts
            .finalize(report, stats, failures, warnings)?;

        for (path, data) in [
            ("AGENT-START.md", artifacts.agent_start.into_bytes()),
            (
                "resource-index.jsonl",
                artifacts.resource_index.into_bytes(),
            ),
            (
                "relation-index.jsonl",
                artifacts.relation_index.into_bytes(),
            ),
            ("log-index.jsonl", artifacts.log_index.into_bytes()),
            ("snapshot.sqlite", artifacts.sqlite_bytes),
        ] {
            self.store_bytes(ArchivePath::Custom(path.into()), data)
                .await?;
        }

        Ok(())
    }

    /// Adds a representation data to the archive under the representation path
    #[instrument(skip_all, fields(repr = repr.path().to_string()))]
    pub async fn sync(&mut self, repr: &Representation) -> anyhow::Result<()> {
        tracing::debug!("Writing...");

        let archive_path: String = repr.path().try_into()?;

        match &mut self.sink {
            WriterSink::Path(archive) => {
                let file_path = archive.0.join(archive_path);

                // generate diff and write
                let original = Reader::new(
                    ArchiveReader::new(archive.clone(), &Storage::FS, DEFAULT_OCI_BUFFER_SIZE)
                        .await,
                    Utc::now(),
                    Storage::FS,
                )
                .await?
                .read(file_path.clone())
                .await?;
                let updated = serde_saphyr::from_str(repr.data())?;
                let patch = &diff(&original, &updated);
                if !patch.deref().is_empty() {
                    let mut patches = File::options()
                        .create(true)
                        .append(true)
                        .open(file_path.with_extension("patch"))?;
                    serde_json::to_writer(patches.try_clone()?, patch)?;
                    patches.write_all(b"\n")?;
                }
                self.store(repr).await?;
            }
            WriterSink::Gzip(Archive(_archive), _builder) => {
                unimplemented!();
            }
            WriterSink::Zip(Archive(_archive), _writer) => {
                unimplemented!();
            }
            WriterSink::Oci(..) => {
                unimplemented!();
            }
        }
        Ok(())
    }

    /// Creates a new `Writer` for the given `Archive` and `Encoding`.
    pub async fn new(
        archive: &Archive,
        encoding: &Encoding,
        client_config: Option<ClientConfig>,
        auth: Option<RegistryAuth>,
        buffer_size: usize,
    ) -> anyhow::Result<Self> {
        let buffer_size = buffer_size.max(1);
        match archive.0.parent() {
            Some(parent) if !parent.as_os_str().is_empty() => {
                DirBuilder::new().recursive(true).create(parent)?;
            }
            Some(_) | None => (),
        };

        Ok(Self {
            sink: match encoding {
                Encoding::Path => WriterSink::Path(archive.clone()),
                Encoding::Gzip => WriterSink::Gzip(
                    archive.clone(),
                    Box::new(Builder::new(GzEncoder::new(
                        File::create(archive.0.with_extension("tar.gz"))?,
                        Compression::default(),
                    ))),
                ),
                Encoding::Zip => WriterSink::Zip(
                    archive.clone(),
                    Box::new(ZipWriter::new(File::create(
                        archive.0.with_extension("zip"),
                    )?)),
                ),
                Encoding::Oci(image_ref) => WriterSink::Oci(OCIState {
                    archive: archive.clone(),
                    config: ManifestConfig { compressed: true },
                    client: Client::new(client_config.unwrap_or_default()),
                    image_ref: image_ref.clone().into(),
                    auth: auth.unwrap_or(RegistryAuth::Anonymous),
                    buffer_size,
                }),
            },
            agent_artifacts: AgentArtifactsState::default(),
        })
    }
}

impl OCIState {
    #[instrument(skip_all, err)]
    async fn publish_image(&self) -> anyhow::Result<()> {
        info!("Pushing image: {:?}", self.image_ref);
        self.client
            .store_auth_if_needed(self.image_ref.resolve_registry(), &self.auth)
            .await;

        let config = Config::new(
            serde_json::to_vec(&self.config)?,
            OCI_IMAGE_MEDIA_TYPE.to_string(),
            None,
        );
        let paths = glob::glob(&format!(
            "{}/**/*",
            self.archive
                .path()
                .to_str()
                .ok_or(anyhow::anyhow!("archive path is not convertable to string"))?,
        ))?;

        let resource_paths = Arc::new(Mutex::new(Default::default()));
        let non_resource_paths: Vec<PathBuf> = stream::iter(paths.into_iter())
            .filter_map(|path| Self::prepare_resource_layer(path, resource_paths.clone()))
            .collect()
            .await;

        // Upload layers
        let layers = Arc::new(Mutex::new(vec![]));
        let raw_layers = stream::iter(non_resource_paths.into_iter())
            .map(|path| self.push_oci_archive_layer(path, layers.clone()))
            .buffer_unordered(self.buffer_size)
            .try_for_each(future::ok::<(), anyhow::Error>);

        let resource_layer_entries = { resource_paths.lock().await.clone() };
        let resources = stream::iter(resource_layer_entries)
            .filter_map(|(p, yamls)| {
                future::ready(
                    Self::combined_oci_archive_layer(&yamls)
                        .ok()
                        .map(|data| (p + ".yaml", data)),
                )
            })
            .map(|(p, data)| self.push_oci_layer(p, data, layers.clone()))
            .buffer_unordered(self.buffer_size)
            .try_for_each(future::ok::<(), anyhow::Error>);

        let yamls = Self::prepare_index(resource_paths.lock().await.values().cloned().collect())?;
        let yamls =
            serde_saphyr::to_string(&yamls).context("unable to collect yamls index file")?;

        let index_layer = self.push_oci_layer("index.yaml".to_string(), yamls, layers.clone());

        try_join!(resources, raw_layers, index_layer).context("failed to upload OCI layers")?;

        info!("Pushing config: {}", self.image_ref);
        let (digest, size) = self.push_blob(config.data).await?;

        let mut manifest = OciImageManifest::default();
        manifest.config.media_type = config.media_type.to_string();
        manifest.layers = layers.lock().await.clone();
        manifest.layers.sort_by(|a, b| a.digest.cmp(&b.digest));
        manifest.config.digest = digest;
        manifest.config.size = size as i64;

        info!("Pushing manifest: {}", self.image_ref);
        let manifest = manifest.into();
        self.push_manifest(&manifest).await
    }

    async fn push_manifest(&self, manifest: &OciManifest) -> anyhow::Result<()> {
        let push = || self.client.push_manifest(&self.image_ref, manifest);
        push.retry(
            ExponentialBuilder::default()
                .with_max_times(20)
                .with_max_delay(Duration::from_secs(10))
                .with_jitter(),
        )
        .when(|e| matches!(e, OciDistributionError::ServerError { code, .. } if *code == 429 ))
        .await?;

        Ok(())
    }

    async fn push_blob(
        &self,
        data: impl Into<bytes::Bytes> + Clone,
    ) -> anyhow::Result<(String, usize)> {
        let mut enc = GzEncoder::new(vec![], Compression::best());
        enc.write_all(&data.into())?;
        let data = BASE64_STANDARD.encode(enc.finish()?);

        let digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(&data)));
        let push = || {
            let data = data.clone();
            self.client.push_blob(&self.image_ref, data, &digest)
        };
        push.retry(
            ExponentialBuilder::default()
                .with_max_times(20)
                .with_max_delay(Duration::from_secs(10))
                .with_jitter(),
        )
        .when(|e| matches!(e, OciDistributionError::ServerError { code, .. } if *code == 429 ))
        .await?;

        Ok((digest, data.len()))
    }

    fn prepare_index(yamls: Vec<Vec<PathBuf>>) -> anyhow::Result<Vec<YamlPath>> {
        let mut list = vec![];
        for yaml_list in yamls {
            let mut index = 0;
            for yaml in yaml_list {
                let path = yaml.to_string_lossy();
                let mut file = File::open(&yaml).context(format!("failed to open file {path}"))?;
                list.push(YamlPath {
                    path: yaml,
                    from: index,
                    to: {
                        let mut data = vec![];
                        file.read_to_end(&mut data)?;
                        index += data.len();
                        index
                    },
                });
                index += 4
            }
        }

        Ok(list)
    }

    async fn prepare_resource_layer(
        path: glob::GlobResult,
        resource_layers: Arc<Mutex<BTreeMap<String, Vec<PathBuf>>>>,
    ) -> Option<PathBuf> {
        let path = path.ok()?;

        if path.is_dir() {
            return Some(path);
        }

        let Some(parent) = path.parent() else {
            return Some(path);
        };

        let Some(ext) = path.extension() else {
            return Some(path);
        };

        if ext != "yaml" || path.to_string_lossy().ends_with("version.yaml") {
            return Some(path);
        }

        let parent = parent.to_str()?;

        debug!("Inserting path {:?} with parent {:?}", path, parent);
        resource_layers
            .lock()
            .await
            .entry(parent.to_string())
            .or_default()
            .push(path);

        None
    }

    async fn push_oci_layer(
        &self,
        archive_path: String,
        mut data: String,
        layers: Arc<Mutex<Vec<OciDescriptor>>>,
    ) -> anyhow::Result<()> {
        if data.is_empty() {
            // That could only happen for empty logs, so we publish an empty json instead
            // as ghcr doesn't allow empty layers
            data = "{}".to_string();
        };

        info!("Pushing layer: {:?}", archive_path);
        let (digest, size) = self.push_blob(data).await?;
        {
            layers.lock().await.push(OciDescriptor {
                artifact_type: None,
                urls: None,
                media_type: IMAGE_LAYER_MEDIA_TYPE.to_string(),
                digest,
                size: size as i64,
                annotations: Some(
                    [(
                        "org.opencontainers.image.title".to_string(),
                        archive_path.to_string(),
                    )]
                    .into(),
                ),
            });
        }

        Ok(())
    }

    #[instrument(skip_all, err)]
    fn combined_oci_archive_layer(yamls: &Vec<PathBuf>) -> anyhow::Result<String> {
        let mut files: Vec<serde_json::Value> = vec![];
        for yaml in yamls {
            let path = yaml.to_string_lossy();
            let file = File::open(yaml).context(format!("failed to open file {path}"))?;
            files.push(
                serde_saphyr::from_reader(file).context(format!("failed to read file {path}"))?,
            );
        }

        let data = serde_saphyr::to_string_multiple(&files)
            .context("failed to serialize a list of yamls")?;
        Ok(data)
    }

    async fn push_oci_archive_layer(
        &self,
        path: PathBuf,
        layers: Arc<Mutex<Vec<OciDescriptor>>>,
    ) -> anyhow::Result<()> {
        if path.is_dir() {
            return anyhow::Result::Ok(());
        }
        let archive_path = path.clone();
        let archive_path = archive_path
            .to_str()
            .ok_or(anyhow::anyhow!("file path is not convertable to string"))?;
        let mut file = File::open(path).context(format!("failed to open file {archive_path}"))?;
        let mut data = String::new();
        File::read_to_string(&mut file, &mut data)
            .context(format!("failed to read file {archive_path}"))?;

        self.push_oci_layer(archive_path.to_string(), data, layers)
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::{
        env,
        fs::{self, File},
        io::Read,
    };

    use chrono::Utc;
    use flate2::read::GzDecoder;
    use tar::Archive as TarArchive;
    use tempfile::TempDir;

    use crate::{
        cli::DEFAULT_OCI_BUFFER_SIZE,
        gather::{
            config::Secrets,
            report::{RunIdentity, RunInputs, RunReport},
            representation::ArchivePath,
            writer::Representation,
        },
    };

    use super::{Archive, Encoding, Writer};

    #[tokio::test]
    async fn test_new_gzip() {
        let tmp_dir = TempDir::new().expect("failed to create temp dir");
        let archive = tmp_dir.path().join("test.tar.gz");
        let archive = Archive::new(archive);
        let result = Writer::new(
            &archive,
            &Encoding::Gzip,
            None,
            None,
            DEFAULT_OCI_BUFFER_SIZE,
        );

        assert!(result.await.is_ok());
    }

    #[tokio::test]
    async fn test_new_zip() {
        let tmp_dir = TempDir::new().expect("failed to create temp dir");
        let archive = tmp_dir.path().join("test.zip");
        let archive = Archive::new(archive);
        let result = Writer::new(
            &archive,
            &Encoding::Zip,
            None,
            None,
            DEFAULT_OCI_BUFFER_SIZE,
        );

        assert!(result.await.is_ok());
    }

    #[tokio::test]
    async fn test_add_gzip() {
        let tmp_dir = TempDir::new().expect("failed to create temp dir");
        let archive = tmp_dir.path().join("test");
        let mut writer = Writer::new(
            &Archive::new(archive.clone()),
            &Encoding::Gzip,
            None,
            None,
            DEFAULT_OCI_BUFFER_SIZE,
        )
        .await
        .unwrap();

        let repr = Representation::new()
            .with_data("content")
            .with_path(ArchivePath::Custom("test.txt".into()));

        assert!(writer.store(&repr).await.is_ok());
        assert!(writer.finish_gzip().is_ok());
        assert!(archive.with_file_name("test.tar.gz").exists());

        let file = File::open(archive.with_file_name("test.tar.gz")).unwrap();
        let mut tar_archive = TarArchive::new(GzDecoder::new(file));
        let entries = tar_archive
            .entries()
            .unwrap()
            .map(|entry| entry.unwrap().path().unwrap().to_string_lossy().to_string())
            .collect::<Vec<_>>();
        assert!(
            entries.iter().any(|path| path.ends_with("test.txt")),
            "missing test.txt entry in {:?}",
            entries
        );
        let file = File::open(archive.with_file_name("test.tar.gz")).unwrap();
        let mut tar_archive = TarArchive::new(GzDecoder::new(file));
        let mut data = String::new();
        tar_archive
            .entries()
            .unwrap()
            .find_map(|entry| {
                let mut entry = entry.ok()?;
                let path = entry.path().ok()?.to_path_buf();
                if path.to_string_lossy().ends_with("test.txt") {
                    entry.read_to_string(&mut data).ok()?;
                    Some(())
                } else {
                    None
                }
            })
            .expect("test.txt contents");
        assert_eq!(data, "content");
    }

    #[tokio::test]
    async fn test_add_zip() {
        use std::{
            fs::File,
            io::{Read, Seek},
        };

        use crate::gather::representation::ArchivePath;

        unsafe {
            env::set_var("SECRET", "secret");
        }

        let tmp_dir = TempDir::new().expect("failed to create temp dir");
        let archive = tmp_dir.path().join("test.zip");
        let mut writer = Writer::new(
            &Archive::new(archive.clone()),
            &Encoding::Zip,
            None,
            None,
            DEFAULT_OCI_BUFFER_SIZE,
        )
        .await
        .unwrap();

        let repr = Representation::new()
            .with_path(ArchivePath::Custom("test.txt".into()))
            .with_data("content with secret");

        let secret: Secrets = vec!["SECRET".into()].into();
        assert!(writer.store(&secret.strip(&repr)).await.is_ok());
        assert!(writer.finish_zip().is_ok());
        assert!(archive.exists());

        fn check_zip_contents(reader: impl Read + Seek) {
            let mut zip = zip::ZipArchive::new(reader).unwrap();
            let mut file = zip.by_name("test/test.txt").unwrap();

            let mut data = String::new();
            file.read_to_string(&mut data).unwrap();
            assert_eq!(data, "content with xxx");
        }

        check_zip_contents(File::open(archive).unwrap());
    }

    #[tokio::test]
    async fn test_add_path() {
        unsafe { env::set_var("SECRET", "secret") };

        let tmp_dir = TempDir::new().expect("failed to create temp dir");
        let archive = tmp_dir.path().join("cluster1/collected");
        let mut writer = Writer::new(
            &Archive::new(archive.clone()),
            &Encoding::Path,
            None,
            None,
            DEFAULT_OCI_BUFFER_SIZE,
        )
        .await
        .unwrap();

        let repr = Representation::new()
            .with_data("content with secret")
            .with_path(ArchivePath::Custom("test.txt".into()));

        let secret: Secrets = vec!["SECRET".into()].into();
        assert!(writer.store(&secret.strip(&repr)).await.is_ok());
        assert!(archive.exists());
        assert!(archive.join("test.txt").exists());
        let data = fs::read_to_string(archive.join("test.txt")).unwrap();
        assert_eq!(data, "content with xxx");
    }

    #[tokio::test]
    async fn test_store_bytes_zip() {
        let tmp_dir = TempDir::new().expect("failed to create temp dir");
        let archive = tmp_dir.path().join("binary.zip");
        let mut writer = Writer::new(
            &Archive::new(archive.clone()),
            &Encoding::Zip,
            None,
            None,
            DEFAULT_OCI_BUFFER_SIZE,
        )
        .await
        .unwrap();

        writer
            .store_bytes(
                ArchivePath::Custom("snapshot.sqlite".into()),
                [1_u8, 2, 3, 4],
            )
            .await
            .unwrap();
        writer.finish_zip().unwrap();

        let mut zip = zip::ZipArchive::new(File::open(archive).unwrap()).unwrap();
        let mut file = zip.by_name("binary/snapshot.sqlite").unwrap();
        let mut data = vec![];
        file.read_to_end(&mut data).unwrap();
        assert_eq!(data, vec![1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn test_write_agent_artifacts_path() {
        let tmp_dir = TempDir::new().expect("failed to create temp dir");
        let archive = tmp_dir.path().join("collected");
        let mut writer = Writer::new(
            &Archive::new(archive.clone()),
            &Encoding::Path,
            None,
            None,
            DEFAULT_OCI_BUFFER_SIZE,
        )
        .await
        .unwrap();

        writer
            .store(
                &Representation::new()
                    .with_path(ArchivePath::Namespaced(
                        "namespaces/default/v1/pod/web-123.yaml".into(),
                    ))
                    .with_data(
                        r#"
apiVersion: v1
kind: Pod
metadata:
  name: web-123
  namespace: default
spec:
  nodeName: worker-1
  serviceAccountName: web-sa
  containers:
    - name: web
      image: nginx:1.27
"#,
                    ),
            )
            .await
            .unwrap();
        writer
            .store(
                &Representation::new()
                    .with_path(ArchivePath::Logs(
                        "namespaces/default/v1/pod/web-123/web/current.log".into(),
                    ))
                    .with_data("INFO boot\nWARN cache miss\n"),
            )
            .await
            .unwrap();

        let report = RunReport {
            identity: RunIdentity::default(),
            inputs: RunInputs::default(),
            started_at: Utc::now(),
            finished_at: Some(Utc::now()),
            duration_ms: Some(10),
            success: true,
            totals: Default::default(),
            stats: Default::default(),
            warnings: vec![],
            failures: vec![],
        };

        writer
            .write_agent_artifacts(&report, &report.stats, &report.failures, &report.warnings)
            .await
            .unwrap();

        assert!(archive.join("AGENT-START.md").is_file());
        assert!(archive.join("resource-index.jsonl").is_file());
        assert!(archive.join("relation-index.jsonl").is_file());
        assert!(archive.join("log-index.jsonl").is_file());
        assert!(archive.join("snapshot.sqlite").is_file());
        assert!(
            fs::read(archive.join("snapshot.sqlite"))
                .unwrap()
                .starts_with(b"SQLite format 3")
        );
    }

    #[tokio::test]
    async fn test_try_into_nested_file_success() {
        let tmp_dir = TempDir::new().expect("failed to create temp dir");
        let tmp_dir = tmp_dir.path();
        Writer::new(
            &Archive::new(tmp_dir.join("nested/output.zip")),
            &Encoding::Zip,
            None,
            None,
            DEFAULT_OCI_BUFFER_SIZE,
        )
        .await
        .unwrap();

        assert!(tmp_dir.join("nested/output.zip").exists());
    }

    #[tokio::test]
    async fn test_try_into_writer_empty_path() {
        assert!(
            Writer::new(
                &Archive::new("".into()),
                &Encoding::Zip,
                None,
                None,
                DEFAULT_OCI_BUFFER_SIZE,
            )
            .await
            .is_err()
        );
    }
}
