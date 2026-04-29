use anyhow::Context;
use backon::{ExponentialBuilder, Retryable};
use flate2::{Compression, write::GzEncoder};

use chrono::Utc;
use futures::{StreamExt as _, TryStreamExt as _, future, stream};
use json_patch::diff;
use k8s_openapi::serde_json;
use oci_client::{
    Client, Reference,
    client::{ClientConfig, Config},
    errors::OciDistributionError,
    manifest::{IMAGE_LAYER_MEDIA_TYPE, OCI_IMAGE_MEDIA_TYPE, OciDescriptor, OciImageManifest},
    secrets::RegistryAuth,
};
use serde::Deserialize;
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
    Oci(Archive, Client, Box<Reference>, RegistryAuth, usize),
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
        let WriterSink::Oci(archive, client, image_ref, auth, buffer_size) = &self.sink else {
            return Ok(());
        };

        info!("Pushing image: {:?}", image_ref);
        client
            .store_auth_if_needed(image_ref.resolve_registry(), auth)
            .await;

        let config = Config::new(b"{}".to_vec(), OCI_IMAGE_MEDIA_TYPE.to_string(), None);
        let paths = glob::glob(&format!(
            "{}/**/*",
            archive
                .path()
                .to_str()
                .ok_or(anyhow::anyhow!("archive path is not convertable to string"))?,
        ))?;

        let layers = Arc::new(Mutex::new(vec![]));
        // Upload layers
        stream::iter(paths.into_iter())
            .map(|path| {
                let layers = layers.clone();
                async move {
                    let path = path?;
                    if path.is_dir() {
                        return anyhow::Result::Ok(());
                    }
                    let archive_path = path.clone();
                    let archive_path = archive_path
                        .to_str()
                        .ok_or(anyhow::anyhow!("file path is not convertable to string"))?;
                    let mut file =
                        File::open(path).context(format!("failed to open file {archive_path}"))?;
                    let mut data = vec![];
                    File::read_to_end(&mut file, &mut data)
                        .context(format!("failed to read file {archive_path}"))?;
                    if data.is_empty() {
                        // That could only happen for empty logs, so we publish an empty json instead
                        // as ghcr doesn't allow empty layers
                        data = b"{}".to_vec();
                    };
                    let size = data.len() as i64;
                    let digest = format!(
                        "sha256:{}",
                        hex::encode(sha2::Sha256::digest(&data))
                    );
                    {
                        layers.lock().await.push(OciDescriptor {
                            urls: None,
                            media_type: IMAGE_LAYER_MEDIA_TYPE.to_string(),
                            digest: digest.clone(),
                            size,
                            annotations: Some(
                                [(
                                    "org.opencontainers.image.title".to_string(),
                                    archive_path.to_string(),
                                )]
                                .into(),
                            ),
                        });
                    }

                    info!("Pushing layer: {:?}", archive_path);
                    let push = || client.push_blob(image_ref, data.clone(), &digest);
                    push.retry(ExponentialBuilder::default().with_max_times(20).with_max_delay(Duration::from_secs(10)).with_jitter())
                        .when(|e| matches!(e, OciDistributionError::ServerError { code, .. } if *code == 429 ))
                        .notify(|e, dur| debug!("Pushing layer: {archive_path:?} - retry after {dur:?} due to {e:?}"))
                        .await
                        .context(format!("failed to push layer for file {archive_path}"))?;

                    anyhow::Result::Ok(())
                }
            })
            .boxed() // Workaround to rustc issue https://github.com/rust-lang/rust/issues/104382
            .buffer_unordered(*buffer_size)
            .try_for_each(future::ok::<(), anyhow::Error>)
            .await?;

        let mut manifest = OciImageManifest::default();
        manifest.config.media_type = config.media_type.to_string();
        manifest.config.size = config.data.len() as i64;
        manifest.config.digest =
            format!("sha256:{}", hex::encode(sha2::Sha256::digest(&config.data)));
        manifest.layers = layers.lock().await.clone();
        manifest.layers.sort_by(|a, b| a.digest.cmp(&b.digest));

        let push = || client.push_blob(image_ref, config.data.clone(), &manifest.config.digest);
        push.retry(
            ExponentialBuilder::default()
                .with_max_times(20)
                .with_max_delay(Duration::from_secs(10))
                .with_jitter(),
        )
        .when(|e| matches!(e, OciDistributionError::ServerError { code, .. } if *code == 429 ))
        .await?;

        let manifest = &manifest.into();
        let push = || client.push_manifest(image_ref, manifest);
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
            WriterSink::Path(Archive(archive)) | WriterSink::Oci(Archive(archive), ..) => {
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
                let updated = serde_yaml::from_str(repr.data())?;
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
                Encoding::Oci(image_ref) => WriterSink::Oci(
                    archive.clone(),
                    Client::new(client_config.unwrap_or_default()),
                    image_ref.clone().into(),
                    auth.unwrap_or(RegistryAuth::Anonymous),
                    buffer_size,
                ),
            },
            agent_artifacts: AgentArtifactsState::default(),
        })
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
