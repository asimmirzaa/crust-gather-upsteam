use std::{
    fs::File,
    io,
    path::{Path, PathBuf},
};

use anyhow::{Context, bail};
use flate2::read::GzDecoder;
use tar::Archive as TarArchive;
use tempfile::TempDir;
use zip::ZipArchive;

#[derive(Debug)]
pub struct SnapshotWorkspace {
    root: PathBuf,
    _tempdir: Option<TempDir>,
}

impl SnapshotWorkspace {
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref();

        if path.is_dir() {
            let root = detect_snapshot_root(path)?;
            return Ok(Self {
                root,
                _tempdir: None,
            });
        }

        if path.is_file() {
            return Self::open_archive(path);
        }

        bail!("snapshot path does not exist: {}", path.display())
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    fn open_archive(path: &Path) -> anyhow::Result<Self> {
        let tempdir = TempDir::new().context("failed to create temp workspace")?;
        let extract_root = tempdir.path().to_path_buf();
        let filename = path.file_name().and_then(|value| value.to_str()).unwrap_or("");

        if filename.ends_with(".tar.gz") || filename.ends_with(".tgz") {
            extract_tar_gz(path, &extract_root)?;
        } else if filename.ends_with(".tar") {
            extract_tar(path, &extract_root)?;
        } else if filename.ends_with(".zip") {
            extract_zip(path, &extract_root)?;
        } else {
            bail!(
                "unsupported snapshot input format for {}",
                path.to_string_lossy()
            );
        }

        let root = detect_snapshot_root(&extract_root)?;
        Ok(Self {
            root,
            _tempdir: Some(tempdir),
        })
    }
}

fn detect_snapshot_root(path: &Path) -> anyhow::Result<PathBuf> {
    if is_snapshot_root(path) {
        return Ok(path.to_path_buf());
    }

    let nested = path.join("snapshot");
    if is_snapshot_root(&nested) {
        return Ok(nested);
    }

    for candidate in std::fs::read_dir(path)
        .with_context(|| format!("failed to read {}", path.display()))?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|candidate| candidate.is_dir())
    {
        if is_snapshot_root(&candidate) {
            return Ok(candidate);
        }

        let nested = candidate.join("snapshot");
        if is_snapshot_root(&nested) {
            return Ok(nested);
        }
    }

    bail!("could not locate snapshot root under {}", path.display())
}

fn is_snapshot_root(path: &Path) -> bool {
    path.is_dir()
        && path.join("analysis-schema.yaml").is_file()
        && path.join("run-report.yaml").is_file()
        && path.join("resource-index.jsonl").is_file()
        && path.join("relation-index.jsonl").is_file()
        && path.join("log-index.jsonl").is_file()
}

fn extract_tar(path: &Path, output_dir: &Path) -> anyhow::Result<()> {
    let file = File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let mut archive = TarArchive::new(file);
    archive
        .unpack(output_dir)
        .with_context(|| format!("failed to unpack {}", path.display()))
}

fn extract_tar_gz(path: &Path, output_dir: &Path) -> anyhow::Result<()> {
    let file = File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let decoder = GzDecoder::new(file);
    let mut archive = TarArchive::new(decoder);
    archive
        .unpack(output_dir)
        .with_context(|| format!("failed to unpack {}", path.display()))
}

fn extract_zip(path: &Path, output_dir: &Path) -> anyhow::Result<()> {
    let file = File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let mut archive =
        ZipArchive::new(file).with_context(|| format!("failed to open zip {}", path.display()))?;

    for index in 0..archive.len() {
        let mut entry = archive.by_index(index)?;
        let Some(name) = entry.enclosed_name().map(|value| value.to_path_buf()) else {
            continue;
        };
        let outpath = output_dir.join(name);

        if entry.is_dir() {
            std::fs::create_dir_all(&outpath)?;
            continue;
        }

        if let Some(parent) = outpath.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let mut outfile = File::create(&outpath)?;
        io::copy(&mut entry, &mut outfile)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{fs, fs::File, io::Write};

    use flate2::{Compression, write::GzEncoder};
    use tempfile::TempDir;
    use zip::write::SimpleFileOptions;

    use crate::analysis::test_support::sample_snapshot;

    use super::SnapshotWorkspace;

    #[test]
    fn opens_extracted_snapshot_directory() {
        let fixture = sample_snapshot("source-extracted").expect("fixture");
        let workspace = SnapshotWorkspace::open(fixture.root()).expect("workspace");
        assert_eq!(workspace.root(), fixture.root());
    }

    #[test]
    fn opens_tar_gz_snapshot() {
        let fixture = sample_snapshot("source-targz").expect("fixture");
        let archive_dir = TempDir::new().expect("archive dir");
        let archive_path = archive_dir.path().join("snapshot.tar.gz");
        let file = File::create(&archive_path).expect("archive");
        let encoder = GzEncoder::new(file, Compression::default());
        let mut builder = tar::Builder::new(encoder);
        builder
            .append_dir_all("snapshot", fixture.root())
            .expect("append dir");
        let encoder = builder.into_inner().expect("encoder");
        encoder.finish().expect("finish");

        let workspace = SnapshotWorkspace::open(&archive_path).expect("workspace");
        assert!(workspace.root().join("analysis-schema.yaml").is_file());
        assert!(workspace.root().join("run-report.yaml").is_file());
    }

    #[test]
    fn opens_zip_snapshot() {
        let fixture = sample_snapshot("source-zip").expect("fixture");
        let archive_dir = TempDir::new().expect("archive dir");
        let archive_path = archive_dir.path().join("snapshot.zip");
        let file = File::create(&archive_path).expect("archive");
        let mut writer = zip::ZipWriter::new(file);
        for entry in walkdir::WalkDir::new(fixture.root()) {
            let entry = entry.expect("walkdir");
            let relative = entry
                .path()
                .strip_prefix(fixture.root().parent().expect("parent"))
                .expect("relative");
            let relative = relative.to_string_lossy().replace('\\', "/");
            if entry.file_type().is_dir() {
                writer
                    .add_directory(relative, SimpleFileOptions::default())
                    .expect("dir");
            } else {
                writer
                    .start_file(relative, SimpleFileOptions::default())
                    .expect("file");
                let bytes = fs::read(entry.path()).expect("bytes");
                writer.write_all(&bytes).expect("write");
            }
        }
        writer.finish().expect("finish");

        let workspace = SnapshotWorkspace::open(&archive_path).expect("workspace");
        assert!(workspace.root().join("resource-index.jsonl").is_file());
    }
}
