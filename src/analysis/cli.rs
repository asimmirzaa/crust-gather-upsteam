use std::{fs, path::PathBuf};

use anyhow::Context;
use clap::{Args, ValueEnum};
use serde::Serialize;

#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, ValueEnum)]
pub enum AnalysisFormat {
    #[default]
    Markdown,
    Json,
}

#[derive(Args, Clone, Debug)]
pub struct SnapshotInput {
    #[arg(long, value_name = "SNAPSHOT")]
    pub snapshot: PathBuf,
}

#[derive(Args, Clone, Debug)]
pub struct ComparisonInput {
    #[arg(long, value_name = "SNAPSHOT")]
    pub before: PathBuf,

    #[arg(long, value_name = "SNAPSHOT")]
    pub after: PathBuf,
}

#[derive(Args, Clone, Debug)]
pub struct OutputOptions {
    #[arg(long, default_value_t, value_enum)]
    pub format: AnalysisFormat,

    #[arg(long, value_name = "PATH")]
    pub output: Option<PathBuf>,
}

pub fn emit_text(output: Option<&PathBuf>, text: &str) -> anyhow::Result<()> {
    match output {
        Some(path) => {
            if let Some(parent) = path.parent().filter(|value| !value.as_os_str().is_empty()) {
                fs::create_dir_all(parent)?;
            }
            fs::write(path, text).with_context(|| format!("failed to write {}", path.display()))?;
        }
        None => {
            print!("{text}");
        }
    }

    Ok(())
}

pub fn emit_json<T: Serialize>(output: Option<&PathBuf>, value: &T) -> anyhow::Result<()> {
    let text = serde_json::to_string_pretty(value)?;
    emit_text(output, format!("{text}\n").as_str())
}
