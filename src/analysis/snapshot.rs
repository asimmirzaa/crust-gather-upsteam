use std::{
    collections::{BTreeMap, HashMap},
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, bail};
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::Value;

use crate::gather::{
    agent_artifacts::{LogIndexEntry, RelationIndexEntry, ResourceIndexEntry},
    analysis_schema::AnalysisSchema,
    report::{CollectorStats, RunMessage, RunReport},
};

use super::source::SnapshotWorkspace;

#[derive(Clone, Debug, Deserialize)]
pub struct AppVersionEntry {
    pub name: String,
    pub namespace: String,
    pub container: String,
    pub container_type: String,
    pub version: String,
}

#[derive(Debug)]
pub struct Snapshot {
    workspace: SnapshotWorkspace,
    pub schema: AnalysisSchema,
    pub report: RunReport,
    pub stats: BTreeMap<String, CollectorStats>,
    pub failures: Vec<RunMessage>,
    pub warnings: Vec<RunMessage>,
    pub resources: Vec<ResourceIndexEntry>,
    pub relations: Vec<RelationIndexEntry>,
    pub logs: Vec<LogIndexEntry>,
    pub app_versions: Vec<AppVersionEntry>,
    resource_by_id: HashMap<String, usize>,
    resource_by_path: HashMap<String, usize>,
}

impl Snapshot {
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let workspace = SnapshotWorkspace::open(path)?;
        Self::from_workspace(workspace)
    }

    pub fn from_workspace(workspace: SnapshotWorkspace) -> anyhow::Result<Self> {
        let root = workspace.root();
        let schema: AnalysisSchema = read_yaml(root.join("analysis-schema.yaml"))?;
        schema.ensure_supported()?;
        let report: RunReport = read_yaml(root.join("run-report.yaml"))?;
        let stats: BTreeMap<String, CollectorStats> = read_yaml(root.join("run-stats.yaml"))?;
        let failures: Vec<RunMessage> = read_yaml(root.join("run-failures.yaml"))?;
        let warnings: Vec<RunMessage> = read_yaml(root.join("run-warnings.yaml"))?;
        let resources: Vec<ResourceIndexEntry> = read_jsonl(root.join("resource-index.jsonl"))?;
        let relations: Vec<RelationIndexEntry> = read_jsonl(root.join("relation-index.jsonl"))?;
        let logs: Vec<LogIndexEntry> = read_jsonl(root.join("log-index.jsonl"))?;
        let app_versions: Vec<AppVersionEntry> =
            read_optional_yaml(root.join("app-versions.yaml"))?.unwrap_or_default();

        let resource_by_id = resources
            .iter()
            .enumerate()
            .map(|(index, resource)| (resource.id.clone(), index))
            .collect::<HashMap<_, _>>();
        let resource_by_path = resources
            .iter()
            .enumerate()
            .map(|(index, resource)| (resource.path.clone(), index))
            .collect::<HashMap<_, _>>();

        Ok(Self {
            workspace,
            schema,
            report,
            stats,
            failures,
            warnings,
            resources,
            relations,
            logs,
            app_versions,
            resource_by_id,
            resource_by_path,
        })
    }

    pub fn root(&self) -> &Path {
        self.workspace.root()
    }

    pub fn resource_path(&self, relative_path: &str) -> PathBuf {
        self.root().join(relative_path)
    }

    pub fn resource_by_id(&self, id: &str) -> Option<&ResourceIndexEntry> {
        self.resource_by_id
            .get(id)
            .map(|index| &self.resources[*index])
    }

    pub fn resource_by_path(&self, path: &str) -> Option<&ResourceIndexEntry> {
        self.resource_by_path
            .get(path)
            .map(|index| &self.resources[*index])
    }

    pub fn load_resource_value(&self, resource: &ResourceIndexEntry) -> anyhow::Result<Value> {
        self.load_resource_value_by_path(resource.path.as_str())
    }

    pub fn load_resource_value_by_path(&self, path: &str) -> anyhow::Result<Value> {
        let payload = fs::read_to_string(self.resource_path(path))
            .with_context(|| format!("failed to read resource file {path}"))?;
        serde_yaml::from_str::<Value>(&payload)
            .with_context(|| format!("failed to parse resource YAML {path}"))
    }

    pub fn ensure_successful(&self) -> anyhow::Result<()> {
        if self.report.success {
            return Ok(());
        }

        bail!(
            "snapshot run was partial or failed: {} failure(s), {} warning(s)",
            self.report.failures.len(),
            self.report.warnings.len()
        )
    }
}

fn read_yaml<T: DeserializeOwned>(path: PathBuf) -> anyhow::Result<T> {
    let payload =
        fs::read_to_string(&path).with_context(|| format!("failed to read {}", path.display()))?;
    serde_yaml::from_str(&payload).with_context(|| format!("failed to parse {}", path.display()))
}

fn read_optional_yaml<T: DeserializeOwned>(path: PathBuf) -> anyhow::Result<Option<T>> {
    if !path.is_file() {
        return Ok(None);
    }

    read_yaml(path).map(Some)
}

fn read_jsonl<T: DeserializeOwned>(path: PathBuf) -> anyhow::Result<Vec<T>> {
    let payload =
        fs::read_to_string(&path).with_context(|| format!("failed to read {}", path.display()))?;
    payload
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).map_err(anyhow::Error::from))
        .collect()
}

#[cfg(test)]
mod tests {
    use crate::analysis::test_support::sample_snapshot;

    use super::Snapshot;

    #[test]
    fn loads_snapshot_models_and_indexes() {
        let fixture = sample_snapshot("snapshot-load").expect("fixture");
        let snapshot = Snapshot::open(fixture.root()).expect("snapshot");

        assert!(snapshot.report.success);
        assert_eq!(snapshot.schema.schema_version, 1);
        assert_eq!(snapshot.resources.len(), 11);
        assert_eq!(snapshot.relations.len(), 4);
        assert_eq!(snapshot.logs.len(), 3);
        assert_eq!(snapshot.app_versions.len(), 2);
        assert!(snapshot.resource_by_id("v1/Pod/default/web-abc").is_some());
    }

    #[test]
    fn loads_raw_resource_yaml() {
        let fixture = sample_snapshot("snapshot-resource-read").expect("fixture");
        let snapshot = Snapshot::open(fixture.root()).expect("snapshot");
        let resource = snapshot
            .resource_by_id("v1/Pod/default/web-abc")
            .expect("resource");
        let value = snapshot.load_resource_value(resource).expect("value");

        assert_eq!(value["kind"], "Pod");
        assert_eq!(value["metadata"]["name"], "web-abc");
    }
}
