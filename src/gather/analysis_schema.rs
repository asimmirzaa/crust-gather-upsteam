use serde::{Deserialize, Serialize};

use super::report::RunIdentity;

pub const CURRENT_ANALYSIS_SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct AnalysisFeatures {
    pub resource_index: bool,
    pub relation_index: bool,
    pub log_index: bool,
    pub snapshot_sqlite: bool,
    pub agent_start: bool,
}

impl Default for AnalysisFeatures {
    fn default() -> Self {
        Self {
            resource_index: true,
            relation_index: true,
            log_index: true,
            snapshot_sqlite: true,
            agent_start: true,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct AnalysisSchema {
    pub schema_version: u32,
    pub collector_version: String,
    pub collector_revision: String,
    pub features: AnalysisFeatures,
}

impl AnalysisSchema {
    pub fn current(identity: &RunIdentity) -> Self {
        Self {
            schema_version: CURRENT_ANALYSIS_SCHEMA_VERSION,
            collector_version: identity.collector_version.clone(),
            collector_revision: identity.collector_revision.clone(),
            features: AnalysisFeatures::default(),
        }
    }

    pub fn ensure_supported(&self) -> anyhow::Result<()> {
        if self.schema_version != CURRENT_ANALYSIS_SCHEMA_VERSION {
            anyhow::bail!(
                "unsupported analysis schema version {} (expected {})",
                self.schema_version,
                CURRENT_ANALYSIS_SCHEMA_VERSION
            );
        }

        Ok(())
    }
}
