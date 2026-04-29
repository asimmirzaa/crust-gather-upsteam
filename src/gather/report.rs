use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::cli::Filters;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunIdentity {
    pub collector_name: String,
    pub collector_version: String,
    pub collector_revision: String,
}

impl Default for RunIdentity {
    fn default() -> Self {
        Self {
            collector_name: env!("CARGO_PKG_NAME").to_string(),
            collector_version: env!("CARGO_PKG_VERSION").to_string(),
            collector_revision: env!("CRUST_GATHER_REVISION").to_string(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InputLog {
    pub name: String,
    pub command: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunInputs {
    pub mode: String,
    pub source: String,
    pub context: Option<String>,
    pub output_path: String,
    pub output_encoding: String,
    pub oci_reference: Option<String>,
    pub clean_output: bool,
    pub duration: String,
    pub list_page_limit: u32,
    pub collect_concurrency: usize,
    pub log_collect_concurrency: usize,
    pub node_log_mode: String,
    pub debug_pod_image: Option<String>,
    pub systemd_units: Vec<String>,
    pub additional_logs: Vec<InputLog>,
    pub filters: Vec<Filters>,
    pub secret_env_names: Vec<String>,
    pub secrets_file: Option<String>,
}

impl Default for RunInputs {
    fn default() -> Self {
        Self {
            mode: "collect".to_string(),
            source: "test".to_string(),
            context: None,
            output_path: "crust-gather".to_string(),
            output_encoding: "path".to_string(),
            oci_reference: None,
            clean_output: false,
            duration: "1m".to_string(),
            list_page_limit: 100,
            collect_concurrency: 32,
            log_collect_concurrency: 8,
            node_log_mode: "deep".to_string(),
            debug_pod_image: None,
            systemd_units: vec![],
            additional_logs: vec![],
            filters: vec![],
            secret_env_names: vec![],
            secrets_file: None,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CollectorStats {
    pub listed_objects: usize,
    pub collected_objects: usize,
    pub written_files: usize,
    pub failed_objects: usize,
    pub warnings: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunMessage {
    pub timestamp: DateTime<Utc>,
    pub phase: String,
    pub collector: String,
    pub object: Option<String>,
    pub message: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RunTotals {
    pub collectors: usize,
    pub listed_objects: usize,
    pub collected_objects: usize,
    pub written_files: usize,
    pub failed_objects: usize,
    pub warnings: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunReport {
    pub identity: RunIdentity,
    pub inputs: RunInputs,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub duration_ms: Option<i64>,
    pub success: bool,
    pub totals: RunTotals,
    pub stats: BTreeMap<String, CollectorStats>,
    pub warnings: Vec<RunMessage>,
    pub failures: Vec<RunMessage>,
}

impl RunReport {
    pub fn new(identity: RunIdentity, inputs: RunInputs, started_at: DateTime<Utc>) -> Self {
        Self {
            identity,
            inputs,
            started_at,
            finished_at: None,
            duration_ms: None,
            success: false,
            totals: RunTotals::default(),
            stats: BTreeMap::new(),
            warnings: vec![],
            failures: vec![],
        }
    }
}

#[derive(Clone, Debug)]
pub struct RunReportState {
    report: RunReport,
}

impl RunReportState {
    pub fn new(inputs: RunInputs) -> Self {
        Self {
            report: RunReport::new(RunIdentity::default(), inputs, Utc::now()),
        }
    }

    pub fn record_listed(&mut self, collector: &str, count: usize) {
        self.report
            .stats
            .entry(collector.to_string())
            .or_default()
            .listed_objects += count;
    }

    pub fn record_success(
        &mut self,
        collector: &str,
        collected_objects: usize,
        written_files: usize,
    ) {
        let stats = self.report.stats.entry(collector.to_string()).or_default();
        stats.collected_objects += collected_objects;
        stats.written_files += written_files;
    }

    pub fn record_failure(
        &mut self,
        phase: impl Into<String>,
        collector: impl Into<String>,
        object: Option<String>,
        message: impl Into<String>,
    ) {
        let collector = collector.into();
        self.report
            .stats
            .entry(collector.clone())
            .or_default()
            .failed_objects += 1;
        self.report.failures.push(RunMessage {
            timestamp: Utc::now(),
            phase: phase.into(),
            collector,
            object,
            message: message.into(),
        });
    }

    pub fn record_warning(
        &mut self,
        phase: impl Into<String>,
        collector: impl Into<String>,
        object: Option<String>,
        message: impl Into<String>,
    ) {
        let collector = collector.into();
        self.report
            .stats
            .entry(collector.clone())
            .or_default()
            .warnings += 1;
        self.report.warnings.push(RunMessage {
            timestamp: Utc::now(),
            phase: phase.into(),
            collector,
            object,
            message: message.into(),
        });
    }

    pub fn finalize(&mut self, success: bool) {
        let finished_at = Utc::now();
        let duration_ms = finished_at
            .signed_duration_since(self.report.started_at)
            .num_milliseconds();

        self.report.finished_at = Some(finished_at);
        self.report.duration_ms = Some(duration_ms.max(0));
        self.report.success = success;
        self.report.totals = self.totals();
    }

    pub fn report(&self) -> &RunReport {
        &self.report
    }

    pub fn stats(&self) -> &BTreeMap<String, CollectorStats> {
        &self.report.stats
    }

    pub fn failures(&self) -> &[RunMessage] {
        &self.report.failures
    }

    pub fn warnings(&self) -> &[RunMessage] {
        &self.report.warnings
    }

    fn totals(&self) -> RunTotals {
        let mut totals = RunTotals {
            collectors: self.report.stats.len(),
            ..Default::default()
        };

        for stats in self.report.stats.values() {
            totals.listed_objects += stats.listed_objects;
            totals.collected_objects += stats.collected_objects;
            totals.written_files += stats.written_files;
            totals.failed_objects += stats.failed_objects;
            totals.warnings += stats.warnings;
        }

        totals
    }
}

impl Default for RunReportState {
    fn default() -> Self {
        Self::new(RunInputs::default())
    }
}
