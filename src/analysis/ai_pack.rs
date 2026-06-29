use std::{fs, path::Path};

use anyhow::Context;
use clap::Args;
use serde::Serialize;

use super::{
    audit,
    cli::{AnalysisFormat, OutputOptions, SnapshotInput, emit_json, emit_text},
    graph, summary,
};

#[derive(Clone, Debug, Serialize)]
pub struct AiSuspect {
    pub reason: String,
    pub object: String,
    pub path: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct AiPack {
    pub summary: summary::SummaryReport,
    pub audit: audit::AuditReport,
    pub suspects: Vec<AiSuspect>,
    pub recommended_paths: Vec<String>,
    pub next_questions: Vec<String>,
    pub graph: graph::GraphReport,
}

#[derive(Clone, Debug, Args)]
pub struct AiPackCommand {
    #[command(flatten)]
    pub input: SnapshotInput,

    #[command(flatten)]
    pub output: OutputOptions,
}

pub fn build(snapshot: &super::snapshot::Snapshot) -> anyhow::Result<AiPack> {
    let summary = summary::build(snapshot)?;
    let audit = audit::build(snapshot)?;
    let graph = graph::build(snapshot, None)?;

    let mut suspects = vec![];
    for node in &summary.non_ready_nodes {
        suspects.push(AiSuspect {
            reason: "node not ready".into(),
            object: node.object.id.clone(),
            path: node.object.path.clone(),
        });
    }
    for pod in summary.pod_hotspots.iter().take(5) {
        suspects.push(AiSuspect {
            reason: format!("pod hotspot with {} restarts", pod.restart_count),
            object: pod.object.id.clone(),
            path: pod.object.path.clone(),
        });
    }
    for finding in audit.findings.iter().take(10) {
        if let Some(object) = &finding.object {
            suspects.push(AiSuspect {
                reason: format!("{} ({})", finding.title, finding.rule_id),
                object: object.id.clone(),
                path: object.path.clone(),
            });
        }
    }
    suspects.sort_by(|left, right| {
        left.object
            .cmp(&right.object)
            .then_with(|| left.reason.cmp(&right.reason))
    });
    suspects.dedup_by(|left, right| left.object == right.object && left.reason == right.reason);

    let mut recommended_paths = suspects
        .iter()
        .map(|suspect| suspect.path.clone())
        .collect::<Vec<_>>();
    recommended_paths.extend(
        summary
            .log_hotspots
            .iter()
            .map(|hotspot| hotspot.path.clone()),
    );
    recommended_paths.sort();
    recommended_paths.dedup();
    recommended_paths.truncate(20);

    let next_questions = vec![
        "Which failing workloads are externally exposed?".into(),
        "Did resource drift or image drift precede the incident?".into(),
        "Do warnings or failures indicate partial snapshot coverage?".into(),
        "Which suspect pods and nodes have the highest error-bearing logs?".into(),
    ];

    Ok(AiPack {
        summary,
        audit,
        suspects,
        recommended_paths,
        next_questions,
        graph,
    })
}

pub fn render_markdown(pack: &AiPack) -> String {
    let mut out = String::new();
    out.push_str("# AI Pack\n\n");
    out.push_str(&format!(
        "- success: {}\n- warnings: {}\n- failures: {}\n- suspects: {}\n\n",
        pack.summary.success,
        pack.summary.warnings,
        pack.summary.failures,
        pack.suspects.len()
    ));

    out.push_str("## Top Suspects\n\n");
    if pack.suspects.is_empty() {
        out.push_str("- none\n\n");
    } else {
        for suspect in &pack.suspects {
            out.push_str(&format!("- `{}`: {}\n", suspect.object, suspect.reason));
        }
        out.push('\n');
    }

    out.push_str("## High-Signal Paths\n\n");
    for path in &pack.recommended_paths {
        out.push_str(&format!("- `{path}`\n"));
    }
    out.push('\n');

    out.push_str("## Next Questions\n\n");
    for question in &pack.next_questions {
        out.push_str(&format!("- {question}\n"));
    }
    out.push('\n');

    out.push_str("## Summary Snapshot\n\n");
    out.push_str(&summary::render_markdown(&pack.summary));
    out.push('\n');

    out.push_str("## Audit Snapshot\n\n");
    let critical = pack
        .audit
        .findings
        .iter()
        .filter(|finding| {
            matches!(
                finding.severity,
                audit::Severity::Critical | audit::Severity::High
            )
        })
        .collect::<Vec<_>>();
    if critical.is_empty() {
        out.push_str("- no high-severity findings\n\n");
    } else {
        for finding in critical {
            out.push_str(&format!("- `{}` {}\n", finding.rule_id, finding.title));
        }
        out.push('\n');
    }

    out.push_str("## Graph\n\n```mermaid\n");
    out.push_str(&graph::render_mermaid(&pack.graph));
    out.push_str("\n```\n");
    out
}

pub fn run(command: AiPackCommand) -> anyhow::Result<()> {
    let snapshot = super::snapshot::Snapshot::open(command.input.snapshot)?;
    let pack = build(&snapshot)?;

    if let Some(output) = command.output.output.as_ref() {
        write_output_bundle(output, &pack)?;
        return Ok(());
    }

    match command.output.format {
        AnalysisFormat::Markdown => emit_text(None, render_markdown(&pack).as_str()),
        AnalysisFormat::Json => emit_json(None, &pack),
    }
}

fn write_output_bundle(path: &Path, pack: &AiPack) -> anyhow::Result<()> {
    fs::create_dir_all(path).with_context(|| format!("failed to create {}", path.display()))?;
    fs::write(path.join("AI-PACK.md"), render_markdown(pack))
        .with_context(|| format!("failed to write {}", path.join("AI-PACK.md").display()))?;
    fs::write(
        path.join("ai-pack.json"),
        format!("{}\n", serde_json::to_string_pretty(pack)?),
    )
    .with_context(|| format!("failed to write {}", path.join("ai-pack.json").display()))?;
    fs::write(path.join("graph.mmd"), graph::render_mermaid(&pack.graph))
        .with_context(|| format!("failed to write {}", path.join("graph.mmd").display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use crate::analysis::{snapshot::Snapshot, test_support::sample_snapshot};

    use super::{build, render_markdown, write_output_bundle};

    #[test]
    fn ai_pack_collects_suspects_and_paths() {
        let fixture = sample_snapshot("ai-pack").expect("fixture");
        let snapshot = Snapshot::open(fixture.root()).expect("snapshot");
        let pack = build(&snapshot).expect("pack");

        assert!(!pack.suspects.is_empty());
        assert!(
            pack.recommended_paths
                .iter()
                .any(|path| path.contains("web-abc"))
        );
    }

    #[test]
    fn ai_pack_writes_bundle_files() {
        let fixture = sample_snapshot("ai-pack-output").expect("fixture");
        let snapshot = Snapshot::open(fixture.root()).expect("snapshot");
        let pack = build(&snapshot).expect("pack");
        let output = TempDir::new().expect("output");

        write_output_bundle(output.path(), &pack).expect("bundle");

        assert!(output.path().join("AI-PACK.md").is_file());
        assert!(output.path().join("ai-pack.json").is_file());
        assert!(output.path().join("graph.mmd").is_file());
        assert!(render_markdown(&pack).contains("Top Suspects"));
    }
}
