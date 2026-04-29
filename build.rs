use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=.git/HEAD");
    if let Some(head_ref) = git_head_ref() {
        println!("cargo:rerun-if-changed=.git/{head_ref}");
    }
    println!("cargo:rerun-if-changed=.git/packed-refs");
    println!("cargo:rerun-if-env-changed=CRUST_GATHER_REVISION");

    let revision = std::env::var("CRUST_GATHER_REVISION")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(git_revision)
        .unwrap_or_else(|| "unknown".to_string());

    println!("cargo:rustc-env=CRUST_GATHER_REVISION={revision}");
}

fn git_revision() -> Option<String> {
    let output = Command::new("git")
        .args(["rev-parse", "--short=12", "HEAD"])
        .output()
        .ok()?;

    if !output.status.success() {
        return None;
    }

    let revision = String::from_utf8(output.stdout).ok()?;
    let revision = revision.trim();
    (!revision.is_empty()).then(|| revision.to_string())
}

fn git_head_ref() -> Option<String> {
    let head = std::fs::read_to_string(".git/HEAD").ok()?;
    head.strip_prefix("ref: ")
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}
