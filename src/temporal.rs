use anyhow::{Context, Result, bail};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde_json::Value;
use std::process::Command;

#[derive(Clone)]
pub struct TemporalConfig {
    pub address: String,
    pub namespace: String,
    pub api_key: Option<String>,
    pub tls: bool,
}

impl TemporalConfig {
    fn base_args(&self) -> Vec<String> {
        let mut args = vec![
            "--address".to_string(),
            self.address.clone(),
            "--namespace".to_string(),
            self.namespace.clone(),
        ];
        if let Some(ref key) = self.api_key {
            args.push("--api-key".to_string());
            args.push(key.clone());
        }
        if self.tls {
            args.push("--tls".to_string());
        }
        args
    }
}

fn workflow_id(call_id: &str) -> String {
    format!("post-processor-{call_id}")
}

fn run_temporal(config: &TemporalConfig, subcommand: &[&str], extra_args: &[&str]) -> Result<String> {
    let base = config.base_args();
    let mut cmd = Command::new("temporal");
    cmd.args(subcommand);
    cmd.args(&base);
    cmd.args(extra_args);

    let output = cmd
        .output()
        .context("Failed to execute temporal CLI. Is it installed?")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("temporal {} failed: {}", subcommand.join(" "), stderr.trim());
    }

    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

pub fn describe_workflow(call_id: &str, config: &TemporalConfig) -> Result<String> {
    let wf_id = workflow_id(call_id);
    run_temporal(
        config,
        &["workflow", "describe"],
        &["-w", &wf_id],
    )
}

pub struct WorkflowInfo {
    pub workflow_type: String,
    pub task_queue: String,
    pub input_json: String,
}

pub fn show_workflow_input(call_id: &str, config: &TemporalConfig) -> Result<WorkflowInfo> {
    let wf_id = workflow_id(call_id);
    let output = run_temporal(
        config,
        &["workflow", "show"],
        &["-w", &wf_id, "--output", "json"],
    )?;

    let history: Value = serde_json::from_str(&output)
        .context("Failed to parse workflow history JSON")?;

    let events = history
        .get("events")
        .and_then(|e| e.as_array())
        .context("No events array in history")?;

    let started = events
        .first()
        .context("Empty event history")?;

    let attrs = started
        .get("workflowExecutionStartedEventAttributes")
        .context("Missing workflowExecutionStartedEventAttributes")?;

    let workflow_type = attrs
        .get("workflowType")
        .and_then(|wt| wt.get("name"))
        .and_then(|n| n.as_str())
        .context("Missing workflowType")?
        .to_string();

    let task_queue = attrs
        .get("taskQueue")
        .and_then(|tq| tq.get("name"))
        .and_then(|n| n.as_str())
        .context("Missing taskQueue")?
        .to_string();

    let data_b64 = attrs
        .get("input")
        .and_then(|i| i.get("payloads"))
        .and_then(|p| p.as_array())
        .and_then(|a| a.first())
        .and_then(|p| p.get("data"))
        .and_then(|d| d.as_str())
        .context("Missing input payload data")?;

    let decoded_bytes = BASE64.decode(data_b64)
        .context("Failed to base64-decode input payload")?;

    let input_json = String::from_utf8(decoded_bytes)
        .context("Input payload is not valid UTF-8")?;

    let mut input_value: Value = serde_json::from_str(&input_json)
        .context("Decoded input is not valid JSON")?;

    // Overwrite these fields to true
    if let Some(obj) = input_value.as_object_mut() {
        obj.insert("overwrite_transcription".to_string(), Value::Bool(true));
        obj.insert("force_recompute_output_variables".to_string(), Value::Bool(true));
    }

    let input_json = serde_json::to_string(&input_value)?;

    Ok(WorkflowInfo {
        workflow_type,
        task_queue,
        input_json,
    })
}

pub fn start_workflow(call_id: &str, config: &TemporalConfig, info: &WorkflowInfo) -> Result<String> {
    let wf_id = workflow_id(call_id);
    run_temporal(
        config,
        &["workflow", "start"],
        &[
            "-w", &wf_id,
            "--type", &info.workflow_type,
            "--task-queue", &info.task_queue,
            "--input", &info.input_json,
            "--id-reuse-policy", "AllowDuplicate",
        ],
    )
}

pub fn reset_workflow(call_id: &str, config: &TemporalConfig, reason: &str) -> Result<String> {
    let wf_id = workflow_id(call_id);
    run_temporal(
        config,
        &["workflow", "reset"],
        &[
            "-w", &wf_id,
            "--type", "FirstWorkflowTask",
            "--reason", reason,
            "-y",
        ],
    )
}
