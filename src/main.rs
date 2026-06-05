mod csv_reader;
mod temporal;

use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use std::sync::Arc;
use temporal::TemporalConfig;
use tokio::sync::Semaphore;

#[derive(Parser)]
#[command(name = "retrigger", about = "Retrigger Temporal post-processor workflows")]
struct Cli {
    /// Temporal server address (e.g. my-ns.tmprl.cloud:7233)
    #[arg(long, env = "TEMPORAL_ADDRESS", default_value = "localhost:7233")]
    address: String,

    /// Temporal namespace
    #[arg(long, env = "TEMPORAL_NAMESPACE", default_value = "default")]
    namespace: String,

    /// API key for Temporal Cloud
    #[arg(long, env = "TEMPORAL_API_KEY")]
    api_key: Option<String>,

    /// Enable TLS (default for cloud)
    #[arg(long, env = "TEMPORAL_TLS", default_value_t = false)]
    tls: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Validate a workflow: show status and original input
    Validate {
        /// The call_id (UUID) to validate
        call_id: String,
    },
    /// Start a new workflow execution with the same input as the original
    Start {
        /// The call_id (UUID) to retrigger
        call_id: String,
    },
    /// Reset a single workflow to retrigger it
    Reset {
        /// The call_id (UUID) to reset
        call_id: String,
        /// Reason for the reset
        #[arg(long, default_value = "retrigger via CLI")]
        reason: String,
    },
    /// Batch reset workflows from CSV
    Batch {
        /// Path to CSV file with call IDs
        #[arg(long, default_value = "ah_public_calls.csv")]
        csv: PathBuf,
        /// Dry run: validate only, don't reset
        #[arg(long, default_value_t = false)]
        dry_run: bool,
        /// Max concurrent resets
        #[arg(long, default_value_t = 5)]
        concurrency: usize,
        /// Reason for the reset
        #[arg(long, default_value = "batch retrigger via CLI")]
        reason: String,
    },
}

fn build_config(cli: &Cli) -> TemporalConfig {
    TemporalConfig {
        address: cli.address.clone(),
        namespace: cli.namespace.clone(),
        api_key: cli.api_key.clone(),
        tls: cli.tls,
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let cli = Cli::parse();
    let config = build_config(&cli);

    match &cli.command {
        Commands::Validate { call_id } => cmd_validate(call_id, &config)?,
        Commands::Start { call_id } => cmd_start(call_id, &config)?,
        Commands::Reset { call_id, reason } => cmd_reset(call_id, reason, &config)?,
        Commands::Batch {
            csv,
            dry_run,
            concurrency,
            reason,
        } => {
            cmd_batch(csv, *dry_run, *concurrency, reason, &config).await?;
        }
    }

    Ok(())
}

fn cmd_validate(call_id: &str, config: &TemporalConfig) -> Result<()> {
    println!("=== Validating post-processor-{call_id} ===\n");

    println!("--- Workflow Description ---");
    match temporal::describe_workflow(call_id, config) {
        Ok(desc) => println!("{desc}"),
        Err(e) => println!("ERROR: {e}"),
    }

    println!("--- Original Input ---");
    match temporal::show_workflow_input(call_id, config) {
        Ok(info) => {
            println!("Workflow Type: {}", info.workflow_type);
            println!("Task Queue:    {}", info.task_queue);
            let v: serde_json::Value = serde_json::from_str(&info.input_json)?;
            println!("Input:\n{}", serde_json::to_string_pretty(&v)?);
        }
        Err(e) => println!("ERROR: {e}"),
    }

    Ok(())
}

fn cmd_start(call_id: &str, config: &TemporalConfig) -> Result<()> {
    println!("Fetching input for post-processor-{call_id}...");
    let info = temporal::show_workflow_input(call_id, config)?;
    println!("Workflow Type: {}", info.workflow_type);
    println!("Task Queue:    {}", info.task_queue);

    println!("\nStarting new workflow execution...");
    let output = temporal::start_workflow(call_id, config, &info)?;
    println!("OK: {}", output.trim());
    Ok(())
}

fn cmd_reset(call_id: &str, reason: &str, config: &TemporalConfig) -> Result<()> {
    println!("Resetting post-processor-{call_id}...");
    let output = temporal::reset_workflow(call_id, config, reason)?;
    println!("OK: {}", output.trim());
    Ok(())
}

async fn cmd_batch(
    csv_path: &PathBuf,
    dry_run: bool,
    concurrency: usize,
    reason: &str,
    config: &TemporalConfig,
) -> Result<()> {
    let call_ids = csv_reader::read_call_ids(csv_path)?;
    let total = call_ids.len();
    println!("Loaded {total} call IDs from {}", csv_path.display());

    if dry_run {
        println!("DRY RUN: validating workflows...\n");
    }

    let semaphore = Arc::new(Semaphore::new(concurrency));
    let config = Arc::new(config.clone());
    let reason = reason.to_string();

    let mut handles = Vec::new();

    for (i, call_id) in call_ids.into_iter().enumerate() {
        let permit = semaphore.clone().acquire_owned().await?;
        let config = config.clone();
        let _reason = reason.clone();

        let handle = tokio::task::spawn_blocking(move || -> bool {
            let idx = i + 1;
            let result = if dry_run {
                temporal::describe_workflow(&call_id, &config)
            } else {
                temporal::show_workflow_input(&call_id, &config)
                    .and_then(|info| temporal::start_workflow(&call_id, &config, &info))
            };

            let ok = match result {
                Ok(_) => {
                    println!("[{idx}/{total}] OK: post-processor-{call_id}");
                    true
                }
                Err(e) => {
                    eprintln!("[{idx}/{total}] FAIL: post-processor-{call_id}: {e}");
                    false
                }
            };

            drop(permit);
            ok
        });

        handles.push(handle);
    }

    let mut success = 0usize;
    let mut failed = 0usize;

    for handle in handles {
        if handle.await? {
            success += 1;
        } else {
            failed += 1;
        }
    }

    println!("\n=== Summary ===");
    println!("Total: {total} | Success: {success} | Failed: {failed}");

    Ok(())
}
