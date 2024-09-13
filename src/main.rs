use anyhow::{Context, Result};
use chrono::Utc;
use clap::{Parser, Subcommand};
use cron::Schedule;
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use std::fs;
use std::process::Command;
use std::str::FromStr;
use tokio::time::sleep;

#[derive(Parser)]
#[command(author, version, about)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Run the application as a daemon
    Daemon,
    /// Run benchmark for a specific commit
    Run {
        /// The commit hash to benchmark
        #[arg(short, long)]
        commit: String,
    },
}

#[derive(Serialize, Deserialize)]
struct HyperfineResults {
    results: Vec<BenchmarkResult>,
}

#[derive(Serialize, Deserialize)]
struct BenchmarkResult {
    command: String,
    mean: f64,
    #[serde(default)]
    stddev: Option<f64>,
    median: f64,
    user: f64,
    system: f64,
    min: f64,
    max: f64,
    times: Vec<f64>,
    exit_codes: Vec<i32>,
    parameters: Option<Parameters>,
}

#[derive(Serialize, Deserialize)]
struct Parameters {
    commit: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match &cli.command {
        Some(Commands::Daemon) => {
            start_daemon().await?;
        }
        Some(Commands::Run { commit }) => {
            run_benchmark(commit.to_string()).await?;
        }
        None => {
            println!("Please specify a command. Use --help for more information.");
        }
    }

    Ok(())
}

async fn start_daemon() -> Result<()> {
    let cron_expression = "0 0 0 * * * *"; // Every day at midnight
    let schedule = Schedule::from_str(cron_expression).unwrap();
    let mut upcoming = schedule.upcoming(Utc);

    while let Some(datetime) = upcoming.next() {
        let now = Utc::now();
        let duration = datetime - now;

        if duration.to_std().is_ok() {
            let delay = duration.to_std().unwrap();
            sleep(delay).await;
        } else {
            // If the duration is negative, skip to next
            continue;
        }

        if let Err(e) = run_benchmark("master".to_string()).await {
            eprintln!("Error running benchmark: {:?}", e);
        }
    }

    Ok(())
}

async fn run_benchmark(commit: String) -> Result<()> {
    let repo_path = "/home/will/src/bitcoin";
    let db_path = "/home/will/src/bitcoin_benchmark/results.db";

    tokio::task::spawn_blocking(move || -> Result<()> {
        git_update_repository(&commit, repo_path)?;
        run_hyperfine(&commit, repo_path)?;
        save_results_to_db(&commit, repo_path, db_path)?;
        Ok(())
    })
    .await
    .map_err(|e| anyhow::anyhow!("Task failed: {}", e))??;
    Ok(())
}

fn git_update_repository(commit: &str, repo_path: &str) -> Result<()> {
    std::env::set_current_dir(repo_path)
        .with_context(|| format!("Failed to change directory to {}", repo_path))?;

    Command::new("git")
        .args(["fetch", "--all"])
        .status()
        .with_context(|| "Failed to fetch git repository")?;

    Command::new("git")
        .args(["checkout", commit])
        .status()
        .with_context(|| format!("Failed to checkout commit {}", commit))?;

    Ok(())
}

fn run_hyperfine(commit: &str, repo_path: &str) -> Result<()> {
    std::env::set_current_dir(repo_path)
        .with_context(|| format!("Failed to change directory to {}", &repo_path))?;

    let hyperfine_command = format!(
        "hyperfine \
        --parameter-list commit {commit} \
        --setup 'rm -Rf build && git checkout {{commit}} && cmake -B build && cmake --build build -j$(nproc)' \
        --prepare 'sync && rm -Rf /mnt/bench/.bitcoin/*' \
        --cleanup '' \
        --runs 1 \
        --show-output \
        --export-json results.json \
        './build/src/bitcoind -datadir=/mnt/bench/.bitcoin -connect=127.0.0.1:8333 -port=8444 -rpcport=8445 -dbcache=16385 -printtoconsole=0 -stopatheight=100000'"
    );

    let output = Command::new("sh")
        .arg("-c")
        .arg(&hyperfine_command)
        .output()
        .with_context(|| "Failed to execute hyperfine command")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        return Err(anyhow::anyhow!(
            "Hyperfine command failed with status {}: \nStdout: {}\nStderr: {}",
            output.status,
            stdout,
            stderr
        ));
    }

    Ok(())
}

fn save_results_to_db(commit: &str, repo_path: &str, db_path: &str) -> Result<()> {
    let results_json_path = format!("{}/results.json", repo_path);
    let data = fs::read_to_string(&results_json_path)
        .with_context(|| format!("Failed to read results.json file at {}", results_json_path))?;
    let results: HyperfineResults =
        serde_json::from_str(&data).with_context(|| "Failed to parse JSON from results.json")?;

    let conn = Connection::open(db_path).with_context(|| "Failed to connect to SQLite database")?;

    conn.execute(
        "CREATE TABLE IF NOT EXISTS benchmarks (
            id INTEGER PRIMARY KEY,
            commit_hash TEXT NOT NULL,
            command TEXT NOT NULL,
            mean REAL,
            stddev REAL,
            median REAL,
            user REAL,
            system REAL,
            min REAL,
            max REAL,
            times TEXT,
            exit_codes TEXT,
            parameters TEXT
        )",
        [],
    )
    .with_context(|| "Failed to create benchmarks table")?;

    // Insert the benchmark results
    for result in results.results {
        // Extract commit from parameters if available
        let commit_value = if let Some(ref params) = result.parameters {
            &params.commit
        } else {
            commit
        };

        conn.execute(
            "INSERT INTO benchmarks (
                commit_hash, command, mean, stddev, median, user, system, min, max, times, exit_codes, parameters
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            params![
                commit_value,
                result.command,
                result.mean,
                result.stddev,
                result.median,
                result.user,
                result.system,
                result.min,
                result.max,
                serde_json::to_string(&result.times)
                    .with_context(|| "Failed to serialize times")?,
                serde_json::to_string(&result.exit_codes)
                    .with_context(|| "Failed to serialize exit_codes")?,
                serde_json::to_string(&result.parameters)
                    .with_context(|| "Failed to serialize parameters")?,
            ],
        )
        .with_context(|| "Failed to insert benchmark result into database")?;
    }

    Ok(())
}
