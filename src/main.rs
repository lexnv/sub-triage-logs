use std::collections::HashMap;

use clap::Parser as ClapParser;
use fetch_git::RegexDetails;
use regex::Regex;

pub mod fetch_git;
pub mod query;

#[derive(Debug)]
struct Stats {
    total: usize,
    empty_lines: usize,
    warning_err: usize,
    unknown: usize,
    now: std::time::Instant,
}

impl Stats {
    fn new() -> Self {
        Stats {
            total: 0,
            empty_lines: 0,
            warning_err: 0,
            unknown: 0,
            now: std::time::Instant::now(),
        }
    }
}

impl Drop for Stats {
    fn drop(&mut self) {
        log::info!(
            "Statistics: Execution took {}s {:?}",
            self.now.elapsed().as_secs(),
            self
        );
    }
}

/// Command for interacting with the CLI.
#[derive(Debug, ClapParser)]
enum Command {
    WarnErr(Config),
    Panics(Config),
}

#[derive(Debug, ClapParser)]
struct Config {
    /// The address of the Loki instance.
    #[clap(long, default_value = "http://loki.parity-versi.parity.io")]
    address: String,

    /// Optionally provide a file for parsing instead of querying the Loki instance.
    #[clap(long)]
    file: Option<String>,

    /// The chain to query.
    #[clap(long, default_value = "versi-networking")]
    chain: String,

    /// The node to query.
    #[clap(long)]
    node: Option<String>,

    /// The start time of the query.
    /// The format is "YYYY-MM-DDTHH:MM:SSZ".
    #[clap(long)]
    start_time: Option<String>,

    /// The end time of the query.
    /// The format is "YYYY-MM-DDTHH:MM:SSZ".
    #[clap(long)]
    end_time: Option<String>,

    /// Optionally provide an organization ID.
    #[clap(long)]
    org_id: Option<String>,

    /// Skip building the regexes.
    #[clap(long)]
    skip_regex_build: bool,

    /// Build the regexes from a repository.
    #[clap(long, default_value = "https://github.com/paritytech/polkadot-sdk/")]
    regex_repo: Option<String>,

    /// Build the regexes from the branch
    #[clap(long, default_value = "master")]
    regex_branch: Option<String>,

    /// Provide the raw lines from the query.
    #[clap(long)]
    raw: bool,
}

struct DeduplicationInfo {
    log_line: String,
    dedup_after: String,
}

fn find_deduplication_key(line: &str, dedup_info: &[DeduplicationInfo]) -> Option<String> {
    for dedup in dedup_info {
        if line.contains(&dedup.log_line) {
            let substr = line.split_once(&dedup.dedup_after);
            if let Some((_, rest)) = substr {
                return Some(rest.to_string());
            }
        }
    }
    None
}

fn process_lines<'a>(
    lines: impl Iterator<Item = &'a str>,
    stats: &mut Stats,
    unknown_lines: &mut Vec<String>,
    regexes: &[(Regex, RegexDetails)],
    found_lines: &mut HashMap<(String, RegexDetails), Vec<String>>,
    dedup_info: &[DeduplicationInfo],
) {
    let now = std::time::Instant::now();

    for line in lines {
        log::debug!("{}", line);

        stats.total += 1;

        if line.is_empty() {
            stats.empty_lines += 1;
            continue;
        }

        let mut found = false;

        for (reg, reg_details) in regexes {
            if reg.is_match(line) {
                let dedup_key = find_deduplication_key(line, dedup_info);

                let entry_key = if let Some(dedup_key) = dedup_key {
                    format!("{} {}", reg.to_string(), dedup_key)
                } else {
                    reg.to_string()
                };

                found_lines
                    .entry((entry_key, reg_details.clone()))
                    .or_default()
                    .push(line.to_string());

                found = true;
                break;
            }
        }

        if !found {
            stats.unknown += 1;
            unknown_lines.push(line.to_string());
        }
    }

    log::info!(" Processing line took {:?}", now.elapsed());
}

async fn run_warn_err(opts: Config) -> Result<(), Box<dyn std::error::Error>> {
    log::info!("Running WarnErr query");
    let mut stats = Stats::new();

    let mut unknown_lines = Vec::with_capacity(1024);
    let mut found_lines = HashMap::new();

    let dedup_info = vec![DeduplicationInfo {
        log_line: "banned, disconnecting, reason:".to_string(),
        dedup_after: "banned, disconnecting, reason:".to_string(),
    }];

    let regexes = if !opts.skip_regex_build {
        let files = fetch_git::fetch(
            opts.regex_repo.ok_or("Missing regex repo")?,
            opts.regex_branch.ok_or("Missing regex branch")?,
        )
        .await?;

        fetch_git::build_regexes(files)
    } else {
        vec![]
    };

    if let Some(file) = &opts.file {
        let bytes = std::fs::read(file)?;
        let result = String::from_utf8_lossy(&bytes);

        let lines = result
            .lines()
            .filter(|x| x.contains("WARN") || x.contains("ERROR"));

        process_lines(
            lines,
            &mut stats,
            &mut unknown_lines,
            &regexes,
            &mut found_lines,
            &dedup_info,
        );
    } else {
        // Build the query.
        let queries = query::QueryBuilder::new()
            .address(opts.address)
            .chain(opts.chain)
            .levels(vec!["WARN".to_string(), "ERROR".to_string()])
            .set_time(opts.start_time, opts.end_time)
            .org_id(opts.org_id)
            .node(opts.node)
            .build_chunks();

        // Run the queries.
        for query in queries {
            let bytes = query::QueryRunner::run(&query)?;
            let result = String::from_utf8_lossy(&bytes);

            process_lines(
                result.lines(),
                &mut stats,
                &mut unknown_lines,
                &regexes,
                &mut found_lines,
                &dedup_info,
            );
        }
    }

    // Sort the found lines by occurrence.
    let mut found_lines: Vec<_> = found_lines.into_iter().collect();
    found_lines.sort_by(|a, b| b.1.len().cmp(&a.1.len()));

    println!();
    println!();
    println!(
        "{0: <10} | {1: <10} | {2:<135}",
        "Count", "Level", "Triage report"
    );

    for ((key, details), value) in found_lines.iter() {
        if value.is_empty() {
            continue;
        }

        println!("{0:<10} | {1:<10} | {2:<135}", value.len(), details.ty, key);
        stats.warning_err += value.len();
    }

    println!(
        "\nUnknown lines [num {}]: {:#?}",
        unknown_lines.len(),
        unknown_lines
    );

    if opts.raw {
        for ((key, details), value) in found_lines.iter() {
            if value.is_empty() {
                continue;
            }

            println!("{0:<10} | {1:<10} | {2:<135}", value.len(), details.ty, key);
            for line in value {
                println!("  - {}", line);
            }
            println!();
        }
    }

    Ok(())
}

fn run_panics(opts: Config) -> Result<(), Box<dyn std::error::Error>> {
    log::info!("Running panic query");
    let mut stats = Stats::new();

    // Build the query.
    let queries = query::QueryBuilder::new()
        .address(opts.address)
        .chain(opts.chain)
        .set_time(opts.start_time, opts.end_time)
        // Panics can appear anywhere.
        .exclude_common_errors(false)
        .append_query("|~ `panic`".to_string())
        .build_chunks();

    for query in queries {
        // Run the query.
        let mut result = query::QueryRunner::run(&query);
        let mut retries = 0;
        while let Err(err) = &result {
            log::error!("Error: {}", err);
            std::thread::sleep(std::time::Duration::from_secs(5));

            retries += 1;

            if retries == 3 {
                log::error!("Failed to run query after 3 retries");
                break;
            }
            result = query::QueryRunner::run(&query);
        }

        let result = result?;
        let result = String::from_utf8_lossy(&result);

        for line in result.lines() {
            log::debug!("{}", line);

            if line.is_empty() {
                stats.empty_lines += 1;
                continue;
            }

            stats.total += 1;
        }

        log::info!("Finished partial query");
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let args = Command::parse();
    match args {
        Command::WarnErr(opts) => run_warn_err(opts).await,
        Command::Panics(opts) => run_panics(opts),
    }
}
