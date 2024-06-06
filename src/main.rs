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

    #[clap(long)]
    skip_build: bool,

    /// Provide the raw lines from the query.
    #[clap(long)]
    raw: bool,
}

fn process_lines<'a>(
    lines: impl Iterator<Item = &'a str>,
    stats: &mut Stats,
    unknown_lines: &mut Vec<String>,
    regexes: &[(Regex, RegexDetails)],
    found_lines: &mut HashMap<String, Vec<String>>,
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

        for (reg, _) in regexes {
            if reg.is_match(line) {
                found_lines
                    .entry(reg.to_string())
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

    let regexes = if !opts.skip_build {
        let files = fetch_git::fetch(
            "https://github.com/paritytech/polkadot-sdk/".into(),
            "master".into(),
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
        );
    } else {
        // Build the query.
        let queries = query::QueryBuilder::new()
            .address(opts.address)
            .chain(opts.chain)
            .levels(vec!["WARN".to_string(), "ERROR".to_string()])
            .set_time(opts.start_time, opts.end_time)
            .org_id(opts.org_id)
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
            );
        }
    }

    // Sort the found lines by occurrence.
    let mut found_lines: Vec<_> = found_lines.into_iter().collect();
    found_lines.sort_by(|a, b| b.1.len().cmp(&a.1.len()));

    println!();
    println!();
    println!("{0: <10} | {1:<135}", "Count", "Triage report");

    for (key, value) in found_lines.iter() {
        if value.is_empty() {
            continue;
        }

        println!("{0: <10} | {1:<135}", value.len(), key);
        stats.warning_err += value.len();
    }

    println!(
        "\nUnknown lines [num {}]: {:?}",
        unknown_lines.len(),
        unknown_lines
    );

    if opts.raw {
        for (key, value) in found_lines.iter() {
            if value.is_empty() {
                continue;
            }

            println!("{0: <10} | {1:<135}", value.len(), key);
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
