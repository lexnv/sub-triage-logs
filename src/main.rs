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

#[derive(Debug, ClapParser, Clone)]
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

enum QueryType {
    /// The triage is running for a provided file.
    File(String),
    /// The triage is running for a query against grafana.
    Grafana(Vec<String>),
}

struct WarnErr {
    /// Statistics about processing lines.
    stats: Stats,

    /// The unknown lines.
    unknown_lines: Vec<String>,
    /// The found lines from the regex.
    found_lines: HashMap<(String, RegexDetails), Vec<String>>,

    /// The deduplication information.
    ///
    /// This is used to provide a better triage report, grouping by specific error
    /// that cannot be extracted by the regex.
    dedup_info: Vec<DeduplicationInfo>,

    /// The regexes to match against, downloaded and compiled from the git repository.
    regexes: Vec<(Regex, RegexDetails)>,

    /// The query type.
    query_type: QueryType,

    /// Provide the raw lines from the query.
    raw: bool,
}

impl WarnErr {
    fn build_query(opts: Config) -> QueryType {
        if let Some(file) = opts.file {
            QueryType::File(file)
        } else {
            let queries = query::QueryBuilder::new()
                .address(opts.address)
                .chain(opts.chain)
                .levels(vec!["WARN".to_string(), "ERROR".to_string()])
                .set_time(opts.start_time, opts.end_time)
                .org_id(opts.org_id)
                .node(opts.node)
                .build_chunks();

            QueryType::Grafana(queries)
        }
    }

    async fn build_regexes(
        opts: Config,
    ) -> Result<Vec<(Regex, RegexDetails)>, Box<dyn std::error::Error>> {
        if opts.skip_regex_build {
            return Ok(vec![]);
        }

        let files = fetch_git::fetch(
            opts.regex_repo.ok_or("Missing regex repo")?,
            opts.regex_branch.ok_or("Missing regex branch")?,
        )
        .await?;

        Ok(fetch_git::build_regexes(files))
    }

    async fn new(opts: Config) -> Result<WarnErr, Box<dyn std::error::Error>> {
        log::info!("Running WarnErr query");

        let raw = opts.raw;
        let query_type = Self::build_query(opts.clone());
        let regexes = Self::build_regexes(opts).await?;

        // Hardcoded currently for peerset.
        let dedup_info = vec![
            // Litep2p peerset.
            DeduplicationInfo {
                log_line: "banned, disconnecting, reason:".to_string(),
                dedup_after: "banned, disconnecting, reason:".to_string(),
            },
            // Libp2p peerset (old backend)
            DeduplicationInfo {
                log_line: "Banned, disconnecting.".to_string(),
                dedup_after: "Reason:".to_string(),
            },
            // Error importing block deduplication.
            DeduplicationInfo {
                log_line: "Error importing block".to_string(),
                dedup_after: ":".to_string(),
            },
        ];

        Ok(WarnErr {
            stats: Stats::new(),
            unknown_lines: Vec::with_capacity(1024),
            found_lines: HashMap::with_capacity(1024),
            dedup_info,
            regexes,
            query_type,
            raw,
        })
    }

    async fn run(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        match &self.query_type {
            QueryType::File(file) => {
                let bytes = std::fs::read(file)?;
                let result = String::from_utf8_lossy(&bytes);

                let lines = result
                    .lines()
                    .filter(|x| x.contains("WARN") || x.contains("ERROR"));

                self.process_lines(lines);
            }
            QueryType::Grafana(queries) => {
                // Run the queries.
                for query in queries.clone() {
                    let bytes = query::QueryRunner::run(&query)?;
                    let result = String::from_utf8_lossy(&bytes);

                    self.process_lines(result.lines());
                }
            }
        }

        self.process_results();

        Ok(())
    }

    fn find_deduplication_key(&self, line: &str) -> Option<String> {
        for dedup in &self.dedup_info {
            if !line.contains(&dedup.log_line) {
                continue;
            }

            let substr = line.rsplit_once(&dedup.dedup_after);
            if let Some((_, rest)) = substr {
                return Some(rest.to_string());
            }
        }
        None
    }

    fn process_lines<'a>(&mut self, lines: impl Iterator<Item = &'a str>) {
        let now = std::time::Instant::now();

        for line in lines {
            log::debug!("{}", line);

            self.stats.total += 1;

            if line.is_empty() {
                self.stats.empty_lines += 1;
                continue;
            }

            let mut found = false;

            for (reg, reg_details) in &self.regexes {
                if reg.is_match(line) {
                    let dedup_key = self.find_deduplication_key(line);

                    let entry_key = if let Some(dedup_key) = dedup_key {
                        format!("{} ({})", reg.to_string(), dedup_key)
                    } else {
                        reg.to_string()
                    };

                    self.found_lines
                        .entry((entry_key, reg_details.clone()))
                        .or_default()
                        .push(line.to_string());

                    found = true;
                    break;
                }
            }

            if !found {
                self.stats.unknown += 1;
                self.unknown_lines.push(line.to_string());
            }
        }

        log::info!(" Processing line took {:?}", now.elapsed());
    }

    fn process_results(&mut self) {
        // Sort the found lines by occurrence.
        let mut found_lines: Vec<_> = self.found_lines.clone().into_iter().collect();
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
            self.stats.warning_err += value.len();
        }

        println!(
            "\nUnknown lines [num {}]: {:#?}",
            self.unknown_lines.len(),
            self.unknown_lines
        );

        if self.raw {
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
    }
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
        Command::WarnErr(opts) => WarnErr::new(opts).await?.run().await,
        Command::Panics(opts) => run_panics(opts),
    }
}
