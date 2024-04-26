use clap::Parser as ClapParser;

pub mod query;

const GROUPED: [&str; 35] = [
    // peerset:
    "Reason: BEEFY: Round vote message. Banned, disconnecting",
    "Reason: BEEFY: Not interested in round. Banned, disconnecting",
    "Reason: Invalid justification. Banned, disconnecting",
    "Reason: Aggregated reputation change. Banned, disconnecting",
    "Reason: Successful gossip. Banned, disconnecting",
    "Reason: Grandpa: Neighbor message. Banned, disconnecting",
    "Reason: Grandpa: Past message. Banned, disconnecting",
    "Reason: Grandpa: Round message. Banned, disconnecting",
    "Reason: BEEFY: Justification. Banned, disconnecting",
    "Reason: Duplicate gossip. Banned, disconnecting",
    "Reason: BEEFY: Future message. Banned, disconnecting",
    "Reason: A collator was reported by another subsystem. Banned, disconnecting",
    "Same block request multiple times",
    "reason: Peer disconnected",

    "Trying to remove unknown reserved node",

    "babe: 👶 Epoch(s) skipped:",
    "babe: Error with block built on",
    "sync: 💔 Called `on_validated_block_announce` with a bad peer ID",

    "parachain::availability-store: Candidate included without being backed?",
    "parachain::availability-distribution: fetch_pov_job err=FetchPoV(NetworkError(NotConnected))",
    "parachain::availability-distribution: fetch_pov_job err=FetchPoV(NetworkError(Refused))",
    "parachain::availability-distribution: fetch_pov_job err=FetchPoV(NetworkError(Network(DialFailure)))",
    "parachain::dispute-coordinator: Attempted import of on-chain backing votes failed",

    // Note: These are the same, however substrate added an extra `\n`.
    "parachain::statement-distribution: Cluster has too many pending statements, something wrong with our connection to our group peers",
    "Restart might be needed if validator gets 0 backing rewards for more than 3-4 consecutive sessions",


    // collator-protocol:
    "Fetching collation failed due to network error",
    // chain-selection:
    "chain-selection: Call to `DetermineUndisputedChain` failed error=DetermineUndisputedChainCanceled(Canceled)",
    // disputes:
    "dispute-coordinator: Received msg before first active leaves update. This is not expected - message will be dropped",
    // Grandpa:
    "grandpa: Re-finalized block",

    // DB pinning:
    "db::notification_pinning: Notification block pinning limit reached.",

    // Beefy:
    "Error: ConsensusReset. Restarting voter.",
    "no BEEFY authority key found in store",

    // Litep2p:
    "litep2p::ipfs::identify: inbound identify substream opened for peer who doesn't exist peer=",

    // Telemetry missing:
    "Error while dialing /dns/telemetry",

    "because all validation slots for this peer are occupied.",
];

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

    /// Provide the raw lines from the query.
    #[clap(long)]
    raw: bool,
}

fn process_lines<'a>(
    lines: impl Iterator<Item = &'a str>,
    stats: &mut Stats,
    grouped_err: &mut [(&str, Vec<String>)],
    unknown_lines: &mut Vec<String>,
) {
    for line in lines {
        log::debug!("{}", line);

        stats.total += 1;

        if line.is_empty() {
            stats.empty_lines += 1;
            continue;
        }

        let mut found = false;
        for (key, value) in grouped_err.iter_mut() {
            if line.contains(*key) {
                value.push(line.to_string());
                found = true;
                // We are not interested in the rest of the keys, they should not be a subset of each other.
                break;
            }
        }

        if !found {
            stats.unknown += 1;
            unknown_lines.push(line.to_string());
        }
    }
}

fn run_warn_err(opts: Config) -> Result<(), Box<dyn std::error::Error>> {
    log::info!("Running WarnErr query");
    let mut stats = Stats::new();

    let mut grouped_err: Vec<_> = GROUPED.iter().map(|&x| (x, vec![])).collect();
    let mut unknown_lines = Vec::with_capacity(1024);

    if let Some(file) = &opts.file {
        let bytes = std::fs::read(file)?;
        let result = String::from_utf8_lossy(&bytes);

        let lines = result
            .lines()
            .filter(|x| x.contains("WARN") || x.contains("ERROR"));

        process_lines(lines, &mut stats, &mut grouped_err, &mut unknown_lines);
    } else {
        // Build the query.
        let queries = query::QueryBuilder::new()
            .address(opts.address)
            .chain(opts.chain)
            .levels(vec!["WARN".to_string(), "ERROR".to_string()])
            .set_time(opts.start_time, opts.end_time)
            .build_chunks();

        // Run the queries.
        for query in queries {
            let bytes = query::QueryRunner::run(&query)?;
            let result = String::from_utf8_lossy(&bytes);

            process_lines(
                result.lines(),
                &mut stats,
                &mut grouped_err,
                &mut unknown_lines,
            );
        }
    }

    println!();
    println!();
    println!("{0: <10} | {1:<135}", "Count", "Triage report");

    grouped_err.sort_by_key(|(_key, value)| std::cmp::Reverse(value.len()));

    for (key, value) in grouped_err.iter() {
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
        for (key, value) in grouped_err.iter() {
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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let args = Command::parse();

    match args {
        Command::WarnErr(opts) => run_warn_err(opts),
        Command::Panics(opts) => run_panics(opts),
    }
}
