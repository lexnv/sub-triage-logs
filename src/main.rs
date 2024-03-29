use clap::Parser as ClapParser;

pub mod query;

const GROUPED: [&str; 26] = [
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

    "Trying to remove unknown reserved node",

    "babe: 👶 Epoch(s) skipped:",
    "babe: Error with block built on",
    "sync: 💔 Called `on_validated_block_announce` with a bad peer ID",

    "parachain::availability-store: Candidate included without being backed?",
    "parachain::availability-distribution: fetch_pov_job err=FetchPoV(NetworkError(NotConnected))",
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

    fn validate(&self) {
        log::info!(
            "Running statistics: Execution took {}s {:?}",
            self.now.elapsed().as_secs(),
            self
        );

        assert_eq!(
            self.total,
            self.empty_lines + self.warning_err + self.unknown
        )
    }
}

/// Command for interacting with the CLI.
#[derive(Debug, ClapParser)]
enum Command {
    WarnErr(WarnErr),
}

#[derive(Debug, ClapParser)]
struct WarnErr {
    /// The address of the Loki instance.
    #[clap(long, default_value = "http://loki.parity-versi.parity.io")]
    address: String,

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
}

fn run_warn_err(opts: WarnErr) -> Result<(), Box<dyn std::error::Error>> {
    log::info!("Running WarnErr query");
    let mut stats = Stats::new();

    // Build the query.
    let query = query::QueryBuilder::new()
        .address(opts.address)
        .chain(opts.chain)
        .levels(vec!["WARN".to_string(), "ERROR".to_string()])
        .set_time(opts.start_time, opts.end_time)
        .build();

    // Run the query.
    let result = query::QueryRunner::run(&query)?;
    let result = String::from_utf8_lossy(&result);

    let mut grouped_err: Vec<_> = GROUPED.iter().map(|&x| (x, 0)).collect();
    let mut unknown_lines = Vec::with_capacity(1024);

    for line in result.lines() {
        log::debug!("{}", line);

        stats.total += 1;

        if line.is_empty() {
            stats.empty_lines += 1;
            continue;
        }

        let mut found = false;
        for (key, value) in grouped_err.iter_mut() {
            if line.contains(*key) {
                *value += 1;
                found = true;
                // We are not interested in the rest of the keys, they should not be a subset of each other.
                break;
            }
        }

        if !found {
            stats.unknown += 1;
            unknown_lines.push(line);
        }
    }

    println!();
    println!();
    println!(
        "{0: <135} | {1:<10}",
        "WarningError -------------------", "Count"
    );

    grouped_err.sort_by_key(|(_key, value)| std::cmp::Reverse(*value));

    for (key, value) in grouped_err.iter() {
        println!("{0: <135} | {1:<10}", key, value);

        stats.warning_err += value;
    }

    println!(
        "\nUnknown lines [num {}]: {:?}",
        unknown_lines.len(),
        unknown_lines
    );

    stats.validate();

    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let args = Command::parse();
    // One command for now.
    let Command::WarnErr(warn_err) = args;
    run_warn_err(warn_err)
}
