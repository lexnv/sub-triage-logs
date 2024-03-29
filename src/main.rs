use std::os::{macos::raw::stat, unix::fs::chroot};

use clap::Parser as ClapParser;

const CMD_WARN_ERR: &str = r#"docker run grafana/logcli:main-926a0b2-amd64 query --addr=http://loki.parity-versi.parity.io --timezone=UTC --from="2024-03-28T14:00:00Z" --to "2024-03-28T17:00:10Z" '{chain="versi-networking", level=~"ERROR|WARN"} != `Error while dialing` != `Some security issues have been detected` != `The hardware does not meet`' --batch 5000 --limit 100000"#;

const CMD_WARN_ERR_FORMAT: &str = r#"docker run grafana/logcli:main-926a0b2-amd64 query --addr={} --timezone=UTC --from="{}" --to="{}" '{chain="versi-networking", level=~"ERROR|WARN"} != `Error while dialing` != `Some security issues have been detected` != `The hardware does not meet`' --batch 5000 --limit 100000"#;

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

struct QueryBuilder {
    address: Option<String>,
    chain: Option<String>,
    start_time: Option<String>,
    end_time: Option<String>,
    batch: usize,
    limit: usize,
    exclude_common_errors: bool,
}

impl QueryBuilder {
    /// Create a new QueryBuilder.
    pub fn new() -> Self {
        Self {
            address: None,
            chain: None,
            start_time: None,
            end_time: None,
            batch: 5000,
            limit: 100000,
            exclude_common_errors: true,
        }
    }

    /// Set the address of the Loki instance.
    ///
    /// Default: "http://loki.parity-versi.parity.io".
    pub fn address(mut self, address: String) -> Self {
        self.address = Some(address);
        self
    }

    /// Set the chain to query.
    ///
    /// Default: "versi-networking".
    pub fn chain(mut self, chain: String) -> Self {
        self.chain = Some(chain);
        self
    }

    /// Set the start and end times of the query.
    ///
    /// The format is "YYYY-MM-DDTHH:MM:SSZ".
    ///
    /// Default: 1 hour before the current time.
    pub fn set_time(mut self, start_time: Option<String>, end_time: Option<String>) -> Self {
        self.start_time = start_time;
        self.end_time = end_time;
        self
    }

    /// Exclude common errors from the query.
    ///
    /// The common errors are:
    /// - "Error while dialing" - telemetry error
    /// - "Some security issues have been detected" - PVF hardware error requiring different kernel settings
    /// - "The hardware does not meet" - Requires a better hardware to be a validator
    ///
    /// Default: true.
    pub fn exclude_common_errors(mut self, exclude_common_errors: bool) -> Self {
        self.exclude_common_errors = exclude_common_errors;
        self
    }

    /// Set the batch size of the query.
    ///
    /// Default: 5000.
    pub fn batch(mut self, batch: usize) -> Self {
        self.batch = batch;
        self
    }

    /// Set the limit of the query.
    ///
    /// Default: 100000.
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }

    /// Build the query.
    pub fn build(&self) -> String {
        let exclude_common_errors = if self.exclude_common_errors {
            " != `Error while dialing` != `Some security issues have been detected` != `The hardware does not meet`"
        } else {
            ""
        };

        const DOCKER_CONTAINER: &str = "grafana/logcli:main-926a0b2-amd64";
        const DEFAULT_URL: &str = "http://loki.parity-versi.parity.io";
        const DEFAULT_CHAIN: &str = "versi-networking";

        let (start_time, end_time) = match (&self.start_time, &self.end_time) {
            (None, None) => {
                // Compute endtime as now.
                let date_time = chrono::Utc::now();
                let end_time = format!("{}", date_time.format("%Y-%m-%dT%H:%M:%SZ"));
                log::debug!("End time: {}", end_time);

                // Subtract 1 hour from date time
                let date_time = date_time - chrono::Duration::hours(1);
                let start_time = format!("{}", date_time.format("%Y-%m-%dT%H:%M:%SZ"));
                log::debug!("Start time: {}", start_time);

                (start_time, end_time)
            }
            (Some(start_time), Some(end_time)) => {
                println!("Using provided");
                (start_time.clone(), end_time.clone())
            }
            _ => {
                panic!("Either both start and end time should be provided or none")
            }
        };

        println!("Start time: {}", start_time);
        println!("End time: {}", end_time);

        let addr = self
            .address
            .as_ref()
            .map(|a| a.clone())
            .unwrap_or(DEFAULT_URL.to_string());
        let chain = self
            .chain
            .as_ref()
            .map(|c| c.clone())
            .unwrap_or(DEFAULT_CHAIN.to_string());

        let res = format!(
            r#"docker run {} query --addr={} --timezone=UTC --from="{}" --to="{}" '{{chain="{}", level=~"ERROR|WARN"}}{}' --batch {} --limit {}"#,
            DOCKER_CONTAINER,
            addr,
            start_time,
            end_time,
            chain,
            exclude_common_errors,
            self.batch,
            self.limit,
        );

        const CMD_WARN_ERR: &str = r#"docker run grafana/logcli:main-926a0b2-amd64 query --addr=http://loki.parity-versi.parity.io --timezone=UTC --from="2024-03-28T14:00:00Z" --to "2024-03-28T17:00:10Z" '{chain="versi-networking", level=~"ERROR|WARN"} != `Error while dialing` != `Some security issues have been detected` != `The hardware does not meet`' --batch 5000 --limit 100000"#;

        let res = format!(
            r#"docker run grafana/logcli:main-926a0b2-amd64 query --addr={} --timezone=UTC --from="{}" --to="{}" '{{chain="{}", level=~"ERROR|WARN"}} {}' --batch {} --limit {}"#,
            // "2024-03-28T14:00:00Z", "2024-03-28T17:00:10Z"
            addr,
            start_time,
            end_time,
            chain,
            exclude_common_errors,
            self.batch,
            self.limit,
        );

        //             2024-03-28T14:00:00Z"
        // Start time: 2024-03-29T09:10:21Z
        // End time:   2024-03-29T10:10:21Z

        println!("Start time: {}", start_time);
        println!("End time: {}", end_time);

        println!("Res {}", res);
        println!("CMD_WARN_ERR {}", CMD_WARN_ERR);

        CMD_WARN_ERR.to_string();
        res
    }
}

fn run_warn_err(mut opts: WarnErr) -> Result<(), Box<dyn std::error::Error>> {
    log::info!("Running WarnErr command");

    let query = QueryBuilder::new()
        .address(opts.address)
        .chain(opts.chain)
        .set_time(opts.start_time, opts.end_time)
        .build();

    println!("Query: {}", query);

    // let (start_time, end_time) = match (opts.start_time, opts.end_time) {
    //     (None, None) => {
    //         // Compute endtime as now.
    //         let date_time = chrono::Utc::now();
    //         let end_time = format!("{}", date_time.format("%Y-%m-%dT%H:%M:%SZ"));
    //         log::debug!("End time: {}", end_time);

    //         // Subtract 1 hour from date time
    //         let date_time = date_time - chrono::Duration::hours(1);
    //         let start_time = format!("{}", date_time.format("%Y-%m-%dT%H:%M:%SZ"));
    //         log::debug!("Start time: {}", start_time);

    //         (start_time, end_time)
    //     }
    //     (Some(start_time), Some(end_time)) => (start_time, end_time),
    //     _ => {
    //         return Err("Either both start and end time should be provided or none.".into());
    //     }
    // };

    let now = std::time::Instant::now();
    let result = std::process::Command::new("sh")
        .arg("-c")
        .arg(query)
        .output()
        .expect("failed to execute process");
    println!("WARN_ERR took: {:?}", now.elapsed());

    if result.status.success() {
        println!("WARN_ERR ran successfully");
    } else {
        println!("WARN_ERR failed");
    }

    let output = String::from_utf8_lossy(&result.stdout);

    let mut grouped_err: Vec<_> = GROUPED.iter().map(|&x| (x, 0)).collect();
    let mut unknown_lines = Vec::with_capacity(1024);

    #[derive(Default)]
    struct Stats {
        total: usize,
        empty_lines: usize,
        warning_err: usize,
        unknown: usize,
    }

    impl Stats {
        fn validate(&self) {
            assert_eq!(
                self.total,
                self.empty_lines + self.warning_err + self.unknown
            )
        }
    }

    let mut stats = Stats::default();

    for line in output.lines() {
        println!("{}", line);

        stats.total += 1;

        let mut found = false;
        for (key, value) in grouped_err.iter_mut() {
            if line.contains(*key) {
                *value += 1;
                found = true;
                // We are not interested in the rest of the keys, they should be unique.
                break;
            }
        }

        if line.is_empty() {
            stats.empty_lines += 1;
            continue;
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

    let mut total_count = 0;

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
