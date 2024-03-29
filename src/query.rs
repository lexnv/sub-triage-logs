//! Query builder and running for fetching the grafana logs.

/// Default URL of the Loki instance.
const DEFAULT_URL: &str = "http://loki.parity-versi.parity.io";
/// Default chain to query.
const DEFAULT_CHAIN: &str = "versi-networking";
/// Exclude common errors from the query.
const EXCLUDE_KNOWN_ERRORS: &str = " != `Error while dialing` != `Some security issues have been detected` != `The hardware does not meet`";

pub struct QueryBuilder {
    address: Option<String>,
    chain: Option<String>,
    start_time: Option<String>,
    end_time: Option<String>,
    levels: Vec<String>,
    batch: usize,
    limit: usize,
    exclude_common_errors: bool,
}

impl Default for QueryBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl QueryBuilder {
    /// Create a new QueryBuilder.
    pub fn new() -> Self {
        Self {
            address: None,
            chain: None,
            start_time: None,
            end_time: None,
            levels: Vec::new(),
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

    /// Set the levels of the query.
    ///
    /// Default: empty.
    pub fn levels(mut self, levels: Vec<String>) -> Self {
        self.levels = levels;
        self
    }

    /// Build the query.
    pub fn build(&self) -> String {
        let exclude_common_errors = self
            .exclude_common_errors
            .then_some(EXCLUDE_KNOWN_ERRORS)
            .unwrap_or_default();

        let (start_time, end_time) = match (&self.start_time, &self.end_time) {
            (None, None) => {
                // Compute endtime as now.
                let date_time = chrono::Utc::now();
                let end_time = format!("{}", date_time.format("%Y-%m-%dT%H:%M:%SZ"));
                // Subtract 1 hour from date time
                let date_time = date_time - chrono::Duration::hours(1);
                let start_time = format!("{}", date_time.format("%Y-%m-%dT%H:%M:%SZ"));

                log::debug!("Generating time {} {}", start_time, end_time);
                (start_time, end_time)
            }
            (Some(start_time), Some(end_time)) => {
                log::debug!("Using provided time {start_time} {end_time}");
                (start_time.clone(), end_time.clone())
            }
            _ => {
                panic!("Either both start and end time should be provided or none")
            }
        };

        let levels = (!self.levels.is_empty())
            .then_some(format!(", level=~\"{}\"", self.levels.join("|")))
            .unwrap_or_default();

        let addr = self
            .address
            .as_ref()
            .cloned()
            .unwrap_or(DEFAULT_URL.to_string());
        let chain = self
            .chain
            .as_ref()
            .cloned()
            .unwrap_or(DEFAULT_CHAIN.to_string());

        format!(
            r#"docker run grafana/logcli:main-926a0b2-amd64 query --addr={} --timezone=UTC --from="{}" --to="{}" '{{chain="{}" {}}} {}' --batch {} --limit {}"#,
            addr,
            start_time,
            end_time,
            chain,
            levels,
            exclude_common_errors,
            self.batch,
            self.limit,
        )
    }
}

pub struct QueryRunner;

impl QueryRunner {
    pub fn run(query: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        log::info!("Running query: {}", query);

        let now = std::time::Instant::now();
        let result = std::process::Command::new("sh")
            .arg("-c")
            .arg(query)
            .output()?;
        log::info!("Query completed in {:?}", now.elapsed());

        if !result.status.success() {
            log::error!("Query failed: {}", String::from_utf8_lossy(&result.stderr));
            return Err("Query failed".into());
        }

        log::info!("Query completed successfully");
        Ok(result.stdout)
    }
}
