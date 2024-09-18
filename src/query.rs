//! Query builder and running for fetching the grafana logs.

/// Default URL of the Loki instance.
const DEFAULT_URL: &str = "127.0.0.1:10700";
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
    appended_query: String,
    org_id: Option<String>,
    node: Option<String>,
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
            appended_query: String::new(),
            org_id: None,
            node: None,
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

    /// Append a query to the existing query.
    pub fn append_query(mut self, query: String) -> Self {
        self.appended_query = query;
        self
    }

    /// Set the organization ID.
    ///
    /// Default: None.
    pub fn org_id(mut self, org_id: Option<String>) -> Self {
        self.org_id = org_id;
        self
    }

    /// Set the node name.
    ///
    /// Default: None.
    pub fn node(mut self, node: Option<String>) -> Self {
        self.node = node;
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

        let org_id = self
            .org_id
            .as_ref()
            .map(|org_id| format!(r#" --org-id='{org_id}' "#))
            .unwrap_or_default();

        let node = self
            .node
            .as_ref()
            .map(|node| format!(r#", node=~"{node}" "#))
            .unwrap_or_default();

        let batch = self.batch;
        let limit = self.limit;

        format!(
            r#"logcli query --addr={addr} --timezone=UTC --from="{start_time}" --to="{end_time}" '{{chain="{chain}" {levels} {node}}} {exclude_common_errors}' --batch {batch} --limit {limit} {org_id}"#,
        )
    }

    /// Build the query.
    pub fn build_chunks(&self) -> Vec<String> {
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

        let advance_time = |current: &str| {
            log::debug!("Current time: {}", current);
            let current =
                chrono::NaiveDateTime::parse_from_str(current, "%Y-%m-%dT%H:%M:%SZ").unwrap();
            let current = current + chrono::Duration::hours(1);
            (format!("{}", current.format("%Y-%m-%dT%H:%M:%SZ")), current)
        };

        let value_end_time =
            chrono::NaiveDateTime::parse_from_str(&end_time, "%Y-%m-%dT%H:%M:%SZ").unwrap();
        let (mut end_time_str, mut end_time_date) = advance_time(&start_time);
        let mut start_time_str = start_time;
        let mut start_time_date =
            chrono::NaiveDateTime::parse_from_str(&start_time_str, "%Y-%m-%dT%H:%M:%SZ").unwrap();

        let mut queries = Vec::new();

        let org_id = self
            .org_id
            .as_ref()
            .map(|org_id| format!(r#" --org-id='{}' "#, org_id))
            .unwrap_or_default();

        let batch = self.batch;
        let limit = self.limit;
        let appended_query = &self.appended_query;

        let node = self
            .node
            .as_ref()
            .map(|node| format!(r#", node=~"{node}" "#))
            .unwrap_or_default();

        let build_query = |start_time_str: &str, end_time_str: &str| {
            format!(
                r#"logcli query --addr={addr} --timezone=UTC --from="{start_time_str}" --to="{end_time_str}" '{{chain="{chain}" {levels} {node}}} {exclude_common_errors} {appended_query}' --batch {batch} --limit {limit} {org_id}"#,
            )
        };

        while end_time_date < value_end_time {
            queries.push(build_query(&start_time_str, &end_time_str));

            (start_time_str, start_time_date) = (end_time_str.clone(), end_time_date);
            (end_time_str, end_time_date) = advance_time(&end_time_str);
        }

        if start_time_date < value_end_time {
            queries.push(build_query(&start_time_str, &end_time));
        }

        log::debug!("Queries: {:?}", queries);

        queries
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

        if !result.status.success() {
            log::error!("Query failed: {}", String::from_utf8_lossy(&result.stderr));
            return Err("Query failed".into());
        }

        log::info!("Query completed in {:?}", now.elapsed());
        Ok(result.stdout)
    }
}
