//! Measure warp sync time.

use clap::Parser as ClapParser;

const MAX_LINES: usize = 1000;

#[derive(Debug, ClapParser, Clone)]
pub struct Config {
    /// Provide a file for parsing.
    #[clap(long)]
    file: String,
}

pub struct WarpTime;

impl WarpTime {
    fn extract_time(line: &str) -> chrono::NaiveDateTime {
        let mut tokens = line.split_whitespace();
        let date = tokens.next().expect("Invalid date");
        let time = tokens.next().expect("Invalid time");
        let current = format!("{} {}", date, time);
        chrono::NaiveDateTime::parse_from_str(&current, "%Y-%m-%d %H:%M:%S.%f")
            .expect("Cannot parse provided time from log line")
    }

    pub fn run(config: Config) -> Result<(), Box<dyn std::error::Error>> {
        log::info!("Running warp time: {:?}", config.file);

        let bytes = std::fs::read(config.file)?;
        let result = String::from_utf8_lossy(&bytes);

        let mut lines = result.lines().take(MAX_LINES);

        // Warping, Downloading finality proofs.
        let start_warp = lines.find(|line| line.contains("Warping, Downloading finality proofs"));
        let start_warp = Self::extract_time(start_warp.expect("Cannot find start warp time"));

        // sync: Warp sync is complete, continuing with state sync.
        let end_warp = lines.find(|line| line.contains("Warp sync is complete"));
        let end_warp = Self::extract_time(end_warp.expect("Cannot find end warp time"));

        // sync: State sync is complete.
        let end_state_sync = lines.find(|line| line.contains("State sync is complete"));
        let end_state_sync =
            Self::extract_time(end_state_sync.expect("Cannot find end state sync time"));

        let warp_time = end_warp.signed_duration_since(start_warp).to_std().unwrap();
        let state_sync_time = end_state_sync
            .signed_duration_since(end_warp)
            .to_std()
            .unwrap();
        let total = end_state_sync
            .signed_duration_since(start_warp)
            .to_std()
            .unwrap();

        println!();
        println!();
        println!("Phase | Time");
        println!(" -|- ");

        println!("Warp  | {:?}", warp_time);
        println!("State | {:?}", state_sync_time);
        println!("Total | {:?}", total);

        println!();
        println!();

        Ok(())
    }
}
