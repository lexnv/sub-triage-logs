use flate2::read::GzDecoder;
use std::io::Read;
use tar::Archive;

/// Fetch the github repo from the provided url and branch.
pub async fn fetch(
    url: String,
    branch: String,
) -> Result<Vec<(String, String)>, Box<dyn std::error::Error>> {
    // Fetch the request.
    let url = format!("{}/archive/{}.tar.gz", url, branch);
    log::info!("Fetching from URL {}", url);
    let now = std::time::Instant::now();

    let body = reqwest::get(url).await?.bytes().await?;

    // Decode the request.
    let decoder = GzDecoder::new(body.as_ref());
    let mut archive = Archive::new(decoder);

    let mut data = Vec::new();
    // Interpret decoded entries.
    let entries = archive.entries()?;
    for entry in entries {
        let mut entry = entry?;

        let path = entry.path()?;
        if path.extension() == Some(std::ffi::OsStr::new("rs")) {
            let owned_path = path.to_string_lossy().into_owned();
            let mut buffer = String::with_capacity(entry.header().size()? as usize);
            entry.read_to_string(&mut buffer)?;
            data.push((owned_path, buffer));
        }
    }

    log::info!("Fetched num files {} in {:?}", data.len(), now.elapsed());

    Ok(data)
}

#[derive(Debug, Clone)]
pub struct RegexDetails {
    pub file: String,
    pub start: usize,
    pub end: usize,
    pub ty: String,
}

fn trim_dummy_chars(ch: char) -> bool {
    ch == '"' || ch == ',' || ch == ' ' || ch == '\\'
}

fn extract_log_line<'a>(mut line: &'a str) -> Option<&'a str> {
    while let Some(in_start) = line.find(',') {
        // Must contain any whitespace followed by quot.
        let in_str = &line[in_start + 1..];

        // Advance for the next search.
        line = &line[in_start + 1..];

        let in_end = in_str.find("\"").unwrap_or_default();

        let to_find_str = &in_str[..in_end];
        if to_find_str.chars().all(|c| c.is_whitespace()) {
            if in_str.len() < in_end + 1 {
                break;
            }

            let new_str = &in_str[in_end + 1..];
            let new_end = new_str.find("\"").unwrap_or_default();
            let mut new_str = &new_str[..new_end];

            // Check for multiline.
            if let Some(new) = new_str.lines().next() {
                new_str = new;
            }

            new_str = new_str.trim_end_matches(trim_dummy_chars);

            log::debug!("Found str line {}", new_str);

            return Some(&new_str);
        }
    }

    // The multiline could not be extracted.
    Some(
        line.trim()
            .trim_end_matches(trim_dummy_chars)
            .trim_start_matches(trim_dummy_chars),
    )
}

pub fn build_regexes(data: Vec<(String, String)>) -> Vec<(regex::Regex, RegexDetails)> {
    let mut regexes = Vec::new();

    for (file_path, content) in data {
        // How the log lines look like.
        let searched_for = ["error!(", "warn!("];

        for searched in searched_for {
            let mut str_content = &content[..];
            let len_searched = searched.len();

            while let Some(start) = str_content.find(searched) {
                let end = if let Some(end) = str_content[start..].find(");") {
                    end
                } else if let Some(end) = str_content[start..].find("),") {
                    end
                } else {
                    // Note: The file must be malformed, don't assume the log ends at the eof.
                    log::error!("File {file_path} is malformed {start}:..");
                    break;
                };

                // str contains everything in between log!( [content] );
                let str = &str_content[start + len_searched..start + end];
                // Advance for the next search.
                str_content = &str_content[start + end..];

                // Handle multiline case.
                let current_str = str;
                let multiline_search = extract_log_line(current_str);
                let Some(line_matched) = multiline_search else {
                    continue;
                };
                if line_matched.is_empty() {
                    continue;
                }

                let mut counting_brackets = 0;
                let mut num_braces = 0;
                let mut line_matched = line_matched
                    .chars()
                    .filter(|c| {
                        if *c == '{' {
                            counting_brackets += 1;
                            num_braces += 1;
                            return true;
                        } else if *c == '}' {
                            counting_brackets -= 1;
                            return true;
                        }

                        counting_brackets == 0
                    })
                    .collect::<String>();
                if counting_brackets > 0 {
                    line_matched.push('}');
                }

                // Only `{}` like lines.
                if line_matched.len() == num_braces * 2 {
                    continue;
                }

                let mut regexed_line = line_matched
                    .replace("{}", ".*")
                    .replace("(", "\\(")
                    .replace(")", "\\)")
                    .replace("[", "\\[")
                    .replace("]", "\\]");

                let has_chars = regexed_line.chars().any(|c| c.is_alphabetic());
                if !has_chars {
                    continue;
                }

                log::debug!("Regexed line {}", regexed_line);
                if regexed_line.len() < 10 {
                    continue;
                }

                // Extra care around misinterpreted lines.
                if regexed_line.starts_with("PoV size") {
                    regexed_line = "PoV size .*".to_string()
                }
                let regex = regex::Regex::new(&regexed_line).unwrap();

                regexes.push((
                    regex,
                    RegexDetails {
                        file: file_path.clone(),
                        start,
                        end,
                        ty: searched[..searched.len() - 2].to_string(),
                    },
                ));
            }
        }
    }

    regexes
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[tokio::test]
    async fn test_inputs() {
        let string = "        log::info!(\"Running panic query\");
        let mut stats = Stats::new();

        log::error!(
            \"Running panic query11\");
    
        log::info!(target: \"bridge\", \"Connecting to {} {}\");
    
        log::warn!(target: \"bridge\",
                            \"Failed to prove {} parachain\");
                            

    warn!(target: LOG_TARGET, \"Missing `per_leaf` for known active\");

    warn!(
                            target: LOG_TARGET,
                            ?session,
                            ?err,
                            \"Could not retrieve session info from RuntimeInfo\",
                        );
    

    error!(
                                    target: LOG_TARGET,
                                    ?session,
                                    ?validator_index,
                                    \"Missing public key for validator\",
                                );
    

    warn!(
                    target: LOG_TARGET,
                    \"Validation code unavailable for code hash {:?} in the state of block {:?}\",
                    req.candidate_receipt().descriptor.validation_code_hash,
                    block_hash,
                );



    warn!(target: LOG_TARGET, \"{peer:?} banned, disconnecting, reason: {}\", reputation_change.reason);";

        let result = build_regexes(vec![("test.rs".to_string(), string.to_string())]);

        let expected: HashSet<_> = [
            // Warns
            "Failed to prove .* parachain",
            "Missing `per_leaf` for known active",
            "Could not retrieve session info from RuntimeInfo",
            "Validation code unavailable for code hash .* in the state of block .*",
            ".* banned, disconnecting, reason: .*",
            // Errors
            "Running panic query11",
            "Missing public key for validator",
        ]
        .into_iter()
        .map(|s| s.to_string())
        .collect();

        let regex_results: HashSet<_> = result.iter().map(|(regex, _)| regex.to_string()).collect();
        assert_eq!(regex_results, expected);
    }
}
