use anyhow::{Context, Result};
use rayon::prelude::*;
use regex::Regex;
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

use crate::storage::FunctionInfo;

#[derive(Debug)]
pub struct SearchMatch {
    pub file: String,
    pub line: usize,
    pub content: String,
}

pub fn search_regex(crate_path: &Path, pattern: &str) -> Result<Vec<SearchMatch>> {
    let regex = Regex::new(pattern).with_context(|| format!("Invalid regex: {}", pattern))?;

    // Collect all .rs files first
    let files: Vec<(PathBuf, String)> = WalkDir::new(crate_path)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "rs"))
        .map(|entry| {
            let file_path = entry.path().to_path_buf();
            let relative_path = file_path
                .strip_prefix(crate_path)
                .unwrap_or(&file_path)
                .to_string_lossy()
                .to_string();
            (file_path, relative_path)
        })
        .collect();

    // Search files in parallel
    let matches: Vec<SearchMatch> = files
        .par_iter()
        .flat_map(|(file_path, relative_path)| {
            search_file(file_path, relative_path, &regex)
        })
        .collect();

    Ok(matches)
}

fn search_file(file_path: &Path, relative_path: &str, regex: &Regex) -> Vec<SearchMatch> {
    let mut matches = Vec::new();

    if let Ok(file) = fs::File::open(file_path) {
        let reader = BufReader::new(file);

        for (line_num, line_result) in reader.lines().enumerate() {
            if let Ok(line) = line_result {
                if regex.is_match(&line) {
                    matches.push(SearchMatch {
                        file: relative_path.to_string(),
                        line: line_num + 1,
                        content: line.trim().to_string(),
                    });
                }
            }
        }
    }

    matches
}

pub fn search_functions(functions: &[FunctionInfo], pattern: Option<&str>) -> Result<Vec<FunctionInfo>> {
    let regex = pattern
        .map(|p| Regex::new(p))
        .transpose()
        .with_context(|| "Invalid regex pattern")?;

    let matches: Vec<FunctionInfo> = functions
        .par_iter()
        .filter(|func| {
            regex
                .as_ref()
                .map(|r| r.is_match(&func.name) || r.is_match(&func.signature))
                .unwrap_or(true)
        })
        .cloned()
        .collect();

    Ok(matches)
}
