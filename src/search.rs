use anyhow::{Context, Result};
use rayon::prelude::*;
use regex::{Regex, RegexBuilder};
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

use crate::storage::FunctionInfo;

/// Build a regex with size limits to prevent ReDoS attacks
pub fn build_regex(pattern: &str) -> Result<Regex> {
    RegexBuilder::new(pattern)
        .size_limit(1024 * 1024) // 1MB compiled size limit
        .dfa_size_limit(1024 * 1024) // 1MB DFA cache limit
        .build()
        .with_context(|| format!("Invalid or too complex regex: {}", pattern))
}

#[derive(Debug)]
pub struct SearchMatch {
    pub file: String,
    pub line: usize,
    pub content: String,
}

pub fn search_regex(crate_path: &Path, pattern: &str) -> Result<Vec<SearchMatch>> {
    let regex = build_regex(pattern)?;

    // Collect all .rs files first
    let files: Vec<(PathBuf, String)> = WalkDir::new(crate_path)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "rs"))
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
    let regex = pattern.map(|p| build_regex(p)).transpose()?;

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

#[derive(Debug, Clone)]
pub struct SemanticSearchResult {
    pub item_id: String,
    pub item_type: String,
    pub similarity: f32,
    pub text_content: String,
    pub crate_key: String,
}
