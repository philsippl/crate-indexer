use anyhow::{bail, Context, Result};
use flate2::read::GzDecoder;
use reqwest::blocking::Client;
use serde::Deserialize;
use std::fs;
use std::io::Cursor;
use std::path::PathBuf;
use tar::Archive;

use crate::storage::crate_path;

#[derive(Debug, Deserialize)]
struct CrateResponse {
    #[serde(rename = "crate")]
    crate_info: CrateMetadata,
}

#[derive(Debug, Deserialize)]
struct CrateMetadata {
    max_stable_version: Option<String>,
    max_version: String,
}

pub struct Fetcher {
    client: Client,
}

impl Fetcher {
    pub fn new() -> Result<Self> {
        let client = Client::builder()
            .user_agent("crate-indexer/0.1.0")
            .build()?;
        Ok(Self { client })
    }

    pub fn get_latest_version(&self, crate_name: &str) -> Result<String> {
        let url = format!("https://crates.io/api/v1/crates/{}", crate_name);
        let response: CrateResponse = self
            .client
            .get(&url)
            .send()
            .with_context(|| format!("Failed to fetch crate info for {}", crate_name))?
            .json()
            .with_context(|| "Failed to parse crate metadata")?;

        Ok(response
            .crate_info
            .max_stable_version
            .unwrap_or(response.crate_info.max_version))
    }

    pub fn fetch_crate(&self, crate_name: &str, version: &str) -> Result<PathBuf> {
        let dest_path = crate_path(crate_name, version);

        if dest_path.exists() {
            println!("Crate {} v{} already downloaded", crate_name, version);
            return Ok(dest_path);
        }

        let url = format!(
            "https://static.crates.io/crates/{}/{}-{}.crate",
            crate_name, crate_name, version
        );

        println!("Downloading {} v{} from crates.io...", crate_name, version);

        let response = self
            .client
            .get(&url)
            .send()
            .with_context(|| format!("Failed to download crate from {}", url))?;

        if !response.status().is_success() {
            bail!(
                "Failed to download crate: HTTP {}",
                response.status().as_u16()
            );
        }

        let bytes = response
            .bytes()
            .with_context(|| "Failed to read response body")?;

        println!("Extracting to {:?}...", dest_path);
        self.extract_crate(&bytes, &dest_path, crate_name, version)?;

        Ok(dest_path)
    }

    fn extract_crate(
        &self,
        bytes: &[u8],
        dest_path: &PathBuf,
        crate_name: &str,
        version: &str,
    ) -> Result<()> {
        fs::create_dir_all(dest_path)?;

        let cursor = Cursor::new(bytes);
        let gz = GzDecoder::new(cursor);
        let mut archive = Archive::new(gz);

        let prefix = format!("{}-{}/", crate_name, version);

        for entry in archive.entries()? {
            let mut entry = entry?;
            let path = entry.path()?;
            let path_str = path.to_string_lossy();

            if let Some(stripped) = path_str.strip_prefix(&prefix) {
                if !stripped.is_empty() {
                    let dest = dest_path.join(stripped);
                    if let Some(parent) = dest.parent() {
                        fs::create_dir_all(parent)?;
                    }
                    entry.unpack(&dest)?;
                }
            }
        }

        Ok(())
    }
}
