use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Database connection URL
    pub database_url: String,

    /// HTTP API port (default: 8080, 0 to disable)
    #[serde(default = "default_port")]
    pub port: u16,

    /// Bind address (default: 0.0.0.0)
    #[serde(default = "default_bind")]
    pub bind: String,

    /// Metrics port (default: 9090, 0 to disable)
    #[serde(default = "default_metrics_port")]
    pub metrics_port: u16,

    /// Chains to index
    pub chains: Vec<ChainConfig>,
}

fn default_port() -> u16 {
    8080
}

fn default_bind() -> String {
    "0.0.0.0".to_string()
}

fn default_metrics_port() -> u16 {
    9090
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChainConfig {
    /// Chain name (for display/logging)
    pub name: String,

    /// Chain ID
    pub chain_id: u64,

    /// RPC URL
    pub rpc_url: String,

    /// Enable backfill to genesis (default: true)
    #[serde(default = "default_backfill")]
    pub backfill: bool,

    /// Batch size for RPC requests (default: 100)
    #[serde(default = "default_batch_size")]
    pub batch_size: u64,
}

fn default_backfill() -> bool {
    true
}

fn default_batch_size() -> u64 {
    100
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file: {}", path.display()))?;

        let config: Config = toml::from_str(&content)
            .with_context(|| format!("Failed to parse config file: {}", path.display()))?;

        if config.chains.is_empty() {
            anyhow::bail!("No chains configured. Add at least one [[chains]] section.");
        }

        Ok(config)
    }
}
