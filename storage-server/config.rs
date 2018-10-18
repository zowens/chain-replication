use std::net::SocketAddr;
use std::usize;

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct Config {
    #[serde(default)]
    pub log: LogConfig,

    pub replication: ReplicationConfig,

    pub frontend: FrontendConfig,

    #[serde(default)]
    pub admin: Option<AdminConfig>,
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct LogConfig {
    #[serde(default = "log_default_dir")]
    pub dir: String,

    #[serde(default = "log_default_index_max_items")]
    pub index_max_items: usize,

    #[serde(default = "log_default_segment_max_bytes")]
    pub segment_max_bytes: usize,

    #[serde(default = "log_default_message_max_bytes")]
    pub message_max_bytes: usize,

    #[serde(default = "log_default_message_buffer_bytes")]
    pub message_buffer_bytes: usize,

    #[serde(default = "log_default_replication_max_bytes")]
    pub replication_max_bytes: usize,
}

fn log_default_dir() -> String {
    ".log".to_string()
}

fn log_default_index_max_items() -> usize {
    10_000_000
}

fn log_default_segment_max_bytes() -> usize {
    1_073_741_824
}

fn log_default_message_max_bytes() -> usize {
    1_048_576
}

fn log_default_message_buffer_bytes() -> usize {
    1_048_576
}

fn log_default_replication_max_bytes() -> usize {
    2_097_152
}

impl Default for LogConfig {
    fn default() -> LogConfig {
        LogConfig {
            dir: log_default_dir(),
            index_max_items: log_default_index_max_items(),
            segment_max_bytes: log_default_segment_max_bytes(),
            message_max_bytes: log_default_message_max_bytes(),
            message_buffer_bytes: log_default_message_buffer_bytes(),
            replication_max_bytes: log_default_replication_max_bytes(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct FrontendConfig {
    pub server_addr: SocketAddr,
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct ReplicationConfig {
    pub server_addr: SocketAddr,
    pub upstream_addr: Option<SocketAddr>,
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct AdminConfig {
    pub server_addr: SocketAddr,
}

#[cfg(test)]
mod tests {
    use super::*;
    use toml;

    #[test]
    fn full_config() {
        let decoded: Config = toml::from_str(
            r#"
        [log]
        dir = "foo"
        index_max_items = 10
        segment_max_bytes = 1000
        message_max_bytes = 100
        message_buffer_bytes = 10000
        replication_max_bytes = 200

        [frontend]
        server_addr = "0.0.0.0:8080"

        [replication]
        server_addr = "0.0.0.0:8081"
        upstream_addr = "0.0.0.0:4000"
    "#,
        )
        .unwrap();

        assert_eq!(
            Config {
                log: LogConfig {
                    dir: "foo".to_string(),
                    index_max_items: 10,
                    segment_max_bytes: 1_000,
                    message_max_bytes: 100,
                    message_buffer_bytes: 10_000,
                    replication_max_bytes: 200,
                },
                frontend: FrontendConfig {
                    server_addr: "0.0.0.0:8080".parse().unwrap(),
                },
                replication: ReplicationConfig {
                    server_addr: "0.0.0.0:8081".parse().unwrap(),
                    upstream_addr: Some("0.0.0.0:4000".parse().unwrap()),
                },
                admin: None,
            },
            decoded
        )
    }

    #[test]
    fn defaulted_config() {
        let decoded: Config = toml::from_str(
            r#"
        [replication]
        server_addr = "0.0.0.0:8081"
        upstream_addr = "0.0.0.0:4000"

        [frontend]
        server_addr = "0.0.0.0:8080"
    "#,
        )
        .unwrap();

        assert_eq!(
            Config {
                log: LogConfig {
                    dir: ".log".to_string(),
                    index_max_items: 10_000_000,
                    segment_max_bytes: 1_073_741_824,
                    message_max_bytes: 1_048_576,
                    message_buffer_bytes: 1_048_576,
                    replication_max_bytes: 2_097_152,
                },
                frontend: FrontendConfig {
                    server_addr: "0.0.0.0:8080".parse().unwrap(),
                },
                replication: ReplicationConfig {
                    server_addr: "0.0.0.0:8081".parse().unwrap(),
                    upstream_addr: Some("0.0.0.0:4000".parse().unwrap()),
                },
                admin: None,
            },
            decoded
        )
    }
}
