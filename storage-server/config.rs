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
    pub dir: String,
    pub index_max_items: usize,
    pub segment_max_bytes: usize,
    pub message_max_bytes: usize,
}

impl Default for LogConfig {
    fn default() -> LogConfig {
        LogConfig {
            dir: ".log".to_string(),
            index_max_items: 10_000_000,
            segment_max_bytes: 1_000_000_000,
            message_max_bytes: usize::MAX,
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

        [frontend]
        server_addr = "0.0.0.0:8080"

        [replication]
        server_addr = "0.0.0.0:8081"
        upstream_addr = "0.0.0.0:4000"
    "#,
        ).unwrap();

        assert_eq!(
            Config {
                log: LogConfig {
                    dir: "foo".to_string(),
                    index_max_items: 10,
                    segment_max_bytes: 1_000,
                    message_max_bytes: 100,
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
        ).unwrap();

        assert_eq!(
            Config {
                log: LogConfig {
                    dir: ".log".to_string(),
                    index_max_items: 10_000_000,
                    segment_max_bytes: 1_000_000_000,
                    message_max_bytes: usize::MAX,
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
