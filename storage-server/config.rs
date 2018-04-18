use std::net::SocketAddr;

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct Config {
    #[serde(default)]
    pub log: LogConfig,

    pub replication: ReplicationConfig,

    #[serde(default)]
    pub frontend: Option<FrontendConfig>,

    #[serde(default)]
    pub admin: Option<AdminConfig>,
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct LogConfig {
    pub dir: String,
}

impl Default for LogConfig {
    fn default() -> LogConfig {
        LogConfig {
            dir: ".log".to_string(),
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
                },
                frontend: Some(FrontendConfig {
                    server_addr: "0.0.0.0:8080".parse().unwrap(),
                }),
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
    "#,
        ).unwrap();

        assert_eq!(
            Config {
                log: LogConfig {
                    dir: ".log".to_string(),
                },
                frontend: None,
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
