pub mod crdt;
pub mod db;
pub mod dots;
pub mod http_client;
pub(crate) mod http_server;
pub(crate) mod journal;
pub mod logging;
pub(crate) mod metrics;
pub(crate) mod network;
pub(crate) mod object_storage;
pub mod orset;
pub mod replica;
pub mod replica_helpers;

pub mod prelude {
    use rand::Rng;
    use serde::{Deserialize, Serialize};
    use std::net::IpAddr;
    use std::net::SocketAddr;
    use std::path::PathBuf;
    use std::time::Duration;
    use std::time::{SystemTime, UNIX_EPOCH};

    pub type Pid = u128;

    pub fn new_pid() -> Pid {
        rand::rng().random()
    }

    #[derive(Debug, Serialize, Deserialize, Clone, Copy)]
    pub struct ServerAddr {
        pub ip: IpAddr,
        pub http_port: u16,
        pub internal_port: u16,
    }

    impl ServerAddr {
        pub fn http(&self) -> SocketAddr {
            SocketAddr::new(self.ip, self.http_port)
        }
        pub fn internal(&self) -> SocketAddr {
            SocketAddr::new(self.ip, self.internal_port)
        }

        /// Keep the configured ports while replacing the bind/published IP.
        pub fn with_ip(self, ip: IpAddr) -> ServerAddr {
            ServerAddr { ip, ..self }
        }

        pub fn from_args() -> ServerAddr {
            Self::from_env()
        }

        pub fn from_env() -> ServerAddr {
            match (
                std::env::var("GRESSE_ADDR"),
                std::env::var("GRESSE_HTTP_PORT"),
                std::env::var("GRESSE_INTERNAL_PORT"),
            ) {
                (Ok(ip), Ok(http_port), Ok(internal_port)) => {
                    let ip: std::net::IpAddr = ip.parse().expect("Failed to parse IP");
                    let http_port: u16 = http_port.parse().expect("Failed to parse port");
                    let internal_port: u16 = internal_port.parse().expect("Failed to parse port");
                    ServerAddr {
                        ip,
                        http_port,
                        internal_port,
                    }
                }
                _ => {
                    panic!(
                        "GRESSE_ADDR, GRESSE_HTTP_PORT, and GRESSE_INTERNAL_PORT environment variables are required"
                    );
                }
            }
        }

        /// returns a new copy with incremented ports
        pub fn increment_ports(&self, increment: u16) -> ServerAddr {
            ServerAddr {
                ip: self.ip,
                http_port: self.http_port + increment,
                internal_port: self.internal_port + increment,
            }
        }
    }

    impl Default for ServerAddr {
        fn default() -> Self {
            ServerAddr {
                ip: "0.0.0.0".parse().unwrap(),
                http_port: 9090,
                internal_port: 8080,
            }
        }
    }

    pub fn now_micros() -> u128 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros()
    }

    #[derive(Clone, Debug)]
    pub struct ObjectStorageConfig {
        pub local_dir: Option<PathBuf>,
        pub url: Option<String>,
        pub region: String,
        pub bucket: String,
        pub access_key: Option<String>,
        pub secret_key: Option<String>,
        pub session_token: Option<String>,
        pub persistent_replica_path: String,
        pub membership_directory_path: String,
        pub discovery_interval: Duration,
    }
}
