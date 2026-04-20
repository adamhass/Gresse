pub mod causal_order;
pub mod crdt;
pub mod replica;
pub mod db;
pub mod dots;
pub mod http_client;
pub mod http_server;
pub mod network;
pub mod object_storage;
// pub use prelude::*; // Optionally re-export prelude items at the crate root

pub mod prelude {
    use serde::{Deserialize, Serialize};
    use std::net::IpAddr;
    use std::net::SocketAddr;
    use std::time::{SystemTime, UNIX_EPOCH};

    pub type Pid = u64;

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

        pub fn from_args() -> ServerAddr {
            match (
                std::env::var("ADDR"),
                std::env::var("PORT"),
                std::env::var("INTERNAL_PORT"),
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
                    eprintln!(
                        "ADDR or PORT environment variables not found, using default address"
                    );
                    ServerAddr::default()
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
}
