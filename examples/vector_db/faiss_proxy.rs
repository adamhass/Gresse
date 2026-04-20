use gresse::prelude::ServerAddr;
use nix::sys::signal::{kill, Signal};
use nix::unistd::Pid;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use tokio::sync::oneshot;

pub struct FaissProxy {
    shutdown_receiver: Option<oneshot::Receiver<()>>,
    python_process: Child,
}

impl FaissProxy {
    pub fn new(address: ServerAddr, config_path: PathBuf) -> (Self, oneshot::Sender<()>) {
        let python_process = Command::new("python")
            .args([
                "../faiss_server/server.py",
                "--cfg",
                config_path.to_str().unwrap(),
                "--host",
                address.ip.to_string().as_str(),
                "--port",
                address.http_port.to_string().as_str(),
            ])
            .stdin(Stdio::piped())
            .spawn()
            .expect("faiss server process could not be spawned");

        let (shutdown_sender, shutdown_receiver) = oneshot::channel::<()>();
        (
            FaissProxy {
                shutdown_receiver: Some(shutdown_receiver),
                python_process,
            },
            shutdown_sender,
        )
    }

    pub async fn run(&mut self) {
        let shutdown_receiver = self
            .shutdown_receiver
            .take()
            .expect("Failed to take shutdown receiver");

        if shutdown_receiver.await.is_ok() {
            if let Err(e) = kill(
                Pid::from_raw(self.python_process.id() as i32),
                Signal::SIGTERM,
            ) {
                eprintln!("Failed to send SIGTERM: {}", e);
            }
            match self.python_process.wait() {
                Ok(status) => println!("Python process exited with status: {}", status),
                Err(e) => eprintln!("Error waiting for Python process: {}", e),
            }
        }
    }
}
