use crate::prelude::{Pid, ServerAddr};
// use crate::prelude::*;
use serde::{de::DeserializeOwned, Serialize};
use std::fmt::Debug;
use tokio::fs::{File, OpenOptions};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::sleep;

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::{
    io::{
        self, AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter, Interest, Lines, ReadHalf,
        WriteHalf,
    },
    sync::mpsc::{channel, Receiver, Sender},
};

/// Network manager for a server, handles the systems internal network connections
/// When it starts it writes its own address to a file and listens for new addresses in the file
/// It also listens for incoming connections and sends out connections to new addresses
pub struct NetworkManager<T> {
    pub local_event_sender: Sender<T>,
    pub connection_sender: Sender<(Pid, Sender<T>)>,
    pub listener: TcpListener,
    pub pid: Pid,
    pub address: ServerAddr,
    pub server_list_file_path: PathBuf,
    // For graceful shutdown
    shutdown_receiver: Option<oneshot::Receiver<()>>,
    task_handles: Vec<JoinHandle<()>>,
}

impl<T: Send + 'static + Serialize + DeserializeOwned + Debug + Sync> NetworkManager<T> {
    pub async fn launch_network_manager(
        address: ServerAddr,
        pid: Pid,
        server_list_file_path: PathBuf,
    ) -> (Receiver<T>, Receiver<(Pid, Sender<T>)>, oneshot::Sender<()>) {
        let (local_event_sender, local_event_receiver) = channel::<T>(100);
        let (connection_sender, connection_receiver) = channel::<(Pid, Sender<T>)>(100);
        let listener = TcpListener::bind(address.internal())
            .await
            .expect("Failed to bind to internal address");
        // Create shutdown channel
        let (shutdown_sender, shutdown_receiver) = oneshot::channel::<()>();

        let mut this = NetworkManager::<T> {
            local_event_sender,
            connection_sender,
            listener,
            pid,
            address,
            server_list_file_path,
            shutdown_receiver: Some(shutdown_receiver),
            task_handles: Vec::new(),
        };
        tokio::spawn(async move {
            this.run().await;
        });
        (local_event_receiver, connection_receiver, shutdown_sender)
    }

    pub async fn run(&mut self) {
        let mut address_receiver = self.init_server_file_monitor().await;

        // Take ownership of shutdown_receiver from self
        let mut shutdown_receiver = self
            .shutdown_receiver
            .take()
            .expect("Failed to take shutdown receiver");

        loop {
            tokio::select! {
                Ok((stream, _)) = self.listener.accept() => {self.handle_new_stream(stream).await},
                Some(address) = address_receiver.recv() => {
                    self.handle_new_address(address).await;
                }
                Ok(()) = &mut shutdown_receiver => {
                    break;
                }
            }
        }
        for handle in self.task_handles.iter() {
            handle.abort();
        }
        println!("NetworkManager for pid {} shutdown complete", self.pid);
    }

    pub async fn handle_new_address(&mut self, address: SocketAddr) {
        // Ensure there's no self connection, or mutual connection attempts
        if address == self.address.internal() || address < self.address.internal() {
            return;
        }
        println!("New address added: {:?}", address);
        let stream = TcpStream::connect(address)
            .await
            .expect("Failed to connect to new address");
        self.handle_new_stream(stream).await;
    }

    /// This method writes its own internal address to the server list file and returns a reader for monitoring
    async fn init_server_file_monitor(&mut self) -> Receiver<SocketAddr> {
        // Ensure the directory exists
        if let Some(parent) = Path::new(&self.server_list_file_path).parent() {
            std::fs::create_dir_all(parent).expect("Failed to create directories");
        }

        // Open or create the file asynchronously
        let file = match OpenOptions::new()
            .create(true) // Create the file if it doesn't exist
            .write(true) // Allow writing to the file
            .read(false) // Allow reading from the file (if needed)
            .truncate(false)
            .append(true)
            .open(&self.server_list_file_path) // Open the file (won't truncate if it exists)
            .await
        {
            Ok(file) => file,
            Err(e) => {
                panic!(
                    "Failed to open file {}: {}",
                    self.server_list_file_path.display(),
                    e
                );
            }
        };
        let mut writer = BufWriter::new(file);
        let _ = writer
            .write(self.address.internal().to_string().as_bytes())
            .await
            .expect("Failed to write address");
        let _ = writer.write(b"\n").await.expect("Failed to write newline");
        writer.flush().await.expect("Failed to flush");
        drop(writer);
        let file = match File::open(&self.server_list_file_path).await {
            Ok(file) => file,
            Err(e) => {
                panic!(
                    "Failed to open file {}: {}",
                    self.server_list_file_path.display(),
                    e
                );
            }
        };

        let line_reader = BufReader::new(file).lines();
        let (address_sender, address_receiver) = channel::<SocketAddr>(10);
        let handle = tokio::spawn(async move {
            Self::file_reader_loop(address_sender, line_reader).await;
        });
        self.task_handles.push(handle);
        address_receiver
    }

    /// This method can be used both for incoming and outgoing streams
    async fn handle_new_stream(&mut self, mut stream: TcpStream) {
        // Tell the stream who we are
        stream
            .write_u128(self.pid)
            .await
            .expect("Failed to write id");
        // Find out who it is on the opposite end
        let pid = stream.read_u128().await.expect("Failed to read id");
        println!(
            "{:?} Received neighbor connection from: {:?}",
            self.pid, pid
        );
        while let Err(e) = stream.ready(Interest::READABLE | Interest::WRITABLE).await {
            eprintln!("Failed to initialize stream: {:?}, retrying", e);
        }
        let (read_half, stream_writer) = io::split(stream);
        let stream_reader = BufReader::new(read_half).lines();
        let (from_local_sender, from_local_receiver) = channel::<T>(100);
        let sender_clone = self.local_event_sender.clone();
        let handle = tokio::spawn(async move {
            Self::read_loop(stream_reader, sender_clone).await;
        });
        self.task_handles.push(handle);
        let handle = tokio::spawn(async move {
            Self::write_loop(stream_writer, from_local_receiver).await;
        });
        self.task_handles.push(handle);
        self.connection_sender
            .send((pid as Pid, from_local_sender))
            .await
            .expect("Failed to send connection");
    }

    async fn read_loop(mut reader: Lines<BufReader<ReadHalf<TcpStream>>>, sender: Sender<T>) {
        // println!("Starting read loop");
        loop {
            // println!("Looping read loop");
            while let Ok(Some(line)) = reader.next_line().await {
                let message: T = serde_json::from_str(&line).expect("Failed to parse request");
                // println!("Received request: {:?}", message);
                sender.send(message).await.expect("Failed to send request");
            }
        }
    }

    pub async fn send_message(
        writer: &mut WriteHalf<TcpStream>,
        message: &T,
    ) -> tokio::io::Result<()> {
        let json = serde_json::to_string(message).unwrap();
        writer.write_all(json.as_bytes()).await?;
        writer.write_all(b"\n").await?;
        Ok(())
    }

    async fn write_loop(mut writer: WriteHalf<TcpStream>, mut receiver: Receiver<T>) {
        loop {
            while let Some(request) = receiver.recv().await {
                // println!("Sending request: {:?}", request);
                Self::send_message(&mut writer, &request)
                    .await
                    .expect("Failed to send message");
            }
        }
    }

    async fn file_reader_loop(sender: Sender<SocketAddr>, mut reader: Lines<BufReader<File>>) {
        loop {
            while let Ok(Some(line)) = reader.next_line().await {
                if !line.is_empty() {
                    println!("New line added: {}", line);
                    let addr: SocketAddr = line.parse().expect("Failed to parse address");
                    sender.send(addr).await.unwrap();
                }
                sleep(Duration::from_secs(1)).await; // Avoid busy looping
            }
        }
    }
}
