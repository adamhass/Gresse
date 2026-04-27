use crate::dots::Counter;
use crate::prelude::{Pid, ServerAddr};
// use crate::prelude::*;
use serde::{de::DeserializeOwned, Serialize};
use std::fmt::Debug;
use std::collections::HashSet;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use std::net::SocketAddr;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::{
    io::{self, AsyncBufReadExt, AsyncWriteExt, BufReader, Interest, Lines, ReadHalf, WriteHalf},
    sync::mpsc::{channel, Receiver, Sender},
};

#[derive(Debug, Clone, Copy)]
pub struct NetworkMember {
    pub pid: Pid,
    pub address: SocketAddr,
    pub final_counter: Option<Counter>,
}

impl NetworkMember {
    pub fn is_shutdown(&self) -> bool {
        self.final_counter.is_some()
    }
}

/// Network manager for a server, handles the systems internal network connections
/// It listens for incoming connections and connects to members received from the replica.
pub struct NetworkManager<T> {
    pub local_event_sender: Sender<T>,
    pub connection_sender: Sender<(Pid, Sender<T>)>,
    pub member_receiver: Receiver<NetworkMember>,
    pub listener: TcpListener,
    pub pid: Pid,
    pub address: ServerAddr,
    dead_members: HashSet<Pid>,
    // For graceful shutdown
    shutdown_receiver: Option<oneshot::Receiver<()>>,
    task_handles: Vec<JoinHandle<()>>,
}

impl<T: Send + 'static + Serialize + DeserializeOwned + Debug + Sync> NetworkManager<T> {
    pub async fn launch_network_manager(
        address: ServerAddr,
        pid: Pid,
    ) -> (
        Receiver<T>,
        Receiver<(Pid, Sender<T>)>,
        Sender<NetworkMember>,
        oneshot::Sender<()>,
    ) {
        let (local_event_sender, local_event_receiver) = channel::<T>(100);
        let (connection_sender, connection_receiver) = channel::<(Pid, Sender<T>)>(100);
        let (member_sender, member_receiver) = channel::<NetworkMember>(100);
        let listener = TcpListener::bind(address.internal())
            .await
            .expect("Failed to bind to internal address");
        // Create shutdown channel
        let (shutdown_sender, shutdown_receiver) = oneshot::channel::<()>();

        let mut this = NetworkManager::<T> {
            local_event_sender,
            connection_sender,
            member_receiver,
            listener,
            pid,
            address,
            dead_members: HashSet::new(),
            shutdown_receiver: Some(shutdown_receiver),
            task_handles: Vec::new(),
        };
        tokio::spawn(async move {
            this.run().await;
        });
        (
            local_event_receiver,
            connection_receiver,
            member_sender,
            shutdown_sender,
        )
    }

    pub async fn run(&mut self) {
        // Take ownership of shutdown_receiver from self
        let mut shutdown_receiver = self
            .shutdown_receiver
            .take()
            .expect("Failed to take shutdown receiver");

        loop {
            tokio::select! {
                Ok((stream, _)) = self.listener.accept() => {self.handle_new_stream(stream).await},
                Some(member) = self.member_receiver.recv() => {
                    self.handle_new_member(member).await;
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

    pub async fn handle_new_member(&mut self, member: NetworkMember) {
        if member.is_shutdown() {
            self.dead_members.insert(member.pid);
            return;
        }

        // Ensure there's no self connection, or mutual connection attempts
        if member.pid == self.pid
            || member.address == self.address.internal()
            || member.address < self.address.internal()
            || self.dead_members.contains(&member.pid)
        {
            return;
        }
        println!("New member added: {:?}", member);
        let stream = TcpStream::connect(member.address)
            .await
            .expect("Failed to connect to new address");
        self.handle_new_stream(stream).await;
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
        if self.dead_members.contains(&pid) {
            return;
        }
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
}
