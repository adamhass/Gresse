// use std::collections::HashMap;

// use serde::{Deserialize, Serialize};

// use crate::prelude::Pid;

// pub struct CausalOrderBuffer<T: VC> {
//     pub buffer: Vec<(T)>,
//     pub vc: VectorClock,
// }

// impl<T: VC> CausalOrderBuffer<T> {
//     pub fn new(pid: Pid) -> Self {
//         CausalOrderBuffer {
//             buffer: Vec::new(),
//             vc: VectorClock::new(pid),
//         }
//     }

//     pub fn is_deliverable(&self, message: &T) -> bool {
//         if self.vc.is_deliverable(&message.get_vc()) {
//             self.vc.receive_event(&message.get_vc());
//             true
//         }
//         false
//     }

//     /// Retrieves the next deliverable message from the buffer and increments the VC.
//     pub fn get_next(&self) -> Option<T> {
//         let mut i = 0;
//         while i < self.buffer.len() {
//             let message = &self.buffer[i];
//             if self.vc.is_deliverable(&message.vc) {
//                 self.vc.receive_event(&message.vc);
//                 // Remove from buffer and deliver
//                 let message = self.buffer.remove(i);
//                 self.handle_remote_event(message).await;
//                 // Reset index to re-check from the start
//                 i = 0;
//             } else {
//                 i += 1;
//             }
//         }
//     }
// }

// pub trait VC {
//     fn get_vc(&self) -> VectorClock;
// }

// /// A vector clock implementation for distributed systems.
// #[derive(Clone, Debug, Serialize, Deserialize)]
// pub struct VectorClock {
//     /// The internal clock represented as a hashmap of process IDs to timestamps.
//     clock: HashMap<Pid, u64>,
//     /// The unique identifier for this process.
//     pid: Pid,
// }

// impl VectorClock {
//     /// Creates a new vector clock for a process with the given process ID.
//     ///
//     /// # Arguments
//     ///
//     /// * `pid` - The unique identifier for this process.
//     pub fn new(pid: Pid) -> Self {
//         let mut clock = HashMap::new();
//         clock.insert(pid, 0);
//         VectorClock { clock, pid }
//     }

//     /// Increments the vector clock for an internal event.
//     pub fn increment(&mut self) {
//         *self.clock.entry(self.pid).or_insert(0) += 1;
//     }

//     /// Handles the sending of a message by incrementing the clock and returning a copy.
//     ///
//     /// # Returns
//     ///
//     /// * A copy of the current vector clock to be sent with the message.
//     pub fn send_event(&mut self) -> VectorClock {
//         self.increment();
//         self.clone()
//     }

//     /// Handles the receipt of a message by updating the vector clock.
//     ///
//     /// # Arguments
//     ///
//     /// * `received_clock` - The vector clock received with the message.
//     pub fn receive_event(&mut self, received_clock: &VectorClock) {
//         // self.increment();
//         for (&pid, &timestamp) in received_clock.clock.iter() {
//             let entry = self.clock.entry(pid).or_insert(0);
//             *entry = (*entry).max(timestamp);
//         }
//     }

//     /// Determines if a message is deliverable according to the causal delivery condition.
//     pub fn is_deliverable(&self, message_vc: &VectorClock) -> bool {
//         for (&pid, &time) in message_vc.clock.iter() {
//             let local_time = *self.clock.get(&pid).unwrap_or(&0);
//             if pid == message_vc.pid {
//                 if time != local_time + 1 {
//                     return false;
//                 }
//             } else if time > local_time {
//                 return false;
//             }
//         }
//         true
//     }

//     /// Prints the current state of the vector clock (useful for debugging).
//     pub fn print_clock(&self) {
//         println!("Process {}: {:?}", self.pid, self.clock);
//     }

//     /// Compares this vector clock with another to determine if it is causally before, after, or concurrent.
//     ///
//     /// # Arguments
//     ///
//     /// * `other` - The other vector clock to compare with.
//     ///
//     /// # Returns
//     ///
//     /// * `Ordering` - Less, Greater, or Equal if causally before, after, or concurrent.
//     pub fn compare(&self, other: &VectorClock) -> std::cmp::Ordering {
//         use std::cmp::Ordering;

//         let mut less = false;
//         let mut greater = false;

//         let all_pids: std::collections::HashSet<_> = self
//             .clock
//             .keys()
//             .chain(other.clock.keys())
//             .cloned()
//             .collect();

//         for pid in all_pids {
//             let self_time = self.clock.get(&pid).unwrap_or(&0);
//             let other_time = other.clock.get(&pid).unwrap_or(&0);

//             if self_time < other_time {
//                 less = true;
//             } else if self_time > other_time {
//                 greater = true;
//             }
//         }

//         match (less, greater) {
//             (true, false) => Ordering::Less, // This clock is causally before the other.
//             (false, true) => Ordering::Greater, // This clock is causally after the other.
//             (false, false) => Ordering::Equal, // Clocks are equal.
//             (true, true) => Ordering::Equal, // Clocks are concurrent.
//         }
//     }
// }
