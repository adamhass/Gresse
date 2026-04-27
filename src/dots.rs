use crate::prelude::Pid;
use serde::{Deserialize, Serialize};
use std::{cmp::Ordering, collections::HashMap};

pub type Counter = i64;

#[derive(Debug, Clone, Serialize, Deserialize, Copy, PartialEq, PartialOrd, Eq, Ord)]
pub struct Dot {
    pub pid: Pid,
    pub counter: Counter,
}

impl From<(&Pid, &Counter)> for Dot {
    fn from(tuple: (&Pid, &Counter)) -> Self {
        Dot {
            pid: *tuple.0,
            counter: *tuple.1,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DotSet {
    set: HashMap<Pid, Counter>,
}

impl PartialEq for DotSet {
    fn eq(&self, other: &Self) -> bool {
        self.set == other.set
    }
}

impl Eq for DotSet {}

impl PartialOrd for DotSet {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DotSet {
    fn cmp(&self, other: &Self) -> Ordering {
        let mut has_lower = false;
        let mut has_greater = false;
        for (pid, counter) in self.set.iter() {
            let other_counter = other.get(pid);
            if counter > other_counter {
                has_greater = true;
            } else if counter < other_counter {
                has_lower = true;
            }
        }
        if has_lower && has_greater {
            Ordering::Equal
        } else if has_lower {
            return Ordering::Less;
        } else {
            return Ordering::Greater;
        }
    }
}

impl Default for DotSet {
    fn default() -> Self {
        Self::new()
    }
}

impl DotSet {
    pub fn new() -> Self {
        Self {
            set: HashMap::new(),
        }
    }

    pub fn merge(&mut self, other: &DotSet) {
        for (pid, counter) in other.set.iter() {
            self.set
                .entry(*pid)
                .and_modify(|c| *c = (*c).max(*counter))
                .or_insert(*counter);
        }
    }

    pub fn set_counter(&mut self, pid: Pid, counter: Counter) {
        self.set.insert(pid, counter);
    }

    pub fn remove_pid(&mut self, pid: &Pid) {
        self.set.remove(pid);
    }

    pub fn pids(&self) -> impl Iterator<Item = Pid> + '_ {
        self.set.keys().copied()
    }

    pub fn counter(&self, pid: &Pid) -> Option<Counter> {
        self.set.get(pid).copied()
    }

    pub fn insert(&mut self, dot: &Dot) {
        if let Some(previous_value) = self.set.insert(dot.pid, dot.counter) {
            assert!(previous_value + 1 == dot.counter, "DotSet in invalid state");
        } else {
            assert!(
                dot.counter == 0,
                "DotSet in invalid state, haven't received the first dot"
            );
        }
    }

    pub fn increment_and_get(&mut self, pid: Pid) -> Dot {
        if let Some(count) = self.set.get_mut(&pid) {
            *count += 1;
            Dot {
                pid,
                counter: *count,
            }
        } else {
            let dot = Dot { pid, counter: 0 };
            self.insert(&dot);
            dot
        }
    }

    pub fn get(&self, pid: &Pid) -> &Counter {
        self.set.get(pid).unwrap_or(&0)
    }

    /// Returns true iff self and other are concurrent
    pub fn is_concurrent_with(&self, other: &DotSet) -> bool {
        self.cmp(other) == Ordering::Equal
    }

    /// Returns true iff self contains the dot
    pub fn contains(&self, dot: &Dot) -> bool {
        if let Some(this_counter) = self.set.get(&dot.pid) {
            *this_counter >= dot.counter
        } else {
            false
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VersionMatrix {
    matrix: HashMap<Pid, DotSet>,
    final_dots: Vec<Dot>,
}

impl VersionMatrix {
    pub fn new() -> Self {
        Self {
            matrix: HashMap::new(),
            final_dots: Vec::new(),
        }
    }

    pub fn update(&mut self, pid: Pid, version_vector: DotSet) {
        self.matrix.insert(pid, version_vector);
    }

    /// Returns a DotSet 
    pub fn get_stable(&self) -> DotSet {
        let mut stable = DotSet::new();
        for pid in self.pids() {
            let counter = self
                .matrix
                .values()
                .map(|version_vector| version_vector.counter(&pid).unwrap_or(-1))
                .min()
                .unwrap_or(-1);
            stable.set_counter(pid, counter);
        }
        stable
    }

    fn pids(&self) -> impl Iterator<Item = Pid> + '_ {
        let row_pids = self.matrix.keys().copied();
        let column_pids = self
            .matrix
            .values()
            .flat_map(|version_vector| version_vector.pids());
        row_pids.chain(column_pids)
    }

    /// We must wait for final count for this Pid to stabilize before we can remove it completely
    pub fn insert_final_dot(&mut self, final_dot: Dot) {
        self.matrix
            .entry(final_dot.pid)
            .and_modify(|version_vector| version_vector.set_counter(final_dot.pid, final_dot.counter))
            .or_insert_with(|| {
                let mut version_vector = DotSet::new();
                version_vector.set_counter(final_dot.pid, final_dot.counter);
                version_vector
            });

        if !self.final_dots.contains(&final_dot) {
            self.final_dots.push(final_dot);
        }
    }

    /// Cleans up any "final dots" and returns a Vec of Pid's that can be GC'd
    pub fn garbage_collect(&mut self, version_vector: &DotSet) -> Option<Vec<Pid>> {
        if self.final_dots.is_empty() {
            return None;
        }

        let filtered_matrix = self.filtered_matrix();
        let filtered_stable = filtered_matrix.get_stable();
        let dots_to_remove = self
            .final_dots
            .iter()
            .copied()
            .filter(|final_dot| {
                version_vector.contains(final_dot) && filtered_stable.contains(final_dot)
            })
            .collect::<Vec<_>>();

        if dots_to_remove.is_empty() {
            return None;
        }

        self.final_dots.retain(|dot| !dots_to_remove.contains(dot));

        for dot in &dots_to_remove {
            self.matrix.remove(&dot.pid);
            for version_vector in self.matrix.values_mut() {
                version_vector.set.remove(&dot.pid);
            }
        }
        Some(dots_to_remove.iter().map(|dot| dot.pid).collect())
    }

    fn filtered_matrix(&self) -> VersionMatrix {
        let final_pids = self.final_dots.iter().map(|dot| dot.pid).collect::<Vec<_>>();
        let matrix = self
            .matrix
            .iter()
            .filter(|(pid, _)| !final_pids.contains(pid))
            .map(|(pid, version_vector)| (*pid, version_vector.clone()))
            .collect();

        VersionMatrix {
            matrix,
            final_dots: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DotMap<T: Clone + PartialEq> {
    map: HashMap<Pid, Vec<(Dot, T)>>,
}

impl<T: Clone + PartialEq> Default for DotMap<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Clone + PartialEq> DotMap<T> {
    pub fn new() -> Self {
        DotMap {
            map: HashMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.map.values().map(Vec::len).sum()
    }

    /// The Dots must be contiguous, with no gaps in sequence numbers per Pid...
    pub fn insert(&mut self, dot: Dot, value: T) {
        let vec = self.map.entry(dot.pid).or_default();
        // Ensure the DotMap is sorted
        if let Some((last_dot, _)) = vec.last() {
            assert!(
                last_dot.counter + 1 == dot.counter,
                "Missing delta! Last Dot: {}, new Dot: {}",
                last_dot.counter,
                dot.counter
            );
        } else {
            assert!(
                dot.counter == 0,
                "Missing initial delta from Pid {}",
                dot.pid
            )
        }
        vec.push((dot, value))
    }

    fn get_greater_iter(&self, dot: &Dot) -> std::slice::Iter<'_, (Dot, T)> {
        if let Some(all) = self.map.get(&dot.pid) {
            let idx = match all.binary_search_by(|(d, _)| d.counter.cmp(&dot.counter)) {
                Ok(pos) => pos + 1, // skip the exact match
                Err(pos) => pos,    // first element > dot.counter
            };
            all[idx..].iter()
        } else {
            [].iter()
        }
    }

    /// Returns all items with dots not subsumed by dot_set
    pub fn get_all_greater_iter<'a>(
        &'a self,
        dot_set: &'a DotSet,
    ) -> impl Iterator<Item = &'a (Dot, T)> + 'a {
        // First, handle keys that exist in both dot_set and self.map
        let existing_keys_iter = dot_set.set.iter().flat_map(move |elem| {
            // bind the converted Dot to a local variable so we don't take
            // a reference to a temporary
            let dot: Dot = elem.into();
            self.get_greater_iter(&dot)
        });

        // Then, handle keys that exist in self.map but not in dot_set
        // For these keys, return all dots as they're all greater than what's in dot_set (which is nothing)
        let missing_keys_iter = self
            .map
            .iter()
            .filter_map(move |(pid, vec)| {
                if dot_set.set.contains_key(pid) {
                    None // Already handled in existing_keys_iter
                } else {
                    Some(vec.iter()) // Return all dots for this pid
                }
            })
            .flatten();

        // Combine both iterators
        existing_keys_iter.chain(missing_keys_iter)
    }

    pub fn retain<F>(&mut self, mut predicate: F)
    where
        F: FnMut(&Dot, &T) -> bool,
    {
        self.map.retain(|_, values| {
            values.retain(|(dot, value)| predicate(dot, value));
            !values.is_empty()
        });
    }
}
