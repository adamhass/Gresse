use crate::crdt::STABLE_REPLICA_PID;
use crate::prelude::Pid;
use serde::{Deserialize, Serialize};
use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
};

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
            Ordering::Less
        } else {
            Ordering::Greater
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

    fn pointwise_min(&mut self, other: &DotSet) {
        let pids = self.pids().chain(other.pids()).collect::<HashSet<_>>();
        for pid in pids {
            self.set_counter(pid, *self.get(&pid).min(other.get(&pid)));
        }
    }

    pub fn set_counter(&mut self, pid: Pid, counter: Counter) {
        self.set.insert(pid, counter);
    }

    pub fn remove_pid(&mut self, pid: Pid) {
        self.set.remove(&pid);
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
            _ = self.set.insert(dot.pid, dot.counter);
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
        self.set.get(pid).unwrap_or(&-1)
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

    pub fn gc_counter(&self) -> &Counter {
        self.get(&STABLE_REPLICA_PID)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionMatrix {
    own_pid: Pid,
    matrix: HashMap<Pid, DotSet>,
    final_dots: HashMap<Pid, Counter>,
}

impl VersionMatrix {
    pub fn new(own_pid: Pid) -> Self {
        let mut matrix = HashMap::new();
        let mut initial_vector = DotSet::new();
        initial_vector.set_counter(own_pid, -1);
        initial_vector.set_counter(STABLE_REPLICA_PID, -1);
        matrix.insert(own_pid, initial_vector);
        Self {
            own_pid,
            matrix,
            final_dots: HashMap::new(),
        }
    }

    pub fn update(&mut self, pid: Pid, version_vector: DotSet) {
        if self.matrix.get(&self.own_pid).unwrap().gc_counter() <= version_vector.gc_counter() {
            self.matrix.insert(pid, version_vector);
        }
    }

    pub fn insert_pid(&mut self, pid: Pid, gc_counter: Counter) {
        if pid == STABLE_REPLICA_PID {
            return;
        }

        self.matrix.entry(pid).or_insert_with(|| {
            let mut version_vector = DotSet::new();
            version_vector.set_counter(pid, -1);
            version_vector.set_counter(STABLE_REPLICA_PID, gc_counter);
            version_vector
        });
    }

    pub(crate) fn own_pid(&self) -> Pid {
        self.own_pid
    }

    pub(crate) fn rebind_own_pid(&mut self, pid: Pid, version_vector: DotSet) {
        self.own_pid = pid;
        self.matrix.insert(pid, version_vector);
    }

    pub fn own_gc_counter(&self) -> &Counter {
        self.matrix
            .get(&self.own_pid)
            .unwrap()
            .get(&STABLE_REPLICA_PID)
    }

    pub fn remove_pids(&mut self, pids: &Vec<Pid>) {
        if let Some(stable_vv) = self.matrix.get(&STABLE_REPLICA_PID).cloned() {
            for pid in pids {
                if let Some(counter) = self.final_dots.get(pid) {
                    let final_dot = Dot {
                        pid: *pid,
                        counter: *counter,
                    };
                    if !stable_vv.contains(&final_dot) {
                        continue;
                    }
                }
                self.matrix.remove(pid);
                self.final_dots.remove(pid);
                for version_vector in self.matrix.values_mut() {
                    version_vector.set.remove(pid);
                }
            }
        }
    }

    pub(crate) fn apply_gc_marker(
        &mut self,
        stable: &DotSet,
        departed_pids: &Option<Vec<Pid>>,
        gc_counter: Counter,
    ) {
        if let Some(departed_pids) = departed_pids {
            self.remove_pids(departed_pids);
        }

        let mut stable_vv = stable.clone();
        stable_vv.set_counter(STABLE_REPLICA_PID, gc_counter);
        self.matrix.insert(STABLE_REPLICA_PID, stable_vv);
        self.matrix
            .get_mut(&self.own_pid)
            .expect("version matrix must contain its own row")
            .set_counter(STABLE_REPLICA_PID, gc_counter);
    }

    /// Returns a DotSet and a list of departed_pids that can now be GC'd
    pub fn get_stable(&self) -> (Option<DotSet>, Option<Vec<Pid>>) {
        // 1. Determine which rows still participate in the stability computation.
        // A shutdown replica i is excluded once *we* have observed its final dot.
        let observer_rows = self.get_observer_rows();
        if observer_rows.is_empty() {
            return (None, None);
        }

        // 2. Compute the pointwise minimum across all remaining observer rows.
        //
        // IMPORTANT: missing entries must be interpreted as counter -1 because
        // the first dot generated by a replica has counter 0.
        //
        // Start with one row, then lower every column according to every other row.
        let mut stable = observer_rows[0].1.clone();

        for (_, vv) in observer_rows.iter().skip(1) {
            stable.pointwise_min(vv);
        }

        // A row can exist before its replica has generated a dot. Preserve
        // those replica columns at -1, matching the old matrix semantics.
        for (pid, _) in &observer_rows {
            if stable.counter(pid).is_none() {
                stable.set_counter(*pid, -1);
            }
        }

        // 3. Determine which departed replicas can now be removed completely.
        //
        // The newly calculated frontier is not sufficient evidence: it must
        // first complete a GC round and become the stable replica's version
        // vector. This makes departure removal a two-phase operation.
        let mut departed_pids = self
            .matrix
            .get(&STABLE_REPLICA_PID)
            .map(|stable_vv| {
                self.final_dots
                    .iter()
                    .filter_map(|(pid, final_counter)| {
                        stable_vv
                            .contains(&Dot {
                                pid: *pid,
                                counter: *final_counter,
                            })
                            .then_some(*pid)
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        departed_pids.sort_unstable();

        // 4. Their columns are no longer relevant and should not be returned
        // as part of the stable frontier.
        for pid in &departed_pids {
            stable.remove_pid(*pid);
        }

        (
            Some(stable),
            (!departed_pids.is_empty()).then_some(departed_pids),
        )
    }

    // Ignore pids that we have seen all mutations from.
    fn get_observer_rows(&self) -> Vec<(Pid, &DotSet)> {
        let own_vv = self.matrix.get(&self.own_pid).unwrap();
        self.matrix
            .iter()
            .filter_map(|(pid, vv)| {
                if *pid == STABLE_REPLICA_PID {
                    return None;
                }
                let exclude = self.final_dots.get(pid).is_some_and(|final_counter| {
                    own_vv.contains(&Dot {
                        pid: *pid,
                        counter: *final_counter,
                    })
                });
                if exclude {
                    None
                } else {
                    Some((*pid, vv))
                }
            })
            .collect()
    }

    /// We must wait for final count for this Pid to stabilize before we can remove it completely
    pub fn insert_final_dot(&mut self, dot: Dot) {
        let _ = self.final_dots.insert(dot.pid, dot.counter);
    }

    /// Returns whether this exact departed-replica marker has already been
    /// observed.  Callers use this to avoid reprocessing the immutable
    /// shutdown payload on every membership-directory poll.
    pub fn contains_final_dot(&self, dot: Dot) -> bool {
        self.final_dots.get(&dot.pid) == Some(&dot.counter)
    }
}

#[cfg(test)]
mod tests {
    use super::{Counter, Dot, DotSet, Pid, VersionMatrix, STABLE_REPLICA_PID};

    fn dot_set(entries: &[(Pid, Counter)]) -> DotSet {
        let mut dots = DotSet::new();
        for (pid, counter) in entries {
            dots.set_counter(*pid, *counter);
        }
        dots
    }

    fn apply_gc(
        matrix: &mut VersionMatrix,
        stable: DotSet,
        departed_pids: Option<Vec<Pid>>,
        gc_counter: Counter,
    ) {
        matrix.apply_gc_marker(&stable, &departed_pids, gc_counter);
    }

    #[test]
    fn stable_is_the_pointwise_minimum_and_missing_means_unseen() {
        let mut matrix = VersionMatrix::new(1);
        matrix.update(1, dot_set(&[(1, 4), (2, 2)]));
        matrix.update(2, dot_set(&[(1, 3)]));

        let (stable, departed) = matrix.get_stable();
        let stable = stable.expect("matrix has observer rows");

        assert_eq!(stable.counter(&1), Some(3));
        assert_eq!(stable.counter(&2), Some(-1));
        assert_eq!(departed, None);
    }

    #[test]
    fn discovered_replica_starts_with_a_conservative_row() {
        let mut matrix = VersionMatrix::new(1);
        matrix.insert_pid(2, 0);

        let discovered = matrix.matrix.get(&2).expect("discovered replica row");
        assert_eq!(discovered.counter(&2), Some(-1));
        assert_eq!(discovered.counter(&STABLE_REPLICA_PID), Some(0));

        let observed = dot_set(&[(STABLE_REPLICA_PID, 0), (1, 3), (2, 2)]);
        matrix.update(2, observed.clone());
        matrix.insert_pid(2, 0);
        assert_eq!(matrix.matrix.get(&2), Some(&observed));
    }

    #[test]
    fn applying_gc_marker_advances_own_epoch_and_rejects_stale_rows() {
        let mut matrix = VersionMatrix::new(1);
        matrix.update(1, dot_set(&[(STABLE_REPLICA_PID, 0), (1, 2)]));
        matrix.update(2, dot_set(&[(STABLE_REPLICA_PID, 0), (1, 1), (2, 1)]));
        let stable = matrix.get_stable().0.expect("stable frontier");

        matrix.apply_gc_marker(&stable, &None, 1);
        assert_eq!(
            matrix
                .matrix
                .get(&1)
                .expect("own row")
                .counter(&STABLE_REPLICA_PID),
            Some(1)
        );

        matrix.update(2, dot_set(&[(STABLE_REPLICA_PID, 0), (1, 2), (2, 2)]));
        assert_eq!(
            matrix.matrix.get(&2).expect("peer row").counter(&2),
            Some(1)
        );
        matrix.update(2, dot_set(&[(STABLE_REPLICA_PID, 1), (1, 2), (2, 2)]));
        assert_eq!(
            matrix.matrix.get(&2).expect("peer row").counter(&2),
            Some(2)
        );
    }

    #[test]
    fn departed_row_stops_blocking_other_columns_after_local_final_dot() {
        let mut matrix = VersionMatrix::new(1);
        matrix.update(1, dot_set(&[(1, 4), (2, 5), (3, 2)]));
        matrix.update(2, dot_set(&[(1, 1), (2, 5)]));
        matrix.update(3, dot_set(&[(1, 3), (2, 4), (3, 2)]));
        matrix.insert_final_dot(Dot { pid: 2, counter: 5 });

        let (stable, departed) = matrix.get_stable();
        let stable = stable.expect("matrix has observer rows");
        assert_eq!(stable.counter(&1), Some(3));
        assert_eq!(stable.counter(&2), Some(4));
        assert_eq!(departed, None);

        matrix.update(3, dot_set(&[(1, 3), (2, 5), (3, 2)]));
        let (stable, departed) = matrix.get_stable();
        let stable = stable.expect("matrix has observer rows");
        assert_eq!(stable.counter(&1), Some(3));
        assert_eq!(stable.counter(&2), Some(5));
        assert_eq!(departed, None);
    }

    #[test]
    fn removed_departure_is_not_reported_again() {
        let mut matrix = VersionMatrix::new(1);
        matrix.update(1, dot_set(&[(STABLE_REPLICA_PID, 0), (1, 0), (2, -1)]));
        matrix.insert_final_dot(Dot {
            pid: 2,
            counter: -1,
        });

        // A missing entry defaults to -1 for frontier calculations, but that
        // does not mean the stable replica has explicitly observed this dot.
        assert_eq!(matrix.get_stable().1, None);
        matrix.update(
            STABLE_REPLICA_PID,
            dot_set(&[(STABLE_REPLICA_PID, 0), (1, 0)]),
        );
        assert_eq!(matrix.get_stable().1, None);

        matrix.update(
            STABLE_REPLICA_PID,
            dot_set(&[(STABLE_REPLICA_PID, 0), (1, 0), (2, -1)]),
        );
        assert_eq!(matrix.get_stable().1, Some(vec![2]));
        matrix.remove_pids(&vec![2]);
        assert_eq!(matrix.get_stable().1, None);
    }

    #[test]
    fn stable_frontier_progresses_across_two_gc_rounds_before_removing_departure() {
        let mut matrix = VersionMatrix::new(1);
        matrix.update(
            1,
            dot_set(&[(STABLE_REPLICA_PID, 0), (1, 4), (2, 5), (3, 2)]),
        );
        matrix.update(
            2,
            dot_set(&[(STABLE_REPLICA_PID, 0), (1, 1), (2, 5), (3, 0)]),
        );
        matrix.update(
            3,
            dot_set(&[(STABLE_REPLICA_PID, 0), (1, 3), (2, 5), (3, 2)]),
        );
        matrix.update(
            STABLE_REPLICA_PID,
            dot_set(&[(STABLE_REPLICA_PID, 0), (1, 1), (2, 4), (3, 0)]),
        );
        matrix.insert_final_dot(Dot { pid: 2, counter: 5 });

        // The current frontier covers replica 2's final dot and advances past
        // the older stable frontier, but removal must wait for one GC round.
        let (first_stable, first_departed) = matrix.get_stable();
        let first_stable = first_stable.expect("matrix has observer rows");
        assert_eq!(first_stable.counter(&1), Some(3));
        assert_eq!(first_stable.counter(&2), Some(5));
        assert_eq!(first_stable.counter(&3), Some(2));
        assert_eq!(first_departed, None);
        apply_gc(&mut matrix, first_stable, first_departed, 1);

        // Once the first frontier is installed as the stable replica's row,
        // the next round can announce and remove the departed replica.
        let (second_stable, second_departed) = matrix.get_stable();
        let second_stable = second_stable.expect("matrix has observer rows");
        assert_eq!(second_stable.counter(&1), Some(3));
        assert_eq!(second_stable.counter(&2), None);
        assert_eq!(second_stable.counter(&3), Some(2));
        assert_eq!(second_departed, Some(vec![2]));
        apply_gc(&mut matrix, second_stable.clone(), second_departed, 2);

        let (settled_stable, settled_departed) = matrix.get_stable();
        assert_eq!(settled_stable, Some(second_stable));
        assert_eq!(settled_departed, None);
    }

    #[test]
    fn tracks_final_dots_for_idempotent_shutdown_processing() {
        let final_dot = Dot {
            pid: 42,
            counter: 17,
        };
        let mut matrix = VersionMatrix::new(0);

        assert!(!matrix.contains_final_dot(final_dot));
        matrix.insert_final_dot(final_dot);
        assert!(matrix.contains_final_dot(final_dot));
        assert!(!matrix.contains_final_dot(Dot {
            pid: 42,
            counter: 18,
        }));
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

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn len(&self) -> usize {
        self.map.values().map(Vec::len).sum()
    }

    /// This simply inserts the dots, we trust the replication protocol to deliver contiguous groups
    pub fn insert(&mut self, dot: Dot, value: T) {
        let vec = self.map.entry(dot.pid).or_default();
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
