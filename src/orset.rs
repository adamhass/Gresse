use crate::crdt::{DeltaGroup, CRDT};
use crate::dots::{Dot, DotMap, DotSet};
use crate::prelude::Pid;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use thiserror::Error;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(bound(
    serialize = "T: Clone + Eq + Hash + Ord + Serialize",
    deserialize = "T: Clone + Eq + Hash + Ord + Deserialize<'de>"
))]
pub enum OrSetMutation<T: Clone + Eq + Hash + Ord> {
    Insert(T),
    Remove(T),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(bound(
    serialize = "T: Clone + Eq + Hash + Ord + Serialize",
    deserialize = "T: Clone + Eq + Hash + Ord + Deserialize<'de>"
))]
pub enum OrSetQuery<T: Clone + Eq + Hash + Ord> {
    Contains(T),
    Elements,
    Meta,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OrSetMeta {
    pub entry_count: usize,
    pub delta_log_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(bound(
    serialize = "T: Clone + Eq + Hash + Ord + Serialize",
    deserialize = "T: Clone + Eq + Hash + Ord + Deserialize<'de>"
))]
pub enum OrSetResponse<T: Clone + Eq + Hash + Ord> {
    Acknowledged,
    Contains(bool),
    Elements(Vec<T>),
    Meta(OrSetMeta),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(bound(
    serialize = "T: Clone + Eq + Hash + Ord + Serialize",
    deserialize = "T: Clone + Eq + Hash + Ord + Deserialize<'de>"
))]
enum OrSetChange<T: Clone + Eq + Hash + Ord> {
    Insert { element: T },
    Remove { element: T, removed_dots: Vec<Dot> },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(bound(
    serialize = "T: Clone + Eq + Hash + Ord + Serialize",
    deserialize = "T: Clone + Eq + Hash + Ord + Deserialize<'de>"
))]
pub struct OrSetDelta<T: Clone + Eq + Hash + Ord> {
    dot: Dot,
    change: OrSetChange<T>,
}

#[derive(Debug, Error, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum OrSetError {
    #[error("replica pid has not been initialized")]
    MissingPid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound(
    serialize = "T: Clone + Eq + Hash + Ord + Serialize",
    deserialize = "T: Clone + Eq + Hash + Ord + Deserialize<'de>"
))]
pub struct ORSet<T: Clone + Eq + Hash + Ord> {
    pid: Option<Pid>,
    entries: HashMap<T, Vec<Dot>>,
    delta_log: DotMap<OrSetDelta<T>>,
    version_vector: DotSet,
}

impl<T> ORSet<T>
where
    T: Clone + Eq + Hash + Ord,
{
    pub fn new() -> Self {
        Self {
            pid: None,
            entries: HashMap::new(),
            delta_log: DotMap::new(),
            version_vector: DotSet::new(),
        }
    }

    fn insert_dot(&mut self, element: T, dot: Dot) {
        let dots = self.entries.entry(element).or_default();
        if !dots.contains(&dot) {
            dots.push(dot);
        }
    }

    fn remove_dots(&mut self, element: &T, removed_dots: &[Dot]) {
        if let Some(dots) = self.entries.get_mut(element) {
            dots.retain(|dot| !removed_dots.contains(dot));
            if dots.is_empty() {
                self.entries.remove(element);
            }
        }
    }

    fn next_dot(&mut self) -> Result<Dot, OrSetError> {
        let pid = self.pid.ok_or(OrSetError::MissingPid)?;
        Ok(self.version_vector.increment_and_get(pid))
    }

    fn meta(&self) -> OrSetMeta {
        OrSetMeta {
            entry_count: self.entries.len(),
            delta_log_count: self.delta_log.len(),
        }
    }
}

impl<T> Default for ORSet<T>
where
    T: Clone + Eq + Hash + Ord,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<T> CRDT for ORSet<T>
where
    T: Clone
        + Eq
        + Hash
        + Ord
        + Serialize
        + for<'de> Deserialize<'de>
        + std::fmt::Debug
        + Send
        + Sync,
{
    type Delta = OrSetDelta<T>;
    type Query = OrSetQuery<T>;
    type Mutation = OrSetMutation<T>;
    type ClientResponse = Result<OrSetResponse<T>, OrSetError>;
    type SideEffects = ();
    type Error = OrSetError;

    fn query(&self, query: Self::Query) -> Self::ClientResponse {
        match query {
            OrSetQuery::Contains(element) => Ok(OrSetResponse::Contains(
                self.entries
                    .get(&element)
                    .is_some_and(|dots| !dots.is_empty()),
            )),
            OrSetQuery::Elements => {
                let mut elements = self.entries.keys().cloned().collect::<Vec<_>>();
                elements.sort();
                Ok(OrSetResponse::Elements(elements))
            }
            OrSetQuery::Meta => Ok(OrSetResponse::Meta(self.meta())),
        }
    }

    fn set_pid(&mut self, pid: Pid) {
        self.pid = Some(pid);
    }

    fn mutate(&mut self, mutation: Self::Mutation) -> Self::ClientResponse {
        let dot = self.next_dot()?;
        let delta = match mutation {
            OrSetMutation::Insert(element) => {
                self.insert_dot(element.clone(), dot);
                OrSetDelta {
                    dot,
                    change: OrSetChange::Insert { element },
                }
            }
            OrSetMutation::Remove(element) => {
                let removed_dots = self.entries.get(&element).cloned().unwrap_or_default();
                self.remove_dots(&element, &removed_dots);
                OrSetDelta {
                    dot,
                    change: OrSetChange::Remove {
                        element,
                        removed_dots,
                    },
                }
            }
        };
        self.delta_log.insert(dot, delta);
        Ok(OrSetResponse::Acknowledged)
    }

    fn get_version_vector(&self) -> &DotSet {
        &self.version_vector
    }

    fn get_delta(&self, version_vector: &DotSet) -> DeltaGroup<Self::Delta, ()> {
        let list = self
            .delta_log
            .get_all_greater_iter(version_vector)
            .map(|(_, delta)| delta.clone())
            .collect::<Vec<_>>();
        DeltaGroup {
            list,
            version_vector: self.version_vector.clone(),
            side_effects: None,
        }
    }

    fn merge_delta_group(&mut self, delta: DeltaGroup<Self::Delta, ()>) {
        for delta in delta.list {
            if self.version_vector.contains(&delta.dot) {
                continue;
            }

            self.version_vector.insert(&delta.dot);
            match &delta.change {
                OrSetChange::Insert { element } => {
                    self.insert_dot(element.clone(), delta.dot);
                }
                OrSetChange::Remove {
                    element,
                    removed_dots,
                } => {
                    self.remove_dots(element, removed_dots);
                }
            }
            self.delta_log.insert(delta.dot, delta);
        }
    }

    fn gc(&mut self, version_vector: &DotSet, departed_pids: &Option<Vec<Pid>>) {
        let departed_pids = departed_pids
            .as_deref()
            .unwrap_or_default()
            .iter()
            .copied()
            .collect::<HashSet<_>>();

        // A delta is needed only when it is neither covered by the stable
        // frontier nor owned by a replica whose final dot has been globally
        // acknowledged.  The conjunction is intentional: either condition
        // is sufficient to discard the delta.
        self.delta_log
            .retain(|dot, _| !version_vector.contains(dot) && !departed_pids.contains(&dot.pid));

        if departed_pids.is_empty() {
            return;
        }

        for pid in &departed_pids {
            self.version_vector.remove_pid(*pid);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_remove_element() {
        let mut set = ORSet::<String>::new();
        set.set_pid(1);

        set.mutate(OrSetMutation::Insert("apple".into())).unwrap();
        assert_eq!(
            set.query(OrSetQuery::Contains("apple".into())).unwrap(),
            OrSetResponse::Contains(true)
        );

        set.mutate(OrSetMutation::Remove("apple".into())).unwrap();
        assert_eq!(
            set.query(OrSetQuery::Contains("apple".into())).unwrap(),
            OrSetResponse::Contains(false)
        );
    }

    #[test]
    fn meta_query_reports_entry_and_delta_log_sizes() {
        let mut set = ORSet::<String>::new();
        set.set_pid(1);

        assert_eq!(
            set.query(OrSetQuery::Meta).unwrap(),
            OrSetResponse::Meta(OrSetMeta {
                entry_count: 0,
                delta_log_count: 0,
            })
        );

        set.mutate(OrSetMutation::Insert("apple".into())).unwrap();
        set.mutate(OrSetMutation::Insert("banana".into())).unwrap();
        set.mutate(OrSetMutation::Remove("apple".into())).unwrap();

        assert_eq!(
            set.query(OrSetQuery::Meta).unwrap(),
            OrSetResponse::Meta(OrSetMeta {
                entry_count: 1,
                delta_log_count: 3,
            })
        );
    }

    #[test]
    fn gc_discards_departed_pid_history_not_covered_by_stable_frontier() {
        let mut set = ORSet::<String>::new();
        set.set_pid(1);

        set.mutate(OrSetMutation::Insert("apple".into())).unwrap();
        let replica_two_delta = OrSetDelta {
            dot: Dot { pid: 2, counter: 0 },
            change: OrSetChange::Insert {
                element: "banana".into(),
            },
        };
        set.merge_delta_group(DeltaGroup {
            list: vec![replica_two_delta],
            version_vector: DotSet::new(),
            side_effects: None,
        });

        let mut stable = DotSet::new();
        stable.set_counter(1, 0);
        // Replica 2's dot is deliberately absent: departure alone must
        // reclaim its history after its final dot was globally acknowledged.
        set.gc(&stable, &Some(vec![2]));

        assert_eq!(
            set.query(OrSetQuery::Elements).unwrap(),
            OrSetResponse::Elements(vec!["apple".into(), "banana".into()])
        );
        assert_eq!(
            set.query(OrSetQuery::Meta).unwrap(),
            OrSetResponse::Meta(OrSetMeta {
                entry_count: 2,
                delta_log_count: 0,
            })
        );
        assert_eq!(set.get_version_vector().counter(&2), None);
    }
}
