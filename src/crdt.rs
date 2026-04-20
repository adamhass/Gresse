use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{error::Error, fmt::Debug};

use crate::{dots::DotSet, prelude::Pid};

pub trait CRDTData: Serialize + DeserializeOwned + Send + Sync + Clone + Debug {}

impl<T> CRDTData for T where T: Serialize + DeserializeOwned + Sync + Send + Clone + Debug {}

pub trait CRDT: Sized + Serialize + DeserializeOwned {
    type Delta: CRDTData;
    type Query: CRDTData;
    type Mutation: CRDTData;
    type ClientResponse: CRDTData;
    type Error: CRDTData + Error;

    /// Queries the state
    fn query(&self, query: Self::Query) -> Self::ClientResponse;

    /// Mutates the state, records the delta and returns the client response
    fn mutate(&mut self, mutation: Self::Mutation) -> Self::ClientResponse;

    /// Returns the greatest observed Sequence number for each replica ID
    fn get_version_vector(&self) -> &DotSet;

    /// Returns the deltas between the current state and the state represented by the given version vector
    /// together with the number of insertions and removals in this delta
    /// Only needs to be implemented for pull_based_delta_mutation
    fn get_delta(&self, version_vector: &DotSet) -> (DeltaGroup<Self::Delta>, u16, u16);

    /// Applies the remote delta to the local state
    /// Returns number of insertions and number of removals
    fn merge_delta_group(&mut self, delta: DeltaGroup<Self::Delta>) -> (u16, u16);
}

/// Delta group is a set of Deltas that are causally ordered
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaGroup<D> {
    pub list: Vec<D>,
    pub version_vector: DotSet,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReplicaMessage<T: CRDT + Debug + Clone> {
    DeltaGroup(DeltaGroup<T::Delta>, u128),
    VersionVector(Pid, DotSet, u128),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "params")]
pub enum CRDTClientRequest<T: CRDT + Debug + Clone> {
    Mutation(T::Mutation),
    Query(T::Query),
}
