use crate::helpers::*;
use crate::vector_db::VectorDb;
use gresse::dots::DotSet;
use gresse::{crdt::*, dots::Dot, dots::DotMap, prelude::*};
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbDelta {
    dot: Dot,
    mutation: DbMutation,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct VectorDBW {
    pub pid: Pid,
    pub db: VectorDb,
    pub dot_map: DotMap<DbMutation>,
    pub version_vector: DotSet,
}

impl CRDT for VectorDBW {
    type Delta = DbDelta;
    type Query = (SKey, usize);
    type Mutation = DbMutation;
    type ClientResponse = DbResponse;
    type Error = DbError;

    fn get_version_vector(&self) -> &DotSet {
        &self.version_vector
    }

    fn query(&self, query: Self::Query) -> Self::ClientResponse {
        let (s_key, k) = query;
        let res = self.db.v.min_k(&s_key, k).ok_or(DbError::KeyNotFound)?;
        Ok(Some(res))
    }

    fn get_delta(&self, dots: &DotSet) -> (DeltaGroup<DbDelta>, u16, u16) {
        let (list, insert_count, remove_count) = self.dot_map.get_all_greater_iter(dots).fold(
            (Vec::new(), 0, 0),
            |(mut acc, ins, rem), (dot, mutation)| {
                let count = match mutation {
                    DbMutation::Insert((_, _)) => (ins + 1, rem),
                    DbMutation::Removal(_) => (ins, rem + 1),
                };
                acc.push(DbDelta {
                    dot: *dot,
                    mutation: mutation.clone(),
                });
                (acc, count.0, count.1)
            },
        );
        (
            DeltaGroup {
                list,
                version_vector: self.version_vector.clone(),
            },
            insert_count,
            remove_count,
        )
    }

    fn merge_delta_group(&mut self, deltas: DeltaGroup<DbDelta>) -> (u16, u16) {
        let mut counts: (u16, u16) = (0, 0);
        for delta in deltas.list {
            let new_counts = self.apply_delta(delta);
            counts.0 += new_counts.0; // insertion count
            counts.1 += new_counts.1; // removal count
        }
        counts
    }

    fn mutate(&mut self, mutation: Self::Mutation) -> Self::ClientResponse {
        let dot = self.version_vector.increment_and_get(self.pid);
        match &mutation {
            DbMutation::Removal(key) => {
                self.db.remove(key);
            }
            DbMutation::Insert((key, vector)) => {
                match key {
                    EitherKey::S(skey) => self.db.insert_into_s(*skey, vector.clone()),
                    EitherKey::U(ukey) => self.db.insert_into_u(*ukey, vector.clone()),
                };
            }
        }
        self.dot_map.insert(dot, mutation);
        Ok(None)
    }
}

impl VectorDBW {
    pub fn new(pid: Pid, db: VectorDb) -> Self {
        VectorDBW {
            pid,
            db,
            dot_map: DotMap::new(),
            version_vector: DotSet::new(),
        }
    }

    fn apply_delta(&mut self, delta: DbDelta) -> (u16, u16) {
        // Early return
        if self.version_vector.contains(&delta.dot) {
            return (0, 0);
        }
        self.version_vector.insert(&delta.dot);
        let counts: (u16, u16) = match &delta.mutation {
            DbMutation::Removal(key) => {
                self.db.remove(key);
                (0, 1)
            }
            DbMutation::Insert((EitherKey::S(skey), vector)) => {
                self.db.insert_into_s(*skey, vector.clone());
                (1, 0)
            }
            DbMutation::Insert((EitherKey::U(ukey), vector)) => {
                self.db.insert_into_u(*ukey, vector.clone());
                (1, 0)
            }
        };
        self.dot_map.insert(delta.dot, delta.mutation);
        counts
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merge_delta() {
        let mut dbw = VectorDBW::new(1, VectorDb::new(Float(10.0)));
        // Insert same value twice
        dbw.mutate(DbMutation::Insert((
            EitherKey::S(1),
            vec![Float(1.0), Float(2.0), Float(5.0)],
        )))
        .expect("Mutation of database failed");
        dbw.mutate(DbMutation::Insert((
            EitherKey::S(1),
            vec![Float(1.0), Float(2.0), Float(5.0)],
        )))
        .expect("Mutation of database failed");

        let deltas = vec![
            // Remove instance with two different pids
            DbDelta {
                dot: Dot { pid: 2, counter: 0 },
                mutation: DbMutation::Removal(EitherKey::S(1)),
            },
            DbDelta {
                dot: Dot { pid: 3, counter: 0 },
                mutation: DbMutation::Removal(EitherKey::S(1)),
            },
            // Add one S vector and one U vector twice
            DbDelta {
                dot: Dot { pid: 3, counter: 1 },
                mutation: DbMutation::Insert((
                    EitherKey::U(1),
                    vec![Float(0.0), Float(1.0), Float(2.0)],
                )),
            },
            DbDelta {
                dot: Dot { pid: 2, counter: 1 },
                mutation: DbMutation::Insert((
                    EitherKey::S(2),
                    vec![Float(4.0), Float(1.0), Float(5.0)],
                )),
            },
            DbDelta {
                dot: Dot { pid: 2, counter: 2 },
                mutation: DbMutation::Insert((
                    EitherKey::U(1),
                    vec![Float(0.0), Float(1.0), Float(2.0)],
                )),
            },
        ];

        let mut dotset = DotSet::new();
        for delta in deltas.iter() {
            dotset.insert(&delta.dot);
        }
        let delta_group = DeltaGroup {
            list: deltas,
            version_vector: dotset,
        };

        dbw.merge_delta_group(delta_group);
        // Check S table
        assert_eq!(dbw.db.s.len(), 1);
        assert_eq!(
            dbw.db.s.get(&2),
            Some(&vec![Float(4.0), Float(1.0), Float(5.0)])
        );
        // Check U table
        assert_eq!(dbw.db.u.len(), 1);
        assert_eq!(dbw.db.u.get_weight(&1), 2);
        assert_eq!(
            dbw.db.u.get(&1),
            Some(&vec![Float(0.0), Float(1.0), Float(2.0)])
        );
        // Check view
        assert_eq!(dbw.db.v.len(), 1);
        assert_eq!(dbw.db.v.get_value(&2, &1), Some(&Float(5.0)));
    }
}
