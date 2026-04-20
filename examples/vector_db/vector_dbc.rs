use crate::helpers::*;
use crate::vector_db::VectorDb;
use gresse::dots::Dot;
use gresse::dots::DotSet;
use gresse::prelude::Pid;
use gresse::{crdt::*, dots::DotMap};
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbDelta {
    dot: Dot,
    mutation: DbMutation,
    view: Option<Vec<((SKey, UKey), Float)>>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct VectorDBC {
    pub pid: Pid,
    pub db: VectorDb,
    pub dot_map: DotMap<DbMutation>,
    pub version_vector: DotSet,
}

impl CRDT for VectorDBC {
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
                    view: match mutation {
                        DbMutation::Insert((k, _)) => Some(self.db.get_view(k)),
                        DbMutation::Removal(_) => None,
                    },
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
        let concurrent_deltas: Vec<DbMutation> = {
            self.get_all_concurrent_deltas(&deltas.version_vector)
                .cloned() // TODO: maybe a rust ninja can avoid this
                .collect()
        };
        let mut counts: (u16, u16) = (0, 0);
        for delta in deltas.list {
            let new_counts = self.apply_delta(delta, &concurrent_deltas);
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

impl VectorDBC {
    pub fn new(pid: Pid, db: VectorDb) -> Self {
        VectorDBC {
            pid,
            db,
            dot_map: DotMap::new(),
            version_vector: DotSet::new(),
        }
    }

    fn apply_delta(&mut self, delta: DbDelta, concurrent_deltas: &[DbMutation]) -> (u16, u16) {
        // Early return
        if self.version_vector.contains(&delta.dot) {
            return (0, 0);
        }
        let counts: (u16, u16) = match &delta.mutation {
            DbMutation::Removal(key) => {
                self.db.remove(key);
                (0, 1)
            }
            DbMutation::Insert((EitherKey::S(skey), svec)) => {
                if self.db.s.insert(*skey, svec.clone()) == 1 {
                    self.db.v.insert_values(delta.view.expect("view expected"));
                    // Handle concurrent u table updates
                    for concurrent in concurrent_deltas.iter() {
                        match concurrent {
                            DbMutation::Removal(EitherKey::U(ukey)) => {
                                if self.db.u.get_weight(ukey) <= 0 {
                                    self.db.v.remove(skey, ukey);
                                }
                            }
                            DbMutation::Insert((EitherKey::U(ukey), uvec)) => {
                                if self.db.u.get_weight(ukey) >= 1 {
                                    self.db.insert_into_view(skey, ukey, svec, uvec);
                                }
                            }
                            _ => {}
                        };
                    }
                }
                (1, 0)
            }
            DbMutation::Insert((EitherKey::U(ukey), uvec)) => {
                if self.db.u.insert(*ukey, uvec.clone()) == 1 {
                    self.db.v.insert_values(delta.view.expect("view expected"));
                    // Handle concurrent s table updates
                    for concurrent in concurrent_deltas.iter() {
                        match concurrent {
                            DbMutation::Removal(EitherKey::S(skey)) => {
                                if self.db.s.get_weight(skey) <= 0 {
                                    self.db.v.remove(skey, ukey);
                                }
                            }
                            DbMutation::Insert((EitherKey::S(skey), svec)) => {
                                if self.db.s.get_weight(skey) >= 1 {
                                    self.db.insert_into_view(skey, ukey, svec, uvec);
                                }
                            }
                            _ => {}
                        };
                    }
                }
                (1, 0)
            }
        };
        self.version_vector.insert(&delta.dot);
        self.dot_map.insert(delta.dot, delta.mutation);
        counts
    }

    /// Returns all concurrently inserted keys that has already been seen
    pub fn get_all_concurrent_deltas<'a>(
        &'a self,
        dot_set: &'a DotSet,
    ) -> impl Iterator<Item = &'a DbMutation> + 'a {
        self.dot_map
            .get_all_greater_iter(dot_set)
            .map(|(_, delta)| delta)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vector_experiment::{get_max_distance, random_vector, ExperimentConfig};
    use rand::SeedableRng;
    use rand_chacha::ChaCha8Rng;
    use std::time::Duration;

    #[test]
    fn test_merge_delta() {
        let mut dbc = VectorDBC::new(1, VectorDb::new(Float(10.0)));
        // Insert same value twice
        dbc.mutate(DbMutation::Insert((
            EitherKey::S(1),
            vec![Float(1.0), Float(2.0), Float(5.0)],
        )))
        .expect("Mutation of database failed");
        dbc.mutate(DbMutation::Insert((
            EitherKey::S(1),
            vec![Float(1.0), Float(2.0), Float(5.0)],
        )))
        .expect("Mutation of database failed");

        let deltas = vec![
            // Remove instance with two different pids
            DbDelta {
                dot: Dot { pid: 2, counter: 0 },
                mutation: DbMutation::Removal(EitherKey::S(1)),
                view: None,
            },
            DbDelta {
                dot: Dot { pid: 3, counter: 0 },
                mutation: DbMutation::Removal(EitherKey::S(1)),
                view: None,
            },
            // Add one S vector and one U vector twice
            DbDelta {
                dot: Dot { pid: 3, counter: 1 },
                mutation: DbMutation::Insert((
                    EitherKey::U(1),
                    vec![Float(0.0), Float(1.0), Float(2.0)],
                )),
                view: Some(vec![((2, 1), Float(5.0))]),
            },
            DbDelta {
                dot: Dot { pid: 2, counter: 1 },
                mutation: DbMutation::Insert((
                    EitherKey::S(2),
                    vec![Float(4.0), Float(1.0), Float(5.0)],
                )),
                view: Some(vec![((2, 1), Float(5.0))]),
            },
            DbDelta {
                dot: Dot { pid: 2, counter: 2 },
                mutation: DbMutation::Insert((
                    EitherKey::U(1),
                    vec![Float(0.0), Float(1.0), Float(2.0)],
                )),
                view: Some(vec![((2, 1), Float(5.0))]),
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

        dbc.merge_delta_group(delta_group);
        // Check S table
        assert_eq!(dbc.db.s.len(), 1);
        assert_eq!(
            dbc.db.s.get(&2),
            Some(&vec![Float(4.0), Float(1.0), Float(5.0)])
        );
        // Check U table
        assert_eq!(dbc.db.u.len(), 1);
        assert_eq!(dbc.db.u.get_weight(&1), 2);
        assert_eq!(
            dbc.db.u.get(&1),
            Some(&vec![Float(0.0), Float(1.0), Float(2.0)])
        );
        // Check view
        assert_eq!(dbc.db.v.len(), 1);
        assert_eq!(dbc.db.v.get_value(&2, &1), Some(&Float(5.0)));
    }

    #[test]
    fn test_network_size() {
        // use config to have prebuild functionality
        let table_size = 10000;
        let selectivity = 0.2;
        let dimensions = 128;
        let cfg = ExperimentConfig {
            db_dir_path: to_absolute(format!("tests/dbs/{table_size}_{selectivity}/")),
            server_list_file_path: to_absolute("tests/"),
            result_dir_path: to_absolute("tests/results/testing_results/"),
            config_path: to_absolute("tests/"),
            runtime: 0,
            clients: 0,
            servers: 10,
            sync_interval: Duration::from_secs(0),
            dimensions,
            max_distance: get_max_distance(selectivity, dimensions).unwrap(),
            init_s: table_size,
            init_u: table_size,
            k: 0,
            eps: 0,
            percent_reads: 0.0,
            percent_inserts: 0.0,
            percent_s: 0.0,
        };

        let s_inserts = 250;
        let s_removals = 250;
        let u_inserts = 250;
        let u_removals = 250;

        let mut db = VectorDBC::new(1, cfg.prebuild_db());
        let seed = [255; 32];
        let mut rng = ChaCha8Rng::from_seed(seed);

        // Mutate database
        for i in 0..s_inserts {
            db.mutate(DbMutation::Insert((
                EitherKey::S(table_size + i),
                random_vector(&mut rng, dimensions),
            )))
            .expect("Mutation of database failed");
        }
        for i in 0..u_inserts {
            db.mutate(DbMutation::Insert((
                EitherKey::U(table_size + i),
                random_vector(&mut rng, dimensions),
            )))
            .expect("Mutation of database failed");
        }
        for i in 0..s_removals {
            db.mutate(DbMutation::Removal(EitherKey::S(i)))
                .expect("Mutation of database failed");
        }
        for i in 0..u_removals {
            db.mutate(DbMutation::Removal(EitherKey::U(i)))
                .expect("Mutation of database failed");
        }

        // Get network view size
        let dot_set = DotSet::new();
        let (delta, insert_count, removal_count) = db.get_delta(&dot_set);
        assert_eq!(insert_count as u64, s_inserts + u_inserts);
        assert_eq!(removal_count as u64, s_removals + u_removals);

        let json = serde_json::to_string(&delta).expect("Failed to serialize");
        let byte_size = json.len() + 1;

        assert!(byte_size < 250 * 1_000_000 / 8) // smaller than 250mbit
    }
}
