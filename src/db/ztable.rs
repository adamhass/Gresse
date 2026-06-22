use std::{
    cmp::Eq,
    collections::HashMap,
    error::Error,
    fs::File,
    hash::{DefaultHasher, Hash, Hasher},
    io::{BufReader, BufWriter},
    path::Path,
    sync::Arc,
};

use serde::{Deserialize, Serialize};
use tokio::{runtime::Handle, task};

pub type Weight = isize;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ZTable<K: Eq + Hash + Copy + std::fmt::Display, V: Default + PartialEq> {
    shards: Vec<Arc<HashMap<K, (Weight, V)>>>,
}

impl<K: Eq + Hash + Copy + std::fmt::Display, V: Clone + Default + PartialEq> Default
    for ZTable<K, V>
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Eq + Hash + Copy + std::fmt::Display, V: Clone + Default + PartialEq> ZTable<K, V> {
    pub fn new() -> ZTable<K, V> {
        Self::new_with_shards(1)
    }

    pub fn new_with_shards(shard_count: usize) -> ZTable<K, V> {
        let shard_count = shard_count.max(1);
        ZTable {
            shards: (0..shard_count)
                .map(|_| Arc::new(HashMap::new()))
                .collect(),
        }
    }

    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    fn shard_index(&self, key: &K) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % self.shards.len()
    }

    fn shard(&self, key: &K) -> &HashMap<K, (Weight, V)> {
        &self.shards[self.shard_index(key)]
    }

    fn shard_mut(&mut self, key: &K) -> &mut HashMap<K, (Weight, V)> {
        let shard_index = self.shard_index(key);
        Arc::make_mut(&mut self.shards[shard_index])
    }

    /// Inserts the key and value, increasing the weight if it's already in there.
    pub fn insert(&mut self, key: K, value: V) -> Weight {
        let inner = self.shard_mut(&key);
        if let Some((weight, val)) = inner.get_mut(&key) {
            *val = value;
            *weight += 1;
            *weight
        } else {
            inner.insert(key, (1, value));
            1
        }
    }

    /// Decerements the weight if it's in there, or inserts with negative
    pub fn remove(&mut self, key: &K) -> Weight {
        let inner = self.shard_mut(key);
        let weight = if let Some((weight, _)) = inner.get_mut(key) {
            *weight -= 1;
            *weight
        } else {
            inner.insert(*key, (-1, V::default()));
            -1
        };
        if weight == 0 {
            let _ = inner.remove(key);
        }
        weight
    }

    pub fn get_weight(&self, key: &K) -> Weight {
        self.shard(key)
            .get(key)
            .map(|(weight, _)| *weight)
            .unwrap_or(0)
    }

    /// Returns an iterator over all active key-value pairs in the ZTable.
    pub fn iter_all(&self) -> impl Iterator<Item = (&K, &V)> {
        self.shards.iter().flat_map(|shard| {
            shard
                .iter()
                .filter(|(_, (weight, _))| *weight > 0)
                .map(|(key, (_, value))| (key, value))
        })
    }

    /// Retrieves the value associated with the key, if it exists.
    pub fn get(&self, key: &K) -> Option<&V> {
        self.shard(key).get(key).map(|(_, value)| value)
    }

    pub fn len(&self) -> usize {
        self.shards.iter().map(|shard| shard.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.shards.iter().all(|shard| shard.is_empty())
    }

    pub async fn map_reduce_active<ShardOutput, Output, MapShard, Reduce>(
        &self,
        map_shard: MapShard,
        init: Output,
        reduce: Reduce,
    ) -> Output
    where
        K: Send + Sync + 'static,
        V: Send + Sync + 'static,
        ShardOutput: Send + 'static,
        Output: Send,
        MapShard: Fn(usize, &HashMap<K, (Weight, V)>) -> ShardOutput + Send + Sync + Copy + 'static,
        Reduce: Fn(Output, ShardOutput) -> Output + Send + Sync + Copy,
    {
        if self.shards.len() == 1 {
            return reduce(init, map_shard(0, &self.shards[0]));
        }

        let handle = match Handle::try_current() {
            Ok(handle) => handle,
            Err(_) => {
                let mut output = init;
                for (shard_index, shard) in self.shards.iter().enumerate() {
                    output = reduce(output, map_shard(shard_index, shard));
                }
                return output;
            }
        };

        task::block_in_place(|| {
            handle.block_on(async {
                let mut handles = Vec::with_capacity(self.shards.len());
                for (shard_index, shard) in self.shards.iter().enumerate() {
                    let shard = Arc::clone(shard);
                    handles.push(task::spawn_blocking(move || map_shard(shard_index, &shard)));
                }

                let mut output = init;
                for handle in handles {
                    output = reduce(output, handle.await.expect("ztable shard task failed"));
                }
                output
            })
        })
    }
}

impl<K, V> ZTable<K, V>
where
    K: Eq + Hash + Copy + std::fmt::Display + Serialize + for<'de> Deserialize<'de>,
    V: Clone + Default + PartialEq + Serialize + for<'de> Deserialize<'de>,
{
    pub fn to_file<P: AsRef<Path>>(&self, path: P) -> Result<(), Box<dyn Error>> {
        let file = File::create(path)?;
        let writer = BufWriter::new(file);
        serde_json::to_writer_pretty(writer, &self)?;
        Ok(())
    }

    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn Error>> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        Ok(serde_json::from_reader(reader)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::runtime::Builder;

    #[test]
    fn test_ztable() {
        let mut table = ZTable::new_with_shards(4);

        table.insert("a", 30);
        table.insert("b", 20);
        table.insert("a", 30);

        assert_eq!(table.get(&"a"), Some(&30));
        assert_eq!(table.get(&"b"), Some(&20));
        assert_eq!(table.get(&"c"), None);

        let mut all_values: Vec<(_, _)> = table.iter_all().collect();
        all_values.sort_by(|a, b| a.0.cmp(b.0));
        assert_eq!(all_values, vec![(&"a", &30), (&"b", &20)]);
    }

    #[test]
    fn test_map_reduce_active() {
        let mut table = ZTable::new_with_shards(4);
        table.insert("a", 10);
        table.insert("b", 20);
        table.insert("c", 30);
        table.remove(&"missing");

        let runtime = Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();

        let (sum, mut keys) = runtime.block_on(async {
            table
                .map_reduce_active(
                    |_shard_index, shard| {
                        let mut local_sum = 0;
                        let mut local_keys = Vec::new();
                        for (key, (weight, value)) in shard {
                            if *weight > 0 {
                                local_sum += *value;
                                local_keys.push(*key);
                            }
                        }
                        (local_sum, local_keys)
                    },
                    (0, Vec::new()),
                    |(sum, mut keys), (local_sum, mut local_keys)| {
                        keys.append(&mut local_keys);
                        (sum + local_sum, keys)
                    },
                )
                .await
        });

        keys.sort();
        assert_eq!(sum, 60);
        assert_eq!(keys, vec!["a", "b", "c"]);
    }
}
