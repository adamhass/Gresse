use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeSet, HashMap, HashSet},
    error::Error,
    fmt,
    fs::File,
    io::{BufReader, BufWriter},
    path::Path,
};

/// A CompositeView supporting queries by composite keys (KeyS, KeyU) and individual components.
/// Maintains efficient indexing to support all query types.
#[derive(Serialize, Deserialize, Clone, PartialEq)]
pub struct CompositeView<Key, Value>
where
    Key: std::hash::Hash + std::cmp::Eq + Ord + PartialEq + Clone + Copy,
    Value: Ord + PartialEq + Clone + Default,
{
    // Maps a CompositeKey to a Value
    #[serde(with = "tuple_vec_map")]
    key_to_value: HashMap<(Key, Key), Value>,

    // Secondary indexes for individual key lookups
    key_s_index: HashMap<Key, HashSet<Key>>,
    key_u_index: HashMap<Key, HashSet<Key>>,

    // Maps a Key S to a set of (Value, Key U) pairs, maintaining order on Values
    value_index: HashMap<Key, BTreeSet<(Value, Key)>>,
}

impl<Key, Value> fmt::Debug for CompositeView<Key, Value>
where
    Key: std::hash::Hash + std::cmp::Eq + Ord + PartialEq + Clone + Copy,
    Value: Ord + PartialEq + Clone + Default,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut debug_struct = f.debug_struct("CompositeView");

        debug_struct.field("key_to_value", &self.key_to_value.len());
        debug_struct.field("key_s_index length", &self.key_s_index.len());
        debug_struct.field("key_u_index length", &self.key_u_index.len());
        debug_struct.field("value_to_keys length", &self.value_index.len());

        debug_struct.finish()
    }
}

impl<Key, Value> Default for CompositeView<Key, Value>
where
    Key: std::hash::Hash + std::cmp::Eq + Ord + PartialEq + Clone + Copy + Serialize,
    Value: Ord + PartialEq + Clone + Default + Serialize,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<Key, Value> CompositeView<Key, Value>
where
    Key: std::hash::Hash + std::cmp::Eq + Ord + PartialEq + Clone + Copy + Serialize,
    Value: Ord + PartialEq + Clone + Default + Serialize,
{
    /// Creates a new empty CompositeView.
    pub fn new() -> Self {
        CompositeView {
            key_to_value: HashMap::new(),
            key_s_index: HashMap::new(),
            key_u_index: HashMap::new(),
            value_index: HashMap::new(),
        }
    }

    pub fn insert_values(&mut self, values: Vec<((Key, Key), Value)>) {
        for (k, v) in values {
            self.insert(k, v)
        }
    }

    /// Returns the number of key-value pairs in the view
    pub fn len(&self) -> usize {
        self.key_to_value.len()
    }

    /// Returns true if the view contains no key-value pairs
    pub fn is_empty(&self) -> bool {
        self.key_to_value.is_empty()
    }

    pub fn export_sorted_view(&self, path: &str) -> std::io::Result<()> {
        // key to value hashmap
        let mut entries: Vec<(Key, Key, Value)> = self
            .key_to_value
            .iter()
            .map(|(&(k1, k2), v)| (k1, k2, v.clone()))
            .collect();
        entries.sort_by(|a, b| (&a.0, &a.1).cmp(&(&b.0, &b.1)));

        // key s index
        // let mut entries: Vec<(Key, Vec<Key>)> = self
        //     .key_s_index
        //     .iter()
        //     .map(|(&k, set)| {
        //         let mut sorted_set: Vec<Key> = set.iter().cloned().collect();
        //         sorted_set.sort();
        //         (k, sorted_set)
        //     })
        //     .collect();
        // entries.sort_by(|a, b| a.0.cmp(&b.0));

        // key u index
        // let mut entries: Vec<(Key, Vec<Key>)> = self
        //     .key_u_index
        //     .iter()
        //     .map(|(&k, set)| {
        //         let mut sorted_set: Vec<Key> = set.iter().cloned().collect();
        //         sorted_set.sort();
        //         (k, sorted_set)
        //     })
        //     .collect();
        // entries.sort_by(|a, b| a.0.cmp(&b.0));

        // value index
        // let mut entries: Vec<(Key, Vec<(Value, Key)>)> = self
        //     .value_index
        //     .iter()
        //     .map(|(&outer_key, inner_map)| {
        //         let inner_entries: Vec<(Value, Key)> =
        //             inner_map.iter().map(|(val, k)| (val.clone(), *k)).collect();
        //         (outer_key, inner_entries)
        //     })
        //     .collect();
        // entries.sort_by(|a, b| a.0.cmp(&b.0));
        // Write to json file
        let mut file = File::create(path)?;
        serde_json::to_writer_pretty(&mut file, &entries)?;
        Ok(())
    }

    // /// Convert the view to a vector of ((Key, Key), Value) pairs
    // pub fn to_vec(&self) -> Vec<((Key, Key), Value)> {
    //     let mut result = Vec::new();

    //     for (value, composite_keys) in &self.value_to_keys {
    //         for composite_key in composite_keys {
    //             result.push((composite_key.clone(), value.clone()));
    //         }
    //     }

    //     result
    // }

    /// Inserts a new (Key, Key) -> Value mapping.
    /// If the composite key already exists, the method does nothing.
    /// This method ensures that the CompositeView is always complete,
    /// i.e. all keys are present in all sets.
    pub fn insert(&mut self, composite_key: (Key, Key), value: Value) {
        // No need to do anything if the value exists
        if self.key_to_value.contains_key(&composite_key) {
            return;
        }
        let (key_s, key_u) = composite_key;
        self.key_to_value.insert(composite_key, value.clone());

        // Update secondary indices
        // 1. Update key_s_index
        self.key_s_index.entry(key_s).or_default().insert(key_u);

        // 2. Update key_u_index
        self.key_u_index.entry(key_u).or_default().insert(key_s);

        // 3. Update the value index
        self.value_index
            .entry(key_s)
            .or_default()
            .insert((value, key_u));
    }

    /// Removes a composite key and its associated Value.
    /// Returns true if the key was in the view.
    pub fn remove(&mut self, key_s: &Key, key_u: &Key) -> bool {
        let composite_key = (*key_s, *key_u);

        // Remove from HashMap and get the old value
        let value_opt = self.key_to_value.remove(&composite_key);

        if let Some(value) = value_opt {
            // Remove from key_s_index
            if let Some(u_set) = self.key_s_index.get_mut(key_s) {
                u_set.remove(key_u);
                if u_set.is_empty() {
                    self.key_s_index.remove(key_s);
                }
            }

            // Remove from key_u_index
            if let Some(s_set) = self.key_u_index.get_mut(key_u) {
                s_set.remove(key_s);
                if s_set.is_empty() {
                    self.key_u_index.remove(key_u);
                }
            }

            // Remove from value index
            if let Some(btree) = self.value_index.get_mut(key_s) {
                btree.remove(&(value, *key_u));
                if btree.is_empty() {
                    self.value_index.remove(key_s);
                }
            }
            true
        } else {
            false
        }
    }

    /// Removes all entries with the specified first key component.
    /// Returns the number of entries removed.
    pub fn remove_by_key_s(&mut self, key_s: &Key) -> usize {
        let mut count = 0;

        // Collect all second key values to avoid borrowing issues
        let key_us: Vec<Key> = if let Some(u_set) = self.key_s_index.get(key_s) {
            u_set.iter().cloned().collect()
        } else {
            return 0;
        };

        // Remove each composite key
        for key_u in key_us {
            if self.remove(key_s, &key_u) {
                count += 1;
            }
        }
        count
    }

    /// Removes all entries with the specified second key component.
    /// Returns the number of entries removed.
    pub fn remove_by_key_u(&mut self, key_u: &Key) -> usize {
        let mut count = 0;

        // Collect all first key values to avoid borrowing issues
        let key_ss: Vec<Key> = if let Some(s_set) = self.key_u_index.get(key_u) {
            s_set.iter().cloned().collect()
        } else {
            return 0;
        };

        // Remove each composite key
        for key_s in key_ss {
            if self.remove(&key_s, key_u) {
                count += 1;
            }
        }
        count
    }

    // /// Removes all keys associated with a Value.
    // pub fn remove_by_value(&mut self, value: &Value) {
    //     if let Some(key_set) = self.value_index.remove(value) {
    //         // Clone keys to avoid borrowing issues
    //         let keys: Vec<(Key, Key)> = key_set.iter().cloned().collect();

    //         for (key_s, key_u) in keys {
    //             self.remove(&key_s, &key_u);
    //         }
    //     }
    // }

    /// Get the value for a composite key
    pub fn get_value(&self, key_s: &Key, key_u: &Key) -> Option<&Value> {
        self.key_to_value.get(&(*key_s, *key_u))
    }

    /// Get all second key values associated with a given first key
    pub fn get_keys_b(&self, key_s: &Key) -> Option<&HashSet<Key>> {
        self.key_s_index.get(key_s)
    }

    /// Get all first key values associated with a given second key
    pub fn get_keys_a(&self, key_u: &Key) -> Option<&HashSet<Key>> {
        self.key_u_index.get(key_u)
    }

    /// Get all pairs ((Key, Key), &Value) associated with key_s
    pub fn get_by_key_s(&self, key_s: &Key) -> Option<Vec<((Key, Key), Value)>> {
        self.value_index.get(key_s).map(|btree| {
            btree
                .iter()
                .map(|(value, key_u)| ((*key_s, *key_u), value.clone()))
                .collect()
        })
    }

    /// Get all pairs ((Key, Key), &Value) associated with key_u
    pub fn get_by_key_u(&self, key_u: &Key) -> Option<Vec<((Key, Key), Value)>> {
        self.key_u_index.get(key_u).map(|set| {
            set.iter()
                .map(|key_s| {
                    let composite_key = (*key_s, *key_u);
                    let val = self
                        .key_to_value
                        .get(&composite_key)
                        .expect("missing value in composite view");
                    (composite_key, val.clone())
                })
                .collect()
        })
    }

    /*
     * QUERIES
     */
    pub fn query_a<'a>(
        &'a self,
        key_s: &'a Key,
    ) -> Option<impl Iterator<Item = (&'a Key, &'a Value)> + 'a> {
        let key_us = self.key_s_index.get(key_s);
        key_us.map(|key_us| {
            key_us.iter().filter_map(move |key_u| {
                let composite = (*key_s, *key_u);
                self.key_to_value.get(&composite).map(|v| (key_u, v))
            })
        })
    }

    /// Query by second key component
    /// Returns an iterator over (first key, Value) pairs where second key matches the given value
    pub fn query_b<'a>(
        &'a self,
        key_u: &'a Key,
    ) -> Option<impl Iterator<Item = (&'a Key, &'a Value)> + 'a> {
        let key_ss = self.key_u_index.get(key_u);
        key_ss.map(|key_ss| {
            key_ss.iter().filter_map(move |key_s| {
                let composite = (*key_s, *key_u);
                self.key_to_value.get(&composite).map(|v| (key_s, v))
            })
        })
    }

    /// Returns the `k` lowest Values with their associated composite Keys.
    pub fn min_k(&self, key_s: &Key, k: usize) -> Option<Vec<(Value, Key)>> {
        self.value_index.get(key_s).map(|btree| {
            btree
                .iter()
                .take(k)
                .map(|(value, key)| (value.clone(), *key))
                .collect()
        })
    }
}

impl<K, V> CompositeView<K, V>
where
    K: std::hash::Hash
        + std::cmp::Eq
        + Ord
        + PartialEq
        + Clone
        + Copy
        + Serialize
        + for<'de> Deserialize<'de>,
    V: Ord + PartialEq + Clone + Default + Serialize + for<'de> Deserialize<'de>,
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
        let table = serde_json::from_reader(reader)?;
        Ok(table)
    }
}

mod tuple_vec_map {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::collections::HashMap;

    pub fn serialize<S, K, V>(map: &HashMap<(K, K), V>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        K: serde::Serialize + Clone,
        V: serde::Serialize,
    {
        let vec: Vec<((K, K), &V)> = map.iter().map(|(k, v)| (k.clone(), v)).collect();
        vec.serialize(serializer)
    }

    pub fn deserialize<'de, D, K, V>(deserializer: D) -> Result<HashMap<(K, K), V>, D::Error>
    where
        D: Deserializer<'de>,
        K: serde::Deserialize<'de> + Eq + std::hash::Hash,
        V: serde::Deserialize<'de>,
    {
        let vec: Vec<((K, K), V)> = Vec::deserialize(deserializer)?;
        Ok(vec.into_iter().collect())
    }
}
