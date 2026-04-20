use std::{
    cmp::Eq,
    collections::HashMap,
    error::Error,
    fs::File,
    io::{BufReader, BufWriter},
    path::Path,
};

use serde::{Deserialize, Serialize};

pub type Weight = isize;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ZTable<K: Eq + std::hash::Hash + Copy + std::fmt::Display, V: Default + PartialEq> {
    inner: HashMap<K, (Weight, V)>,
}

impl<K: Eq + std::hash::Hash + Copy + std::fmt::Display, V: Clone + Default + PartialEq> Default
    for ZTable<K, V>
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Eq + std::hash::Hash + Copy + std::fmt::Display, V: Clone + Default + PartialEq>
    ZTable<K, V>
{
    pub fn new() -> ZTable<K, V> {
        ZTable {
            inner: HashMap::new(),
        }
    }

    /// Inserts the key and value, increasing the weight if it's already in there.
    pub fn insert(&mut self, key: K, value: V) -> Weight {
        // Check if the key exists in the HashMap
        if let Some((weight, val)) = self.inner.get_mut(&key) {
            // INFO: We always overwrite the current vectors there
            // This means that using this for CRDTs means that the keys and the
            // values are NOT independent
            // One should not try to add different vectors at the same key, as this
            // could lead to inconsistencies if done concurrently
            *val = value;
            *weight += 1;
            *weight
        } else {
            // If the key doesn't exist, insert it with weight 1
            self.inner.insert(key, (1, value));
            1
        }
    }

    /// Decerements the weight if it's in there, or inserts with negative
    pub fn remove(&mut self, key: &K) -> Weight {
        // Check if the key exists in the HashMap
        let weight = if let Some((weight, _)) = self.inner.get_mut(key) {
            // If it exists, decrement the weight
            *weight -= 1;
            *weight
        } else {
            // If the key doesn't exist, insert it with weight -1
            self.inner.insert(*key, (-1, V::default()));
            -1
        };
        if weight == 0 {
            let _ = self.inner.remove(key);
        }
        weight
    }

    pub fn get_weight(&self, key: &K) -> Weight {
        self.inner.get(key).map(|(weight, _)| *weight).unwrap_or(0)
    }

    /// Returns an iterator over all the key-value pairs in the ZTable.
    pub fn iter_all(&self) -> impl Iterator<Item = (&K, &V)> {
        self.inner
            .iter()
            .filter(|(_, (weight, _))| *weight > 0)
            .map(|(key, (_, value))| (key, value))
    }

    /// Retrieves the value associated with the key, if it exists.
    pub fn get(&self, key: &K) -> Option<&V> {
        self.inner.get(key).map(|(_, value)| value)
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

impl<K, V> ZTable<K, V>
where
    K: Eq + std::hash::Hash + Copy + std::fmt::Display + Serialize + for<'de> Deserialize<'de>,
    V: Clone + Default + PartialEq + Serialize + for<'de> Deserialize<'de>,
{
    pub fn to_file<P: AsRef<Path>>(&self, path: P) -> Result<(), Box<dyn Error>> {
        let file = File::create(path)?;
        let writer = BufWriter::new(file);
        serde_json::to_writer_pretty(writer, &self.inner)?;
        Ok(())
    }

    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn Error>> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let inner: HashMap<K, (Weight, V)> = serde_json::from_reader(reader)?;
        Ok(ZTable { inner })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ztable() {
        let mut table = ZTable {
            inner: HashMap::new(),
        };

        // Test insert
        table.insert("a", 30);
        table.insert("b", 20);
        table.insert("a", 30);

        // Test get
        assert_eq!(table.get(&"a"), Some(&30));
        assert_eq!(table.get(&"b"), Some(&20));
        assert_eq!(table.get(&"c"), None);

        // Test iter_all
        let mut all_values: Vec<(_, _)> = table.iter_all().collect();
        all_values.sort_by(|a, b| a.0.cmp(b.0)); // Sorting to ensure order doesn't affect test
        assert_eq!(all_values, vec![(&"a", &30), (&"b", &20)]);
    }
}
