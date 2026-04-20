use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fmt,
};

/// A View supporting range queries for the values and point queries (and efficient inserts) for the keys.
#[derive(Serialize, Deserialize, Clone, PartialEq)]
pub struct View<Key: std::hash::Hash + std::cmp::Eq + Ord + PartialEq, Value: Ord + PartialEq> {
    // Maps a Key to a Value.
    key_to_value: HashMap<Key, Value>,

    // Maps a Value to a set of Keys, maintaining order on Values.
    value_to_keys: BTreeMap<Value, HashSet<Key>>,
}

impl<Key: std::hash::Hash + std::cmp::Eq + Ord + PartialEq, Value: Ord + PartialEq> fmt::Debug
    for View<Key, Value>
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("View")
            .field("key_to_value length", &self.key_to_value.len())
            .field("value_to_keys length", &self.value_to_keys.len())
            .finish()
    }
}

impl<Key: Eq + std::hash::Hash + Clone + Ord, Value: Ord + Clone> Default for View<Key, Value> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Key: Eq + std::hash::Hash + Clone + Ord, Value: Ord + Clone> View<Key, Value> {
    /// Creates a new empty View.
    pub fn new() -> Self {
        View {
            key_to_value: HashMap::new(),
            value_to_keys: BTreeMap::new(),
        }
    }

    pub fn insert_values(&mut self, values: &Vec<(Key, Value)>) {
        for (k, v) in values {
            self.insert(k.clone(), v.clone())
        }
    }

    pub fn len(&self) -> usize {
        self.key_to_value.len()
    }

    pub fn is_empty(&self) -> bool {
        self.key_to_value.is_empty()
    }

    pub fn to_vec(&self) -> Vec<(Key, Value)> {
        self.key_to_value
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Inserts a new Key -> Value mapping.
    /// If the Key already exists, the Value is updated.
    pub fn insert(&mut self, key: Key, value: Value) {
        if let Some(old_value) = self.key_to_value.insert(key.clone(), value.clone()) {
            // Remove the Key from the old Value's set in value_to_keys.
            if let Some(set) = self.value_to_keys.get_mut(&old_value) {
                set.remove(&key);
                if set.is_empty() {
                    self.value_to_keys.remove(&old_value);
                }
            }
        }

        // Insert the Key into the new Value's set in value_to_keys.
        self.value_to_keys
            .entry(value.clone())
            .or_default()
            .insert(key);
    }

    /// Removes a Key and its associated Value. Returns true iff the key was in the view
    pub fn remove_by_key(&mut self, key: &Key) -> bool {
        if let Some(value) = self.key_to_value.remove(key) {
            // Remove the Key from the Value's set in value_to_keys.
            if let Some(set) = self.value_to_keys.get_mut(&value) {
                set.remove(key);
                if set.is_empty() {
                    self.value_to_keys.remove(&value);
                }
            }
            true
        } else {
            false
        }
    }

    /// Removes all Keys associated with a Value.
    pub fn remove_by_value(&mut self, value: &Value) {
        if let Some(keys) = self.value_to_keys.remove(value) {
            for key in keys {
                self.key_to_value.remove(&key);
            }
        }
    }

    /// Query the Value for a given Key.
    pub fn get_value(&self, key: &Key) -> Option<&Value> {
        self.key_to_value.get(key)
    }

    /// Query all Keys associated with a given Value.
    pub fn get_keys_by_value(&self, value: &Value) -> Option<&HashSet<Key>> {
        self.value_to_keys.get(value)
    }

    /// Perform a range query: get all Keys for Values in the specified range.
    /// Returns a collection of (Value, Keys) pairs where the Values are in the given range.
    pub fn range_query(&self, range: std::ops::Range<Value>) -> Vec<(Value, HashSet<Key>)> {
        self.value_to_keys
            .range(range)
            .map(|(value, keys)| (value.clone(), keys.clone()))
            .collect()
    }

    /// Returns the `k` lowest Values with their associated Keys.
    pub fn min_k(&self, k: usize) -> Vec<(Value, Key)> {
        self.value_to_keys
            .iter()
            .flat_map(|(value, keys)| keys.iter().map(move |key| (value.clone(), key.clone())))
            .take(k)
            .collect()
    }
}
