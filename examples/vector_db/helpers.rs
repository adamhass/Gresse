use serde::{Deserialize, Serialize};
use std::{
    cmp::{Eq, Ord, Ordering},
    env,
    path::{Path, PathBuf},
};
use thiserror::Error;

pub const HOST: &str = "db_server";
pub const URI: &str = "/db";

pub type Dimensions = u32;
pub type Vector = Vec<Float>;
pub type Key = u64;

#[derive(Debug, Error, Clone, Serialize, Deserialize)]
pub enum DbError {
    #[error("Key not found")]
    KeyNotFound,
}

// VectorDB stores S and U and supports the fast query S nearest neighbors in U
// The KNN in U is the view, that is incrementally updated.
// The view V is maintained
pub type SKey = Key;
pub type UKey = Key;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "type", content = "params")]
pub enum DbRequest {
    Query((Key, usize)),
    Mutation(DbMutation),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", content = "params")]
pub enum DbMutation {
    Insert((EitherKey, Vector)),
    Removal(EitherKey),
}

pub type DbResponse = Result<Option<Vec<(Float, Key)>>, DbError>;

impl DbRequest {
    pub fn query(key: Key, k: usize) -> Self {
        DbRequest::Query((key, k))
    }

    pub fn vector(key: EitherKey, vector: Vector) -> Self {
        DbRequest::Mutation(DbMutation::Insert((key, vector)))
    }

    pub fn remove(key: EitherKey) -> Self {
        DbRequest::Mutation(DbMutation::Removal(key))
    }

    pub fn record_str(&self) -> String {
        match self {
            DbRequest::Mutation(DbMutation::Removal(key)) => match key {
                EitherKey::S(_) => "s-removal".into(),
                EitherKey::U(_) => "u-removal".into(),
            },
            DbRequest::Query(_) => "query".into(),
            DbRequest::Mutation(DbMutation::Insert((key, _))) => match key {
                EitherKey::S(_) => "s-insertion".into(),
                EitherKey::U(_) => "u-insertion".into(),
            },
            // DbRequest::Mutation(DbMutation::ObsoleteInsert(_)) => "obsolete".into(),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Ord, PartialOrd, Hash)]
#[serde(tag = "type", content = "key")]
pub enum EitherKey {
    S(SKey),
    U(UKey),
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, Default)] // Derive PartialEq and Eq since we're implementing Ord
pub struct Float(pub f32);

pub fn l2_norm(vec1: &Vector, vec2: &Vector) -> Float {
    assert_eq!(vec1.len(), vec2.len(), "Vectors must be of the same length");
    let sum_of_squares: f32 = vec1
        .iter()
        .zip(vec2.iter())
        .map(|(a, b)| {
            let diff = a.0 - b.0;
            diff * diff
        })
        .sum();
    Float(sum_of_squares.sqrt())
}

impl Eq for Float {}
impl PartialOrd for Float {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let a = self.0;
        let b = other.0;

        // Handle NaN comparisons
        if a.is_nan() && b.is_nan() {
            return Some(Ordering::Equal);
        }
        if a.is_nan() {
            return Some(Ordering::Greater); // NaN is considered greater
        }
        if b.is_nan() {
            return Some(Ordering::Less);
        }

        // Handle infinity comparisons
        if a.is_infinite() && b.is_infinite() {
            if a.is_sign_positive() && b.is_sign_positive() {
                return Some(Ordering::Equal);
            } else if a.is_sign_negative() && b.is_sign_negative() {
                return Some(Ordering::Equal);
            } else if a.is_sign_positive() {
                return Some(Ordering::Greater); // Positive infinity is greater
            } else {
                return Some(Ordering::Less); // Negative infinity is lesser
            }
        }

        // For finite numbers, fall back to normal comparison
        Some(a.partial_cmp(&b).unwrap_or(Ordering::Equal))
    }
}

impl Ord for Float {
    fn cmp(&self, other: &Self) -> Ordering {
        let a = self.0;
        let b = other.0;

        // Handle NaN comparisons
        if a.is_nan() && b.is_nan() {
            return Ordering::Equal;
        }
        if a.is_nan() {
            return Ordering::Greater; // NaN is considered greater
        }
        if b.is_nan() {
            return Ordering::Less;
        }

        // Handle infinity comparisons
        if a.is_infinite() && b.is_infinite() {
            if a.is_sign_positive() && b.is_sign_positive() {
                return Ordering::Equal;
            } else if a.is_sign_negative() && b.is_sign_negative() {
                return Ordering::Equal;
            } else if a.is_sign_positive() {
                return Ordering::Greater; // Positive infinity is greater
            } else {
                return Ordering::Less; // Negative infinity is lesser
            }
        }

        // For finite numbers, fall back to the normal comparison
        a.partial_cmp(&b).unwrap_or(Ordering::Equal)
    }
}

pub fn to_absolute<P: AsRef<Path>>(input: P) -> PathBuf {
    let path = input.as_ref();
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        env::current_dir().unwrap().join(path)
    }
}
