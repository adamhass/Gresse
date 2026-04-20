use crate::helpers::{l2_norm, EitherKey, Float, Key, SKey, UKey, Vector};
use gresse::db::{composite_view::CompositeView, ztable::ZTable};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct VectorDb {
    pub s: ZTable<SKey, Vector>,
    pub u: ZTable<UKey, Vector>,
    pub v: CompositeView<SKey, Float>,
    pub max_distance: Float,
}

impl VectorDb {
    pub fn new(max_distance: Float) -> Self {
        VectorDb {
            s: ZTable::new(),
            u: ZTable::new(),
            v: CompositeView::new(),
            max_distance,
        }
    }

    /// Inserts an element into s, and updates the view v accordingly.
    pub fn insert_into_s(&mut self, key: SKey, value: Vector) {
        if self.s.insert(key, value.clone()) == 1 {
            // s.distinct is changing from this insert...
            self.update_view_from_s(&key, &value);
        }
    }

    /// Updates the view v based on the new insertion into s.
    fn update_view_from_s(&mut self, key: &SKey, value: &Vector) {
        for (ukey, uvec) in self.u.iter_all() {
            // Compute the view for matching keys in s and u
            let dist = l2_norm(value, uvec);
            if dist < self.max_distance {
                self.v.insert((*key, *ukey), dist);
            }
        }
    }

    /// Inserts an element into u, and updates the view v accordingly.
    pub fn insert_into_u(&mut self, key: UKey, value: Vector) {
        if self.u.insert(key, value.clone()) == 1 {
            // u.distinct set is changing from this insert...
            self.update_view_from_u(&key, &value);
        }
    }

    /// Updates all the views in v with the new u element.
    fn update_view_from_u(&mut self, key: &UKey, value: &Vector) {
        for (skey, svec) in self.s.iter_all() {
            // Compute the view for matching keys in s and u
            let dist = l2_norm(value, svec);
            if dist < self.max_distance {
                self.v.insert((*skey, *key), dist);
            }
        }
    }

    pub fn insert_into_view(&mut self, skey: &SKey, ukey: &UKey, svec: &Vector, uvec: &Vector) {
        let dist = l2_norm(svec, uvec);
        if dist < self.max_distance {
            self.v.insert((*skey, *ukey), dist);
        }
    }

    pub fn remove(&mut self, key: &EitherKey) {
        match key {
            EitherKey::S(skey) => {
                if self.s.remove(skey) == 0 {
                    self.v.remove_by_key_s(skey);
                }
            }
            EitherKey::U(ukey) => {
                if self.u.remove(ukey) == 0 {
                    self.v.remove_by_key_u(ukey);
                }
            }
        }
    }

    pub fn get_view(&self, key: &EitherKey) -> Vec<((Key, Key), Float)> {
        match key {
            EitherKey::S(skey) => self.v.get_by_key_s(skey),
            EitherKey::U(ukey) => self.v.get_by_key_u(ukey),
        }
        .unwrap_or_default()
    }

    pub fn save_db(&self, dir_path: PathBuf) -> Result<(), Box<dyn std::error::Error>> {
        std::fs::create_dir_all(dir_path.clone())?;
        self.s.to_file(dir_path.clone().join("s.json"))?;
        self.u.to_file(dir_path.clone().join("u.json"))?;
        self.v.to_file(dir_path.join("v.json"))?;
        Ok(())
    }
}
