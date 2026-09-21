use crate::crdt::PersistentReplica;
use crate::crdt::{DeltaGroup, CRDT};
use log::warn;
use nix::errno::Errno;
use nix::fcntl::{Flock, FlockArg};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex as StdMutex;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RawDurabilityRecord {
    #[serde(default)]
    generation: u64,
    record_type: String,
    payload: serde_json::Value,
}

/// Durable storage used by a replica. Implementations are selected at compile
/// time; `DiskJournal` is the production implementation.
pub(crate) trait Journal: Send + Sync {
    fn recover<T: CRDT + Debug + Clone>(
        &self,
    ) -> Result<Option<PersistentReplica<T>>, JournalError>;
    fn append_snapshot<T: CRDT + Debug + Clone>(
        &self,
        snapshot: &PersistentReplica<T>,
    ) -> Result<(), JournalError>;
    fn append_delta_group<T: CRDT + Debug + Clone>(
        &self,
        delta_group: &DeltaGroup<T::Delta, T::SideEffects>,
    ) -> Result<(), JournalError>;
    fn append_mutation<T: CRDT + Debug + Clone>(
        &self,
        mutation: &T::Mutation,
    ) -> Result<(), JournalError>;
}

#[derive(Debug)]
struct DiskJournalPaths {
    snapshot: PathBuf,
    mutations: PathBuf,
}

/// A crash-safe, process-exclusive disk journal.
///
/// Snapshots and mutations use separate files. A generation stamped on both
/// means recovery ignores an old log if a crash happens after snapshot
/// replacement but before log truncation.
#[derive(Debug)]
pub(crate) struct DiskJournal {
    paths: Option<DiskJournalPaths>,
    _process_lock: Option<Flock<std::fs::File>>,
    append_lock: StdMutex<()>,
    generation: StdMutex<u64>,
}

impl DiskJournal {
    pub(crate) fn new(path: PathBuf) -> Result<Self, JournalError> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let lock_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(Self::lock_path(&path))?;
        let process_lock = match Flock::lock(lock_file, FlockArg::LockExclusiveNonblock) {
            Ok(lock) => lock,
            Err((_, Errno::EWOULDBLOCK)) => return Err(JournalError::AlreadyLocked(path)),
            Err((_, source)) => return Err(JournalError::Lock { path, source }),
        };
        let paths = DiskJournalPaths {
            snapshot: Self::snapshot_path(&path),
            mutations: Self::mutation_log_path(&path),
        };
        let generation = Self::snapshot_generation(&paths.snapshot)?;
        Ok(Self {
            paths: Some(paths),
            _process_lock: Some(process_lock),
            append_lock: StdMutex::new(()),
            generation: StdMutex::new(generation),
        })
    }

    /// Keeps a concrete journal in Replica when durability is disabled.
    pub(crate) fn disabled() -> Self {
        Self {
            paths: None,
            _process_lock: None,
            append_lock: StdMutex::new(()),
            generation: StdMutex::new(0),
        }
    }

    pub(crate) fn snapshot_path(path: &Path) -> PathBuf {
        Self::sibling_path(path, "snapshot.json")
    }

    pub(crate) fn mutation_log_path(path: &Path) -> PathBuf {
        Self::sibling_path(path, "mutations.jsonl")
    }

    pub(crate) fn lock_path(path: &Path) -> PathBuf {
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("durability");
        path.with_file_name(format!(".{file_name}.lock"))
    }

    fn sibling_path(path: &Path, suffix: &str) -> PathBuf {
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("durability");
        path.with_file_name(format!("{file_name}.{suffix}"))
    }

    fn snapshot_generation(path: &Path) -> Result<u64, JournalError> {
        if !path.exists() {
            return Ok(0);
        }
        let record: RawDurabilityRecord = serde_json::from_slice(&fs::read(path)?)?;
        if record.record_type != "snapshot" {
            return Err(JournalError::UnknownRecordType(record.record_type));
        }
        Ok(record.generation)
    }

    fn paths(&self) -> Option<&DiskJournalPaths> {
        self.paths.as_ref()
    }

    fn recover_locked<T: CRDT + Debug + Clone>(
        &self,
        paths: &DiskJournalPaths,
    ) -> Result<Option<PersistentReplica<T>>, JournalError> {
        if !paths.snapshot.exists() {
            if paths.mutations.exists() && fs::metadata(&paths.mutations)?.len() > 0 {
                return Err(JournalError::MissingSnapshot);
            }
            return Ok(None);
        }
        let snapshot_record: RawDurabilityRecord =
            serde_json::from_slice(&fs::read(&paths.snapshot)?)?;
        if snapshot_record.record_type != "snapshot" {
            return Err(JournalError::UnknownRecordType(snapshot_record.record_type));
        }
        let generation = snapshot_record.generation;
        let mut recovered: PersistentReplica<T> = serde_json::from_value(snapshot_record.payload)?;
        let mut discard_suffix = false;

        if paths.mutations.exists() {
            let mut reader = BufReader::new(std::fs::File::open(&paths.mutations)?);
            let mut line = Vec::new();
            loop {
                line.clear();
                if reader.read_until(b'\n', &mut line)? == 0 {
                    break;
                }
                if !line.ends_with(b"\n") {
                    warn!(
                        "discarding incomplete mutation record from {}",
                        paths.mutations.display()
                    );
                    discard_suffix = true;
                    break;
                }
                if line.iter().all(u8::is_ascii_whitespace) {
                    continue;
                }
                let record: RawDurabilityRecord = match serde_json::from_slice(&line) {
                    Ok(record) => record,
                    Err(error) => {
                        warn!(
                            "discarding corrupt mutation-log suffix from {}: {error}",
                            paths.mutations.display()
                        );
                        discard_suffix = true;
                        break;
                    }
                };
                if record.generation != generation {
                    continue;
                }
                match record.record_type.as_str() {
                    "delta_group" => match serde_json::from_value(record.payload) {
                        Ok(delta) => recovered.local_state.merge_delta_group(delta),
                        Err(error) => {
                            warn!(
                                "discarding corrupt mutation-log suffix from {}: {error}",
                                paths.mutations.display()
                            );
                            discard_suffix = true;
                            break;
                        }
                    },
                    "mutation" => match serde_json::from_value(record.payload) {
                        Ok(mutation) => {
                            recovered.local_state.mutate(mutation);
                        }
                        Err(error) => {
                            warn!(
                                "discarding corrupt mutation-log suffix from {}: {error}",
                                paths.mutations.display()
                            );
                            discard_suffix = true;
                            break;
                        }
                    },
                    other => {
                        warn!("discarding corrupt mutation-log suffix from {}: unknown record type {other}", paths.mutations.display());
                        discard_suffix = true;
                        break;
                    }
                }
            }
        }

        if discard_suffix {
            warn!("repairing durability state from the last valid snapshot and mutation records");
            self.replace_snapshot_and_clear_log_locked(paths, generation + 1, &recovered)?;
            *self
                .generation
                .lock()
                .expect("durability journal generation lock poisoned") = generation + 1;
        }
        Ok(Some(recovered))
    }

    fn append_record_locked(
        &self,
        paths: &DiskJournalPaths,
        record_type: &str,
        payload: serde_json::Value,
    ) -> Result<(), JournalError> {
        let generation = *self
            .generation
            .lock()
            .expect("durability journal generation lock poisoned");
        let record = RawDurabilityRecord {
            generation,
            record_type: record_type.to_owned(),
            payload,
        };
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&paths.mutations)?;
        serde_json::to_writer(&mut file, &record)?;
        file.write_all(b"\n")?;
        file.sync_data()?;
        Ok(())
    }

    fn replace_snapshot_and_clear_log_locked<T: CRDT + Debug + Clone>(
        &self,
        paths: &DiskJournalPaths,
        generation: u64,
        snapshot: &PersistentReplica<T>,
    ) -> Result<(), JournalError> {
        let record = RawDurabilityRecord {
            generation,
            record_type: "snapshot".to_owned(),
            payload: serde_json::to_value(snapshot)?,
        };
        self.replace_snapshot_record_locked(paths, &record)?;
        self.clear_mutation_log_locked(paths)
    }

    fn replace_snapshot_record_locked(
        &self,
        paths: &DiskJournalPaths,
        record: &RawDurabilityRecord,
    ) -> Result<(), JournalError> {
        let mut bytes = serde_json::to_vec(record)?;
        bytes.push(b'\n');
        self.replace_bytes_locked(&paths.snapshot, bytes)
    }

    fn clear_mutation_log_locked(&self, paths: &DiskJournalPaths) -> Result<(), JournalError> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&paths.mutations)?;
        file.sync_all()?;
        Self::sync_parent(&paths.mutations)
    }

    fn replace_bytes_locked(&self, path: &Path, bytes: Vec<u8>) -> Result<(), JournalError> {
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("durability.snapshot.json");
        let replacement_path =
            path.with_file_name(format!(".{file_name}.{}.tmp", std::process::id()));
        let mut replacement = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&replacement_path)?;
        replacement.write_all(&bytes)?;
        replacement.sync_all()?;
        fs::rename(&replacement_path, path)?;
        Self::sync_parent(path)
    }

    fn sync_parent(path: &Path) -> Result<(), JournalError> {
        if let Some(parent) = path.parent() {
            std::fs::File::open(parent)?.sync_all()?;
        }
        Ok(())
    }
}

impl Journal for DiskJournal {
    fn recover<T: CRDT + Debug + Clone>(
        &self,
    ) -> Result<Option<PersistentReplica<T>>, JournalError> {
        let _append_guard = self
            .append_lock
            .lock()
            .expect("durability journal lock poisoned");
        let Some(paths) = self.paths() else {
            return Ok(None);
        };
        self.recover_locked(paths)
    }

    fn append_snapshot<T: CRDT + Debug + Clone>(
        &self,
        snapshot: &PersistentReplica<T>,
    ) -> Result<(), JournalError> {
        let _append_guard = self
            .append_lock
            .lock()
            .expect("durability journal lock poisoned");
        let Some(paths) = self.paths() else {
            return Ok(());
        };
        let mut generation = self
            .generation
            .lock()
            .expect("durability journal generation lock poisoned");
        let next_generation = *generation + 1;
        self.replace_snapshot_and_clear_log_locked(paths, next_generation, snapshot)?;
        *generation = next_generation;
        Ok(())
    }

    fn append_delta_group<T: CRDT + Debug + Clone>(
        &self,
        delta_group: &DeltaGroup<T::Delta, T::SideEffects>,
    ) -> Result<(), JournalError> {
        let _append_guard = self
            .append_lock
            .lock()
            .expect("durability journal lock poisoned");
        let Some(paths) = self.paths() else {
            return Ok(());
        };
        self.append_record_locked(paths, "delta_group", serde_json::to_value(delta_group)?)
    }

    fn append_mutation<T: CRDT + Debug + Clone>(
        &self,
        mutation: &T::Mutation,
    ) -> Result<(), JournalError> {
        let _append_guard = self
            .append_lock
            .lock()
            .expect("durability journal lock poisoned");
        let Some(paths) = self.paths() else {
            return Ok(());
        };
        self.append_record_locked(paths, "mutation", serde_json::to_value(mutation)?)
    }
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum JournalError {
    #[error("durability journal requires a snapshot before mutation records")]
    MissingSnapshot,
    #[error("unknown durability record type: {0}")]
    UnknownRecordType(String),
    #[error("durability journal is already in use: {0}")]
    AlreadyLocked(PathBuf),
    #[error("failed to lock durability journal {path}: {source}")]
    Lock { path: PathBuf, source: Errno },
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}
