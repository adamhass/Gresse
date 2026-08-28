use crate::crdt::{DeltaGroup, CRDT};
use crate::replica::PersistentReplica;
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
    record_type: String,
    payload: serde_json::Value,
}

/// A crash-safe, process-exclusive journal for one replica's durable state.
#[derive(Debug)]
pub(crate) struct Journal {
    path: PathBuf,
    // Held for the journal lifetime. The mutex serializes threads in this
    // replica; the OS lock excludes a duplicate process using the same path.
    _process_lock: Flock<std::fs::File>,
    append_lock: StdMutex<()>,
}

impl Journal {
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
        Ok(Self {
            path,
            _process_lock: process_lock,
            append_lock: StdMutex::new(()),
        })
    }

    pub(crate) fn lock_path(path: &Path) -> PathBuf {
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("durability.journal");
        path.with_file_name(format!(".{file_name}.lock"))
    }

    pub(crate) fn recover<T: CRDT + Debug + Clone>(
        &self,
    ) -> Result<Option<PersistentReplica<T>>, JournalError> {
        let _guard = self
            .append_lock
            .lock()
            .expect("durability journal lock poisoned");
        if !self.path.exists() {
            return Ok(None);
        }

        let mut reader = BufReader::new(std::fs::File::open(&self.path)?);
        let mut recovered: Option<PersistentReplica<T>> = None;
        let mut discard_suffix = false;
        let mut line = Vec::new();
        loop {
            line.clear();
            if reader.read_until(b'\n', &mut line)? == 0 {
                break;
            }
            if !line.ends_with(b"\n") {
                warn!(
                    "discarding incomplete durability record from {}",
                    self.path.display()
                );
                discard_suffix = recovered.is_some();
                break;
            }
            if line.iter().all(u8::is_ascii_whitespace) {
                continue;
            }
            let record: RawDurabilityRecord = match serde_json::from_slice(&line) {
                Ok(record) => record,
                Err(error) if recovered.is_some() => {
                    warn!(
                        "discarding corrupt durability journal suffix from {}: {error}",
                        self.path.display()
                    );
                    discard_suffix = true;
                    break;
                }
                Err(error) => return Err(error.into()),
            };
            match record.record_type.as_str() {
                "snapshot" => match serde_json::from_value(record.payload) {
                    Ok(snapshot) => recovered = Some(snapshot),
                    Err(error) if recovered.is_some() => {
                        warn!(
                            "discarding corrupt durability journal suffix from {}: {error}",
                            self.path.display()
                        );
                        discard_suffix = true;
                        break;
                    }
                    Err(error) => return Err(error.into()),
                },
                "delta_group" => {
                    let Some(snapshot) = recovered.as_mut() else {
                        return Err(JournalError::MissingSnapshot);
                    };
                    match serde_json::from_value(record.payload) {
                        Ok(delta) => snapshot.local_state.merge_delta_group(delta),
                        Err(error) => {
                            warn!(
                                "discarding corrupt durability journal suffix from {}: {error}",
                                self.path.display()
                            );
                            discard_suffix = true;
                            break;
                        }
                    }
                }
                "mutation" => {
                    let Some(snapshot) = recovered.as_mut() else {
                        return Err(JournalError::MissingSnapshot);
                    };
                    match serde_json::from_value(record.payload) {
                        Ok(mutation) => snapshot.local_state.mutate(mutation),
                        Err(error) => {
                            warn!(
                                "discarding corrupt durability journal suffix from {}: {error}",
                                self.path.display()
                            );
                            discard_suffix = true;
                            break;
                        }
                    };
                }
                other if recovered.is_some() => {
                    warn!("discarding corrupt durability journal suffix from {}: unknown record type {other}", self.path.display());
                    discard_suffix = true;
                    break;
                }
                other => return Err(JournalError::UnknownRecordType(other.to_owned())),
            }
        }

        if discard_suffix {
            if let Some(snapshot) = recovered.as_ref() {
                warn!(
                    "repairing durability journal {} from its last valid state",
                    self.path.display()
                );
                self.replace_record_locked(&RawDurabilityRecord {
                    record_type: "snapshot".to_owned(),
                    payload: serde_json::to_value(snapshot)?,
                })?;
            }
        }
        Ok(recovered)
    }

    pub(crate) fn append_snapshot<T: CRDT + Debug + Clone>(
        &self,
        snapshot: &PersistentReplica<T>,
    ) -> Result<(), JournalError> {
        self.replace_record(&RawDurabilityRecord {
            record_type: "snapshot".to_owned(),
            payload: serde_json::to_value(snapshot)?,
        })
    }

    pub(crate) fn replace_with_serialized_snapshot(
        &self,
        serialized_snapshot: &[u8],
    ) -> Result<(), JournalError> {
        let _guard = self
            .append_lock
            .lock()
            .expect("durability journal lock poisoned");
        self.replace_bytes_locked(
            [
                br#"{"record_type":"snapshot","payload":"# as &[u8],
                serialized_snapshot,
                b"}\n",
            ]
            .concat(),
        )
    }

    pub(crate) fn append_delta_group<T: CRDT + Debug + Clone>(
        &self,
        delta_group: &DeltaGroup<T::Delta, T::SideEffects>,
    ) -> Result<(), JournalError> {
        self.append_record(&RawDurabilityRecord {
            record_type: "delta_group".to_owned(),
            payload: serde_json::to_value(delta_group)?,
        })
    }

    pub(crate) fn append_mutation<T: CRDT + Debug + Clone>(
        &self,
        mutation: &T::Mutation,
    ) -> Result<(), JournalError> {
        self.append_record(&RawDurabilityRecord {
            record_type: "mutation".to_owned(),
            payload: serde_json::to_value(mutation)?,
        })
    }

    fn append_record(&self, record: &RawDurabilityRecord) -> Result<(), JournalError> {
        let _guard = self
            .append_lock
            .lock()
            .expect("durability journal lock poisoned");
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        serde_json::to_writer(&mut file, record)?;
        file.write_all(b"\n")?;
        file.sync_data()?;
        Ok(())
    }

    fn replace_record(&self, record: &RawDurabilityRecord) -> Result<(), JournalError> {
        let _guard = self
            .append_lock
            .lock()
            .expect("durability journal lock poisoned");
        self.replace_record_locked(record)
    }

    fn replace_record_locked(&self, record: &RawDurabilityRecord) -> Result<(), JournalError> {
        let mut bytes = serde_json::to_vec(record)?;
        bytes.push(b'\n');
        self.replace_bytes_locked(bytes)
    }

    fn replace_bytes_locked(&self, bytes: Vec<u8>) -> Result<(), JournalError> {
        let file_name = self
            .path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("durability.journal");
        let replacement_path = self
            .path
            .with_file_name(format!(".{file_name}.{}.tmp", std::process::id()));
        let mut replacement = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&replacement_path)?;
        replacement.write_all(&bytes)?;
        replacement.sync_all()?;
        fs::rename(&replacement_path, &self.path)?;
        if let Some(parent) = self.path.parent() {
            std::fs::File::open(parent)?.sync_all()?;
        }
        Ok(())
    }
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum JournalError {
    #[error("durability journal requires a snapshot before delta records")]
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
