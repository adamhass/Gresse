use super::*;
use crate::journal::{DiskJournal, Journal, JournalError, RawDurabilityRecord};
use crate::network::network_connection_candidates;
use crate::orset::{ORSet, OrSetMutation, OrSetQuery, OrSetResponse};
use crate::prelude::{ObjectStorageConfig, ServerAddr};
use std::collections::HashSet;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn temp_journal_path(name: &str) -> PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock drifted before unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("gresse-{name}-{unique}.jsonl"))
}

fn cleanup_journal(path: &PathBuf) {
    for path in [
        DiskJournal::snapshot_path(path),
        DiskJournal::mutation_log_path(path),
        DiskJournal::lock_path(path),
    ] {
        let _ = std::fs::remove_file(path);
    }
}

fn membership_descriptor(
    pid: Pid,
    gc_counter: Counter,
    final_counter: Option<Counter>,
) -> ReplicaDescriptor {
    ReplicaDescriptor {
        pid,
        address: "127.0.0.1:19080".parse().unwrap(),
        gc_counter,
        final_counter,
    }
}

#[test]
fn network_connection_candidates_deduplicates_and_skips_departed_members() {
    let members = vec![
        membership_descriptor(1, 1, None),
        membership_descriptor(1, 2, None),
        membership_descriptor(2, 1, None),
        membership_descriptor(2, 2, Some(9)),
        membership_descriptor(3, 1, None),
        membership_descriptor(4, 1, None),
    ];
    let connected_pids = HashSet::from([4]);

    let candidates = network_connection_candidates(&members, 3, &connected_pids);

    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].pid, 1);
    assert_eq!(candidates[0].gc_counter, 2);
}

#[test]
fn recovered_replica_uses_persisted_pid() {
    let recovered = PersistentReplica {
        local_state: ORSet::<String>::new(),
        crdt_wrapper: CRDTWrapper::new(41),
    };

    assert_eq!(
        Replica::<ORSet<String>>::effective_pid(99, Some(&recovered)),
        41
    );
    assert_eq!(Replica::<ORSet<String>>::effective_pid(99, None), 99);
}

#[test]
fn rebound_object_storage_state_persists_the_launch_pid() {
    let journal_path = temp_journal_path("rebound-bootstrap");
    let journal =
        DiskJournal::new(journal_path.clone()).expect("failed to create durability journal");
    let mut state = ORSet::<String>::new();
    state.set_pid(1);
    state
        .mutate(OrSetMutation::Insert("source".into()))
        .unwrap();
    let mut downloaded = PersistentReplica {
        local_state: state,
        crdt_wrapper: CRDTWrapper::new(1),
    };

    downloaded.rebind_own_pid(2);
    journal.append_snapshot(&downloaded);

    let mut recovered = journal
        .recover::<ORSet<String>>()
        .expect("failed to recover rebound bootstrap state")
        .expect("rebound bootstrap state missing from journal");
    assert_eq!(recovered.crdt_wrapper.own_pid(), 2);
    recovered
        .local_state
        .mutate(OrSetMutation::Insert("local".into()))
        .unwrap();
    assert_eq!(
        recovered.local_state.get_version_vector().counter(&2),
        Some(0)
    );

    cleanup_journal(&journal_path);
}

#[tokio::test]
#[serial_test::serial]
async fn object_storage_bootstrap_journals_the_new_replica_identity() {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock drifted before unix epoch")
        .as_nanos();
    let test_root = std::env::temp_dir().join(format!("gresse-bootstrap-pid-{unique}"));
    let object_store_root = test_root.join("object-store");
    let journal_path = test_root.join("replica.journal");
    std::fs::create_dir_all(&object_store_root).expect("failed to create object-store directory");

    let mut source_state = ORSet::<String>::new();
    source_state.set_pid(1);
    source_state
        .mutate(OrSetMutation::Insert("source".into()))
        .unwrap();
    let source_snapshot = PersistentReplica {
        local_state: source_state,
        crdt_wrapper: CRDTWrapper::new(1),
    };
    std::fs::write(
        object_store_root.join("persistent.json"),
        serde_json::to_vec(&source_snapshot).expect("failed to serialize source snapshot"),
    )
    .expect("failed to seed object storage");

    let address = ServerAddr {
        ip: "127.0.0.1".parse().expect("failed to parse loopback"),
        http_port: 19190,
        internal_port: 18180,
    };
    let config = ReplicaConfig {
        address,
        advertised_address: address,
        sync_interval: Duration::from_secs(60),
        gc_interval: Duration::from_secs(60),
        durability_path: Some(journal_path.clone()),
        object_storage_config: ObjectStorageConfig {
            local_dir: Some(object_store_root),
            url: None,
            region: "local".to_string(),
            bucket: "local".to_string(),
            access_key: None,
            secret_key: None,
            session_token: None,
            persistent_replica_path: "persistent.json".to_string(),
            membership_directory_path: "membership".to_string(),
            discovery_interval: Duration::from_secs(60),
        },
    };
    let (mut replica, shutdown_sender) =
        Replica::with_config(2, ORSet::<String>::new(), config).await;
    let replica_task = tokio::spawn(async move { replica.run().await });
    let _ = shutdown_sender.send(());
    tokio::time::timeout(Duration::from_secs(5), replica_task)
        .await
        .expect("replica shutdown timed out")
        .expect("replica task failed");

    let journal =
        DiskJournal::new(journal_path.clone()).expect("failed to reopen durability journal");
    let recovered = journal
        .recover::<ORSet<String>>()
        .expect("failed to recover bootstrapped journal")
        .expect("bootstrapped journal was empty");
    assert_eq!(recovered.crdt_wrapper.own_pid(), 2);

    drop(journal);
    std::fs::remove_dir_all(test_root).expect("failed to clean bootstrap test directory");
}

#[test]
fn durability_journal_recovers_snapshot_and_delta_groups() {
    let journal_path = temp_journal_path("durability");
    let journal =
        DiskJournal::new(journal_path.clone()).expect("failed to create durability journal");

    let mut base = ORSet::<String>::new();
    base.set_pid(1);
    base.mutate(OrSetMutation::Insert("apple".into())).unwrap();

    journal.append_snapshot(&PersistentReplica {
        local_state: base.clone(),
        crdt_wrapper: CRDTWrapper::new(1),
    });

    let version_vector_before = base.get_version_vector().clone();
    let mut candidate = base.clone();
    candidate
        .mutate(OrSetMutation::Insert("banana".into()))
        .unwrap();
    let delta_group = candidate.get_delta(&version_vector_before);

    journal.append_delta_group::<ORSet<String>>(&delta_group);

    let recovered = journal
        .recover::<ORSet<String>>()
        .expect("failed to recover durability journal")
        .expect("expected recovered replica");

    assert_eq!(
        recovered
            .local_state
            .query(OrSetQuery::Elements)
            .expect("failed to query recovered ORSet"),
        OrSetResponse::Elements(vec!["apple".into(), "banana".into()])
    );

    cleanup_journal(&journal_path);
}

#[test]
fn durability_snapshot_compacts_prior_history() {
    let journal_path = temp_journal_path("snapshot-compaction");
    let journal =
        DiskJournal::new(journal_path.clone()).expect("failed to create durability journal");

    let mut state = ORSet::<String>::new();
    state.set_pid(1);
    state.mutate(OrSetMutation::Insert("apple".into())).unwrap();
    journal.append_snapshot(&PersistentReplica {
        local_state: state.clone(),
        crdt_wrapper: CRDTWrapper::new(1),
    });

    journal
        .append_mutation::<ORSet<String>>(&OrSetMutation::Insert("banana".into()))
        .expect("failed to append mutation");
    state
        .mutate(OrSetMutation::Insert("banana".into()))
        .unwrap();
    journal.append_snapshot(&PersistentReplica {
        local_state: state,
        crdt_wrapper: CRDTWrapper::new(1),
    });

    let contents = std::fs::read_to_string(DiskJournal::snapshot_path(&journal_path))
        .expect("failed to read compacted snapshot");
    assert_eq!(contents.lines().count(), 1);
    assert!(
        std::fs::read_to_string(DiskJournal::mutation_log_path(&journal_path))
            .expect("failed to read compacted mutation log")
            .is_empty()
    );
    let recovered = journal
        .recover::<ORSet<String>>()
        .expect("failed to recover compacted journal")
        .expect("expected recovered replica");
    assert_eq!(
        recovered
            .local_state
            .query(OrSetQuery::Elements)
            .expect("failed to query recovered ORSet"),
        OrSetResponse::Elements(vec!["apple".into(), "banana".into()])
    );

    cleanup_journal(&journal_path);
}

#[test]
fn durability_recovery_ignores_a_log_left_by_an_interrupted_compaction() {
    let journal_path = temp_journal_path("compaction-generation");
    let journal =
        DiskJournal::new(journal_path.clone()).expect("failed to create durability journal");
    let mut state = ORSet::<String>::new();
    state.set_pid(1);
    state.mutate(OrSetMutation::Insert("apple".into())).unwrap();
    journal.append_snapshot(&PersistentReplica {
        local_state: state.clone(),
        crdt_wrapper: CRDTWrapper::new(1),
    });
    journal
        .append_mutation::<ORSet<String>>(&OrSetMutation::Insert("banana".into()))
        .expect("failed to append mutation");
    state
        .mutate(OrSetMutation::Insert("banana".into()))
        .unwrap();
    journal.append_snapshot(&PersistentReplica {
        local_state: state,
        crdt_wrapper: CRDTWrapper::new(1),
    });

    // A crash after the atomic snapshot replacement but before truncating
    // the former log leaves generation 1 records behind. Recovery must
    // ignore them rather than replay them over generation 2.
    std::fs::write(
        DiskJournal::mutation_log_path(&journal_path),
        b"{\"generation\":1,\"record_type\":\"mutation\",\"payload\":{\"Insert\":\"obsolete\"}}\n",
    )
    .expect("failed to restore stale mutation log");

    let recovered = journal
        .recover::<ORSet<String>>()
        .expect("failed to recover compacted journal")
        .expect("expected recovered replica");
    assert_eq!(
        recovered
            .local_state
            .query(OrSetQuery::Elements)
            .expect("failed to query recovered ORSet"),
        OrSetResponse::Elements(vec!["apple".into(), "banana".into()])
    );
    cleanup_journal(&journal_path);
}

#[test]
fn durability_journal_discards_an_incomplete_trailing_record() {
    let journal_path = temp_journal_path("torn-tail");
    let journal =
        DiskJournal::new(journal_path.clone()).expect("failed to create durability journal");

    let mut base = ORSet::<String>::new();
    base.set_pid(1);
    base.mutate(OrSetMutation::Insert("apple".into())).unwrap();
    journal.append_snapshot(&PersistentReplica {
        local_state: base,
        crdt_wrapper: CRDTWrapper::new(1),
    });

    let mut file = OpenOptions::new()
        .append(true)
        .open(DiskJournal::mutation_log_path(&journal_path))
        .expect("failed to reopen durability journal");
    file.write_all(br#"{"record_type":"mutation","payload":"#)
        .expect("failed to write incomplete record");
    file.sync_data().expect("failed to sync incomplete record");

    let recovered = journal
        .recover::<ORSet<String>>()
        .expect("incomplete trailing record should be ignored")
        .expect("expected recovered replica");
    assert_eq!(
        recovered
            .local_state
            .query(OrSetQuery::Elements)
            .expect("failed to query recovered ORSet"),
        OrSetResponse::Elements(vec!["apple".into()])
    );
    let contents = std::fs::read_to_string(DiskJournal::mutation_log_path(&journal_path))
        .expect("failed to read repaired mutation log");
    assert!(contents.is_empty());

    cleanup_journal(&journal_path);
}

#[test]
fn durability_journal_repairs_a_corrupt_non_tail_suffix() {
    let journal_path = temp_journal_path("corrupt-suffix");
    let journal =
        DiskJournal::new(journal_path.clone()).expect("failed to create durability journal");

    let mut base = ORSet::<String>::new();
    base.set_pid(1);
    base.mutate(OrSetMutation::Insert("apple".into())).unwrap();
    journal.append_snapshot(&PersistentReplica {
        local_state: base,
        crdt_wrapper: CRDTWrapper::new(1),
    });

    let mut file = OpenOptions::new()
        .append(true)
        .open(DiskJournal::mutation_log_path(&journal_path))
        .expect("failed to reopen durability journal");
    // This models a torn write followed by a later writer appending its
    // own complete record.  The two fragments form one malformed physical
    // line, so merely discarding an unterminated final line is insufficient.
    file.write_all(br#"{"record_type":"mutation","payload":{"Insert":"#)
        .expect("failed to write torn record prefix");
    file.write_all(br#"{"record_type":"mutation","payload":{"Insert":"banana"}}\n"#)
        .expect("failed to write later record");
    file.sync_data().expect("failed to sync corrupt suffix");

    let recovered = journal
        .recover::<ORSet<String>>()
        .expect("corrupt suffix should be repaired")
        .expect("expected recovered replica");
    assert_eq!(
        recovered
            .local_state
            .query(OrSetQuery::Elements)
            .expect("failed to query recovered ORSet"),
        OrSetResponse::Elements(vec!["apple".into()])
    );

    let contents = std::fs::read_to_string(DiskJournal::mutation_log_path(&journal_path))
        .expect("failed to read repaired mutation log");
    assert!(contents.is_empty());

    journal
        .append_mutation::<ORSet<String>>(&OrSetMutation::Insert("cherry".into()))
        .expect("failed to append after repair");
    let recovered_after_append = journal
        .recover::<ORSet<String>>()
        .expect("repaired journal should remain recoverable")
        .expect("expected recovered replica");
    assert_eq!(
        recovered_after_append
            .local_state
            .query(OrSetQuery::Elements)
            .expect("failed to query recovered ORSet"),
        OrSetResponse::Elements(vec!["apple".into(), "cherry".into()])
    );

    cleanup_journal(&journal_path);
}

#[test]
fn durability_journal_excludes_another_process_owner() {
    let journal_path = temp_journal_path("exclusive-lock");
    let first =
        DiskJournal::new(journal_path.clone()).expect("failed to create first durability journal");

    assert!(matches!(
        DiskJournal::new(journal_path.clone()),
        Err(JournalError::AlreadyLocked(path)) if path == journal_path
    ));

    drop(first);
    DiskJournal::new(journal_path.clone())
        .expect("journal should be available after its owner exits");
    cleanup_journal(&journal_path);
}

#[test]
fn durability_journal_serializes_concurrent_appends() {
    let journal_path = temp_journal_path("concurrent-appends");
    let journal = Arc::new(
        DiskJournal::new(journal_path.clone()).expect("failed to create durability journal"),
    );
    let mut base = ORSet::<String>::new();
    base.set_pid(1);
    journal.append_snapshot(&PersistentReplica {
        local_state: base,
        crdt_wrapper: CRDTWrapper::new(1),
    });

    let writers = (0..16)
        .map(|index| {
            let journal = journal.clone();
            thread::spawn(move || {
                journal
                    .append_mutation::<ORSet<String>>(&OrSetMutation::Insert(format!(
                        "value-{index}"
                    )))
                    .expect("failed to append concurrent mutation");
            })
        })
        .collect::<Vec<_>>();
    for writer in writers {
        writer.join().expect("concurrent writer panicked");
    }

    let contents = std::fs::read_to_string(DiskJournal::mutation_log_path(&journal_path))
        .expect("failed to read mutation log");
    assert_eq!(contents.lines().count(), 16);
    for line in contents.lines() {
        serde_json::from_str::<RawDurabilityRecord>(line)
            .expect("concurrent append produced malformed JSON");
    }
    journal
        .recover::<ORSet<String>>()
        .expect("concurrent append journal should recover");

    cleanup_journal(&journal_path);
}
