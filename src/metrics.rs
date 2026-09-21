use crate::dots::Counter;
use crate::prelude::{now_micros, Pid};
use crate::replica_helpers::ReplicaDescriptor;
use csv::WriterBuilder;
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::path::Path;

#[derive(Debug, Serialize)]
pub(crate) struct MetricRecord {
    source: String,
    event: String,
    phase: String,
    timestamp_us: u128,
    replica_pid: Pid,
    peer_pid: Option<Pid>,
    gc_marker: Option<Counter>,
    sent_us: Option<u128>,
    received_us: Option<u128>,
    start_us: Option<u128>,
    end_us: Option<u128>,
    insert_count: Option<u16>,
    delete_count: Option<u16>,
    detail: Option<String>,
    client_id: Option<String>,
    operation: Option<String>,
    value: Option<i32>,
    status_code: Option<u16>,
    latency_us: Option<u128>,
}

/// Timestamps collected from replica construction through bootstrap completion.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StartupMetrics {
    pub crdt_pid_set_us: u128,
    pub http_server_ready_us: u128,
    pub network_manager_ready_us: u128,
    pub metric_writer_ready_us: u128,
    pub object_storage_client_start_us: u128,
    pub object_storage_client_ready_us: u128,
    pub replica_run_start_us: u128,
    pub replica_init_start_us: u128,
    pub persistent_state_fetch_completed_us: u128,
    pub membership_descriptor_write_completed_us: u128,
    pub membership_directory_read_completed_us: u128,
    pub replica_init_completed_us: u128,
}

impl StartupMetrics {
    pub(crate) fn construction_complete(
        crdt_pid_set_us: u128,
        http_server_ready_us: u128,
        network_manager_ready_us: u128,
        metric_writer_ready_us: u128,
        object_storage_client_start_us: u128,
        object_storage_client_ready_us: u128,
    ) -> Self {
        Self {
            crdt_pid_set_us,
            http_server_ready_us,
            network_manager_ready_us,
            metric_writer_ready_us,
            object_storage_client_start_us,
            object_storage_client_ready_us,
            ..Self::default()
        }
    }

    pub(crate) fn record_run_started(&mut self, timestamp_us: u128) {
        self.replica_run_start_us = timestamp_us;
    }

    pub(crate) fn record_bootstrap_completed(
        &mut self,
        replica_init_start_us: u128,
        persistent_state_fetch_completed_us: u128,
        membership_descriptor_write_completed_us: u128,
        membership_directory_read_completed_us: u128,
        replica_init_completed_us: u128,
    ) {
        self.replica_init_start_us = replica_init_start_us;
        self.persistent_state_fetch_completed_us = persistent_state_fetch_completed_us;
        self.membership_descriptor_write_completed_us = membership_descriptor_write_completed_us;
        self.membership_directory_read_completed_us = membership_directory_read_completed_us;
        self.replica_init_completed_us = replica_init_completed_us;
    }
}

/// A completed asynchronous operation together with its timing and CSV detail.
pub(crate) struct TimedResult<T> {
    pub(crate) value: T,
    pub(crate) start_us: u128,
    pub(crate) end_us: u128,
    pub(crate) detail: String,
}

impl<T> TimedResult<T> {
    pub(crate) async fn measure<F, D>(future: F, detail_fn: D) -> Self
    where
        F: Future<Output = T>,
        D: FnOnce(&T) -> String,
    {
        let start_us = now_micros();
        let value = future.await;
        let end_us = now_micros();
        let detail = detail_fn(&value);
        Self {
            value,
            start_us,
            end_us,
            detail,
        }
    }
}

pub(crate) struct MembershipInitResult {
    pub(crate) descriptor_write: TimedResult<()>,
    pub(crate) membership_list: TimedResult<()>,
    pub(crate) members: Vec<ReplicaDescriptor>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ClientMutationMetric {
    pub(crate) received_us: u128,
    pub(crate) start_us: u128,
    pub(crate) end_us: u128,
}

/// Per-replica CSV metrics with named protocol events.
pub(crate) struct Metrics {
    replica_pid: Pid,
    writer: csv::Writer<std::fs::File>,
}

impl Metrics {
    pub(crate) fn create(result_dir: &Path, replica_pid: Pid) -> Self {
        std::fs::create_dir_all(result_dir).expect("Failed to create metrics directory");
        let path = result_dir.join(format!("server_{replica_pid}.csv"));
        let file = std::fs::File::create(&path)
            .unwrap_or_else(|_| panic!("Failed to create result file {path:?}"));
        Self {
            replica_pid,
            writer: WriterBuilder::new().flexible(true).from_writer(file),
        }
    }

    pub(crate) fn client_mutation_completed(
        &mut self,
        received_us: u128,
        start_us: u128,
        end_us: u128,
    ) {
        self.span(
            "client_mutation",
            "completed",
            None,
            None,
            start_us,
            end_us,
            Some(received_us),
            Some("http_data_plane".to_owned()),
        )
    }

    pub(crate) fn peer_replication_connection_established(&mut self, peer_pid: Pid) {
        self.instant(
            "peer_replication_connection",
            "established",
            Some(peer_pid),
            None,
            None,
        );
    }

    pub(crate) fn recovered_membership_cleanup(
        &mut self,
        peer_pid: Pid,
        result: Result<(), String>,
    ) {
        match result {
            Ok(()) => self.instant(
                "recovered_membership_cleanup",
                "completed",
                Some(peer_pid),
                None,
                None,
            ),
            Err(detail) => self.instant(
                "recovered_membership_cleanup",
                "failed",
                Some(peer_pid),
                None,
                Some(detail),
            ),
        };
    }

    pub(crate) fn replica_init_started(&mut self) -> u128 {
        self.instant("replica_init", "start", None, None, None)
    }
    pub(crate) fn replica_init_completed(&mut self) -> u128 {
        self.instant("replica_init", "completed", None, None, None)
    }

    pub(crate) fn persistent_state_fetch_completed(
        &mut self,
        start_us: u128,
        end_us: u128,
        detail: String,
    ) {
        self.span(
            "persistent_state_fetch",
            "completed",
            None,
            None,
            start_us,
            end_us,
            None,
            Some(detail),
        );
    }
    pub(crate) fn persistent_state_fetch_instant(&mut self, detail: impl Into<String>) -> u128 {
        self.instant(
            "persistent_state_fetch",
            "completed",
            None,
            None,
            Some(detail.into()),
        )
    }

    pub(crate) fn membership_descriptor_write_completed(
        &mut self,
        start_us: u128,
        end_us: u128,
        detail: String,
    ) {
        self.span(
            "membership_descriptor_write",
            "completed",
            None,
            None,
            start_us,
            end_us,
            None,
            Some(detail),
        );
    }
    pub(crate) fn membership_directory_read_completed(
        &mut self,
        gc_marker: Option<Counter>,
        start_us: u128,
        end_us: u128,
        detail: String,
    ) {
        self.span(
            "membership_directory_read",
            "completed",
            None,
            gc_marker,
            start_us,
            end_us,
            None,
            Some(detail),
        );
    }
    pub(crate) fn membership_directory_read_failed(
        &mut self,
        gc_marker: Option<Counter>,
        start_us: u128,
        end_us: u128,
        operation: &str,
        error: impl std::fmt::Display,
    ) {
        self.span(
            "membership_directory_read",
            "failed",
            None,
            gc_marker,
            start_us,
            end_us,
            None,
            Some(format!("{operation}: {error}")),
        );
    }

    pub(crate) fn membership_directory_read_with_count(
        &mut self,
        gc_marker: Option<Counter>,
        start_us: u128,
        end_us: u128,
        operation: &str,
        descriptor_count: usize,
    ) {
        self.membership_directory_read_completed(
            gc_marker,
            start_us,
            end_us,
            format!("{operation}:{descriptor_count} descriptors"),
        );
    }

    pub(crate) fn persistent_state_write_completed(
        &mut self,
        start_us: u128,
        end_us: u128,
        detail: Option<String>,
    ) {
        self.span(
            "persistent_state_write",
            "completed",
            None,
            None,
            start_us,
            end_us,
            None,
            detail,
        )
    }
    pub(crate) fn persistent_state_write_failed(
        &mut self,
        start_us: u128,
        end_us: u128,
        detail: String,
    ) {
        self.span(
            "persistent_state_write",
            "failed",
            None,
            None,
            start_us,
            end_us,
            None,
            Some(detail),
        )
    }

    pub(crate) fn peer_replication_merge_delta_completed(
        &mut self,
        gc_marker: Option<Counter>,
        received_us: u128,
        start_us: u128,
        end_us: u128,
        delta_len: usize,
        sent_us: u128,
    ) {
        self.span(
            "peer_replication_merge_delta",
            "completed",
            None,
            gc_marker,
            start_us,
            end_us,
            Some(received_us),
            Some(format!("delta_len={delta_len},sent_us={sent_us}")),
        )
    }
    pub(crate) fn peer_replication_merge_delta_refused_stale_gc(
        &mut self,
        remote_gc_counter: Counter,
        current_gc_counter: Counter,
        received_us: u128,
        delta_len: usize,
        sent_us: u128,
    ) {
        let refused_us = now_micros();
        self.write(MetricRecord {
            source: "server".to_owned(),
            event: "peer_replication_merge_delta".to_owned(),
            phase: "refused_stale_gc".to_owned(),
            timestamp_us: refused_us,
            replica_pid: self.replica_pid,
            peer_pid: None,
            gc_marker: Some(remote_gc_counter),
            sent_us: Some(sent_us),
            received_us: Some(received_us),
            start_us: None,
            end_us: None,
            insert_count: None,
            delete_count: None,
            detail: Some(format!(
                "remote_gc_counter={remote_gc_counter},current_gc_counter={current_gc_counter},delta_len={delta_len}"
            )),
            client_id: None,
            operation: None,
            value: None,
            status_code: None,
            latency_us: Some(refused_us.saturating_sub(received_us)),
        });
    }
    pub(crate) fn peer_replication_get_delta_completed(
        &mut self,
        peer_pid: Pid,
        gc_marker: Option<Counter>,
        received_us: u128,
        start_us: u128,
        end_us: u128,
        delta_len: usize,
        sent_us: u128,
    ) {
        self.span(
            "peer_replication_get_delta",
            "completed",
            Some(peer_pid),
            gc_marker,
            start_us,
            end_us,
            Some(received_us),
            Some(format!("delta_len={delta_len},sent_us={sent_us}")),
        )
    }
    pub(crate) fn peer_replication_pull_request_sent(&mut self, connected_peers: usize) {
        self.instant(
            "peer_replication_pull_request",
            "sent",
            None,
            None,
            Some(format!("connected_peers={connected_peers}")),
        );
    }

    pub(crate) fn gc_init_started(&mut self, marker: Counter, previous_marker: Counter) {
        self.instant(
            "gc_init",
            "start",
            None,
            Some(marker),
            Some(format!("previous_gc_marker={previous_marker}")),
        );
    }
    pub(crate) fn gc_init_failed(&mut self, marker: Counter, detail: impl Into<String>) {
        self.instant("gc_init", "failed", None, Some(marker), Some(detail.into()));
    }
    pub(crate) fn gc_init_aborted(&mut self, marker: Counter) {
        self.instant(
            "gc_init",
            "aborted",
            None,
            Some(marker),
            Some("membership_changed".to_owned()),
        );
    }
    pub(crate) fn gc_local_collect_completed(
        &mut self,
        marker: Counter,
        start_us: u128,
        end_us: u128,
        departed_pids: &Option<Vec<Pid>>,
    ) {
        self.span(
            "gc_local_collect",
            "completed",
            None,
            Some(marker),
            start_us,
            end_us,
            None,
            Some(format!("departed_pids={departed_pids:?}")),
        );
    }
    pub(crate) fn gc_persistent_state_write_failed(&mut self, marker: Counter, detail: String) {
        self.instant(
            "gc_persistent_state_write",
            "failed",
            None,
            Some(marker),
            Some(detail),
        );
    }
    pub(crate) fn gc_persistent_state_write_completed(
        &mut self,
        marker: Counter,
        start_us: u128,
        end_us: u128,
        state_bytes: usize,
    ) {
        self.span(
            "gc_persistent_state_write",
            "completed",
            None,
            Some(marker),
            start_us,
            end_us,
            None,
            Some(format!("state_bytes={state_bytes}")),
        )
    }
    pub(crate) fn gc_finalize_completed(&mut self, marker: Counter, detail: String) {
        self.instant("gc_finalize", "completed", None, Some(marker), Some(detail));
    }
    pub(crate) fn gc_membership_cleanup_completed(&mut self, marker: Option<Counter>) {
        self.instant("gc_membership_cleanup", "completed", None, marker, None);
    }
    pub(crate) fn gc_membership_descriptor_write_completed(
        &mut self,
        marker: Counter,
        detail: String,
    ) {
        self.instant(
            "gc_membership_descriptor_write",
            "completed",
            None,
            Some(marker),
            Some(detail),
        );
    }
    pub(crate) fn gc_observed_started(&mut self, marker: Counter) {
        self.instant("gc_observed", "start", None, Some(marker), None);
    }
    pub(crate) fn gc_observed_completed(&mut self, marker: Counter) {
        self.instant("gc_observed", "completed", None, Some(marker), None);
    }

    fn instant(
        &mut self,
        event: &str,
        phase: &str,
        peer_pid: Option<Pid>,
        gc_marker: Option<Counter>,
        detail: Option<String>,
    ) -> u128 {
        let timestamp_us = now_micros();
        self.write(MetricRecord {
            source: "server".to_owned(),
            event: event.to_owned(),
            phase: phase.to_owned(),
            timestamp_us,
            replica_pid: self.replica_pid,
            peer_pid,
            gc_marker,
            sent_us: None,
            received_us: None,
            start_us: None,
            end_us: None,
            insert_count: None,
            delete_count: None,
            detail,
            client_id: None,
            operation: None,
            value: None,
            status_code: None,
            latency_us: None,
        });
        timestamp_us
    }

    fn span(
        &mut self,
        event: &str,
        phase: &str,
        peer_pid: Option<Pid>,
        gc_marker: Option<Counter>,
        start_us: u128,
        end_us: u128,
        received_us: Option<u128>,
        detail: Option<String>,
    ) {
        self.write(MetricRecord {
            source: "server".to_owned(),
            event: event.to_owned(),
            phase: phase.to_owned(),
            timestamp_us: end_us,
            replica_pid: self.replica_pid,
            peer_pid,
            gc_marker,
            sent_us: None,
            received_us,
            start_us: Some(start_us),
            end_us: Some(end_us),
            insert_count: None,
            delete_count: None,
            detail,
            client_id: None,
            operation: None,
            value: None,
            status_code: None,
            latency_us: Some(end_us.saturating_sub(start_us)),
        });
    }

    fn write(&mut self, record: MetricRecord) {
        self.writer
            .serialize(record)
            .expect("Failed to write metric");
        self.writer.flush().expect("Failed to flush metric");
    }
}

#[cfg(test)]
mod tests {
    use super::Metrics;
    use crate::prelude::now_micros;
    use std::collections::HashMap;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn stale_gc_delta_refusal_records_counters_and_timing() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock drifted before unix epoch")
            .as_nanos();
        let result_dir = std::env::temp_dir().join(format!("gresse-stale-gc-metric-{unique}"));
        let received_us = now_micros();
        let mut metrics = Metrics::create(&result_dir, 7);

        metrics.peer_replication_merge_delta_refused_stale_gc(4, 6, received_us, 12, 99);

        let mut reader = csv::Reader::from_path(result_dir.join("server_7.csv"))
            .expect("failed to read metric CSV");
        let row = reader
            .deserialize::<HashMap<String, String>>()
            .next()
            .expect("missing metric row")
            .expect("failed to deserialize metric row");
        let timestamp_us = row["timestamp_us"]
            .parse::<u128>()
            .expect("invalid refusal timestamp");

        assert_eq!(row["event"], "peer_replication_merge_delta");
        assert_eq!(row["phase"], "refused_stale_gc");
        assert_eq!(row["gc_marker"], "4");
        assert_eq!(row["sent_us"], "99");
        assert_eq!(row["received_us"], received_us.to_string());
        assert_eq!(
            row["detail"],
            "remote_gc_counter=4,current_gc_counter=6,delta_len=12"
        );
        assert_eq!(
            row["latency_us"].parse::<u128>().expect("invalid latency"),
            timestamp_us.saturating_sub(received_us)
        );

        drop(reader);
        drop(metrics);
        std::fs::remove_dir_all(result_dir).expect("failed to clean metric test directory");
    }
}
