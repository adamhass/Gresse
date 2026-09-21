"""Regression tests for the geo-churn remote agent."""

from __future__ import annotations

import csv
import importlib.util
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
from unittest import TestCase, main
from unittest.mock import Mock, patch


SOURCE = Path(__file__).with_name("geo_churn_remote_agent.py")
SPEC = importlib.util.spec_from_file_location("geo_churn_remote_agent", SOURCE)
assert SPEC is not None and SPEC.loader is not None
agent = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = agent
SPEC.loader.exec_module(agent)


class SleepUntilTests(TestCase):
    def test_deadline_that_has_just_passed_does_not_sleep(self) -> None:
        with patch.object(agent.time, "sleep") as sleep:
            agent.sleep_until(10.0, 0.1, lambda: 10.000001)
        sleep.assert_not_called()

    def test_sleep_is_limited_by_remaining_time(self) -> None:
        with patch.object(agent.time, "sleep") as sleep:
            agent.sleep_until(10.0, 0.1, lambda: 9.95)
        sleep.assert_called_once()
        self.assertAlmostEqual(sleep.call_args.args[0], 0.05)


class ReplicaIdentityTests(TestCase):
    def write_completed_metrics(self, result_dir: Path, pid: int) -> None:
        with (result_dir / f"server_{pid}.csv").open("w", newline="") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=["source", "event", "phase", "replica_pid"],
            )
            writer.writeheader()
            writer.writerow({
                "source": "server",
                "event": "replica_init",
                "phase": "completed",
                "replica_pid": pid,
            })

    def test_completed_replica_pid_uses_the_runtime_identity(self) -> None:
        with TemporaryDirectory() as temporary_dir:
            result_dir = Path(temporary_dir)
            self.write_completed_metrics(result_dir, 41)

            self.assertEqual(agent.completed_replica_pid(result_dir), 41)

    def test_bootstrap_updates_the_agent_after_journal_recovery(self) -> None:
        with TemporaryDirectory() as temporary_dir:
            result_dir = Path(temporary_dir)
            self.write_completed_metrics(result_dir, 41)
            replica = agent.Replica(slot=0, pid=99, remote_dir=result_dir)
            replica.process = Mock()
            replica.process.poll.return_value = None
            lifecycle_agent = agent.Agent.__new__(agent.Agent)
            lifecycle_agent.startup_timeout_seconds = 1
            lifecycle_agent.log = Mock()

            lifecycle_agent.wait_until_bootstrapped(replica)

            self.assertEqual(replica.pid, 41)
            lifecycle_agent.log.write.assert_called_once()
            self.assertEqual(
                lifecycle_agent.log.write.call_args.args[0],
                "replica_pid_recovered",
            )

    def test_retiring_a_journal_removes_every_sidecar(self) -> None:
        with TemporaryDirectory() as temporary_dir:
            base_path = Path(temporary_dir) / "replica.journal"
            journal_files = agent.durability_journal_files(base_path)
            for journal_file in journal_files:
                journal_file.write_text("test")

            agent.retire_durability_journal(base_path)

            self.assertTrue(all(not journal_file.exists() for journal_file in journal_files))


if __name__ == "__main__":
    main()
