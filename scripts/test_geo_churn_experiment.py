"""Unit tests for identity-related geo-churn controller helpers."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
from unittest import TestCase, main


SOURCE = Path(__file__).with_name("geo_churn_experiment.py")
SPEC = importlib.util.spec_from_file_location("geo_churn_experiment", SOURCE)
assert SPEC is not None and SPEC.loader is not None
experiment = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = experiment
SPEC.loader.exec_module(experiment)


class ReplicaIdentityHelperTests(TestCase):
    def test_bootstrap_probe_reports_the_runtime_identity(self) -> None:
        self.assertEqual(experiment.replica_pid_from_probe("noise\n41\n"), 41)
        self.assertIsNone(experiment.replica_pid_from_probe("noise\n"))

    def test_remote_journal_paths_cover_every_sidecar(self) -> None:
        self.assertEqual(
            experiment.durability_journal_files("/tmp/replica.journal"),
            (
                "/tmp/replica.journal",
                "/tmp/replica.journal.snapshot.json",
                "/tmp/replica.journal.mutations.jsonl",
                "/tmp/.replica.journal.lock",
            ),
        )


if __name__ == "__main__":
    main()
