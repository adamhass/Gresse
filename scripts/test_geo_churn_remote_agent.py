"""Regression tests for the geo-churn remote agent."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
from unittest import TestCase, main
from unittest.mock import patch


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


if __name__ == "__main__":
    main()
