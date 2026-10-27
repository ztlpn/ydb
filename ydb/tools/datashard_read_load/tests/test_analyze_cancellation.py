import copy
import importlib.util
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from unittest import mock


SCRIPT = Path(__file__).parents[1] / "analyze_cancellation.py"
SPEC = importlib.util.spec_from_file_location("analyze_cancellation", SCRIPT)
analyzer = importlib.util.module_from_spec(SPEC)
with mock.patch.object(sys, "path", [str(SCRIPT.parent), *sys.path]):
    SPEC.loader.exec_module(analyzer)


def counts(launched=0, success=0, cancelled=0, capacity=0, all_p99_us=200_000):
    histogram = {"p50": 22_000, "p95": 50_000, "p99": all_p99_us, "max": all_p99_us}
    return {
        "scheduled": launched + capacity, "launched": launched, "completed": success + cancelled,
        "outstanding": launched - success - cancelled, "skipped_scheduler": 0, "skipped_capacity": capacity,
        "statuses": {"SUCCESS": success, "CANCELLED": cancelled},
        "latency_us": histogram,
        "success_latency_us": {"p50": 6000, "p95": 7000, "p99": 8000, "max": 9000},
    }


def populations(short=None, probe=None):
    return {"long": counts(), "short": short or counts(), "probe": probe or counts()}


def record(kind, phase, elapsed, short=None, probe=None, origin=None, interval=None):
    result = {
        "kind": kind, "phase": phase, "elapsed_s": elapsed,
        "timestamp": "2026-10-27T09:00:00Z", "totals": populations(short, probe),
    }
    if origin is not None:
        result["origin"] = origin
    if interval is not None:
        result["interval"] = interval
        result["interval_s"] = 0.001  # Historical duration bug must not affect rates.
    return result


def report_fixture():
    baseline_short, baseline_probe = counts(10, 10), counts(5, 5, all_p99_us=6000)
    baseline_probe["latency_us"]["p50"] = 4000
    sustained_short = counts(100, 10, 90)
    sustained_probe = counts(8, 8, capacity=2, all_p99_us=25_000_000)
    sustained_probe["latency_us"]["p50"] = 20_000_000
    end_origin = populations(counts(100, 9, 90), counts(8, 3, capacity=2))
    return [
        {
            "kind": "run_start", "rows_per_read": 256, "cancel_after_us": 20_000,
            "caps": {"long": 0, "short": 8192, "probe": 16},
            "phases": [
                {"name": "baseline", "duration_us": 5_000_000, "rates": {"long": 0, "short": 2, "probe": 1}},
                {"name": "sustained", "duration_us": 10_000_000, "rates": {"long": 0, "short": 10, "probe": 1}},
            ],
        },
        record("phase_start", "baseline", 0, origin=populations()),
        record("phase_end", "baseline", 5, baseline_short, baseline_probe,
               origin=populations(baseline_short, baseline_probe)),
        record("phase_start", "sustained", 5, baseline_short, baseline_probe, origin=populations()),
        record("interval", "sustained", 6, counts(20, 12, 8), counts(6, 5),
               interval=populations(counts(20, 12, 8), counts(6, 5))),
        record("interval", "sustained", 10, counts(60, 15, 45), counts(9, 6, capacity=1),
               interval=populations(counts(40, 3, 37, all_p99_us=100_000), counts(3, 1, capacity=1))),
        record("phase_end", "sustained", 15, counts(110, 19, 90), counts(13, 8, capacity=2), origin=end_origin),
        {
            **record("final", "drain", 40, counts(110, 20, 90), counts(13, 13, capacity=2)),
            "drained": True, "interrupted": False,
            "origins": {
                "baseline": populations(baseline_short, baseline_probe),
                "sustained": populations(sustained_short, sustained_probe),
            },
        },
    ]


class AnalyzeCancellationTest(unittest.TestCase):
    def summarize(self, records, timeline=True):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "run.jsonl"
            path.write_text("".join(json.dumps(record) + "\n" for record in records), encoding="utf-8")
            return analyzer.summarize(path, timeline)

    def test_probe_eventual_success_does_not_hide_starvation(self):
        report = self.summarize(report_fixture())
        probe = report["phases"][1]["populations"]["probe"]
        self.assertEqual(analyzer.probe_counts(probe["cohort"]), "8/8/10")
        self.assertEqual(probe["cohort"]["skipped_capacity"], 2)
        self.assertEqual(probe["phase_end_cohort"]["completed"], 3)
        self.assertEqual(probe["phase_end_cohort"]["outstanding"], 5)
        self.assertEqual(probe["window_success_rps"], 0.3)
        self.assertEqual(probe["baseline_p50_ratio"], 5000)
        self.assertEqual(report["drain_s"], 25)

    def test_short_cancelled_percentage_and_all_status_latency(self):
        short = self.summarize(report_fixture())["phases"][1]["populations"]["short"]
        self.assertEqual(short["cancelled_pct"], 90)
        self.assertEqual(short["success_pct"], 10)
        self.assertEqual(short["window_success_rps"], 0.9)
        self.assertEqual(short["latency_ms"]["p99"], 200)
        self.assertEqual(short["success_latency_ms"]["p99"], 8)
        self.assertEqual(short["timeline"][0]["cancelled_rps"], 8)

    def test_boundary_histograms_are_not_attributed_to_new_phase(self):
        stats = self.summarize(report_fixture())["phases"][1]["populations"]["short"]
        first, middle, last = stats["timeline"]
        self.assertIsNone(first["latency_ms"])
        self.assertEqual(middle["latency_ms"]["p99"], 100)
        self.assertIsNone(last["latency_ms"])
        self.assertEqual(middle["duration_s"], 4)
        self.assertEqual(middle["cancelled_rps"], 37 / 4)

    def test_no_success_is_not_shown_as_zero_latency(self):
        records = report_fixture()
        short = records[-1]["origins"]["sustained"]["short"]
        short["statuses"] = {"CANCELLED": 100}
        # Preserve the total counts while moving successes to cancellations.
        records[-1]["totals"]["short"]["statuses"] = {"SUCCESS": 10, "CANCELLED": 100}
        with self.assertRaisesRegex(ValueError, "cumulative counters decreased"):
            self.summarize(records)
        records = report_fixture()
        for entry in records[3:]:
            total = entry["totals"]["short"]
            total["statuses"] = {"SUCCESS": 10, "CANCELLED": total["completed"] - 10}
        records[6]["origin"]["short"]["statuses"] = {"CANCELLED": 99}
        records[-1]["origins"]["sustained"]["short"]["statuses"] = {"CANCELLED": 100}
        short = self.summarize(records)["phases"][1]["populations"]["short"]
        self.assertIsNone(short["success_latency_ms"])
        self.assertEqual(short["cancelled_pct"], 100)
        self.assertEqual(short["latency_ms"]["p99"], 200)

    def test_text_comparison_and_json_share_metrics(self):
        report = self.summarize(report_fixture())
        output = io.StringIO()
        with redirect_stdout(output):
            analyzer.print_report(report, True)
            analyzer.print_comparison([report, copy.deepcopy(report)])
        self.assertIn("8/8/10", output.getvalue())
        self.assertIn("Pending at phase end", output.getvalue())
        self.assertIn("Sustained-phase comparison", output.getvalue())
        self.assertIn("25,000.000", output.getvalue())
        self.assertEqual(json.loads(json.dumps(report))["phases"][1]["populations"]["short"]["cancelled_pct"], 90)

    def test_missing_baseline_and_unfinished_report(self):
        records = report_fixture()
        records[0]["phases"][0]["name"] = "warmup"
        for entry in records[1:3]:
            entry["phase"] = "warmup"
        records[-1]["origins"]["warmup"] = records[-1]["origins"].pop("baseline")
        probe = self.summarize(records)["phases"][1]["populations"]["probe"]
        self.assertIsNone(probe["baseline_p50_ratio"])
        with self.assertRaisesRegex(ValueError, "missing final"):
            self.summarize(records[:-1])


if __name__ == "__main__":
    unittest.main()
