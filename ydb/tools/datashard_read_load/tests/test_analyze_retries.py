import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest


SPEC = importlib.util.spec_from_file_location("analyze_retries", Path(__file__).parents[1] / "analyze_retries.py")
analyzer = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(analyzer)


def counts(launched=0, success=0, overloaded=0):
    completed = success + overloaded
    histogram = {"p50": 2000, "p95": 5000, "p99": 9000, "max": 10000}
    return {
        "scheduled": launched,
        "launched": launched,
        "completed": completed,
        "outstanding": launched - completed,
        "skipped_scheduler": 0,
        "skipped_capacity": 0,
        "statuses": {"SUCCESS": success, "OVERLOADED": overloaded},
        "latency_us": histogram.copy(),
        "success_latency_us": histogram.copy(),
    }


def populations(long=None):
    return {"long": long or counts(), "short": counts(), "probe": counts()}


def phase_record(kind, phase, elapsed, totals, origin=None):
    result = {
        "kind": kind, "phase": phase, "elapsed_s": elapsed,
        "timestamp": "2026-10-27T09:00:00Z", "totals": populations(totals),
    }
    if origin is not None:
        result["origin"] = populations(origin)
    return result


def report_fixture():
    config = {
        "kind": "run_start", "rows_per_read": 1,
        "caps": {"long": 1000, "short": 0, "probe": 0},
        "phases": [
            {"name": name, "duration_us": 10_000_000, "rates": {"long": 10, "short": 0, "probe": 0}}
            for name in ("burst", "sustained")
        ],
    }
    burst_end = counts(100, 50, 30)
    sustained_end = counts(200, 163, 35)
    return [
        config,
        phase_record("phase_start", "burst", 0, counts(), counts()),
        phase_record("phase_end", "burst", 10, burst_end, burst_end),
        phase_record("phase_start", "sustained", 10, burst_end, counts()),
        # These failures belong to old burst requests, not the sustained cohort.
        phase_record("interval", "sustained", 11, counts(110, 75, 35)),
        phase_record("interval", "sustained", 15, counts(150, 115, 35)),
        phase_record("phase_end", "sustained", 20, sustained_end, counts(100, 98)),
        {
            **phase_record("final", "drain", 21, counts(200, 165, 35)),
            "drained": True, "interrupted": False,
            "origins": {"burst": populations(counts(100, 65, 35)), "sustained": populations(counts(100, 100))},
        },
    ]


class AnalyzeRetriesTest(unittest.TestCase):
    def summarize(self, records):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "run.jsonl"
            path.write_text("".join(json.dumps(record) + "\n" for record in records), encoding="utf-8")
            return analyzer.summarize(path, include_timeline=True)

    def test_cohort_window_and_drain_are_separate(self):
        summary = self.summarize(report_fixture())
        sustained = summary["phases"][1]["populations"]["long"]
        self.assertEqual(sustained["cohort"]["statuses"]["SUCCESS"], 100)
        self.assertEqual(sustained["dispatch_rps"], 10)
        # 113 successes occur during sustained load; 2 more finish during drain.
        self.assertEqual(sustained["window_success_rps"], 11.3)
        self.assertEqual(sustained["window_statuses"]["OVERLOADED"], 5)
        self.assertEqual(sustained["overloaded_pct"], 0)
        self.assertEqual(sustained["outstanding_start"], 20)
        self.assertEqual(sustained["outstanding_end"], 2)
        self.assertEqual(summary["drain_s"], 1)
        self.assertEqual(summary["outstanding"]["long"], 0)

    def test_timeline_ignores_old_interval_duration_bug(self):
        records = report_fixture()
        for record in records:
            if record["kind"] == "interval":
                record["interval_s"] = 0.001
                record["interval"] = populations(counts(9999, 9999))
        timeline = self.summarize(records)["phases"][1]["populations"]["long"]["timeline"]
        self.assertEqual([row["duration_s"] for row in timeline], [1, 4, 5])
        self.assertEqual(timeline[0]["dispatch_rps"], 10)
        self.assertEqual(timeline[0]["success_rps"], 25)
        self.assertEqual(sum(row["statuses"]["SUCCESS"] for row in timeline), 113)

    def test_recovery_tail_starts_after_backlog_and_failures(self):
        stats = self.summarize(report_fixture())["phases"][1]["populations"]["long"]
        self.assertEqual(stats["clean_tail"]["start_s"], 1)
        self.assertEqual(stats["clean_tail"]["duration_s"], 9)
        self.assertEqual(stats["clean_tail"]["outstanding_threshold"], 10)
        self.assertEqual(stats["clean_tail"]["sampled_peak_outstanding"], 2)

    def test_short_or_still_backlogged_suffix_is_not_recovery(self):
        summary = self.summarize(report_fixture())
        stats = summary["phases"][1]["populations"]["long"]
        self.assertIsNone(analyzer.clean_tail(stats["timeline"], 100, 10))  # threshold=1, ends at 2
        self.assertIsNone(analyzer.clean_tail(stats["timeline"][:2], 1000, 10))  # only 4 clean seconds

    def test_zero_success_histogram_is_absent_not_zero_latency(self):
        self.assertIsNone(analyzer.latency_ms(counts(10, 0, 10), "success_latency_us", 0))
        stats = self.summarize(report_fixture())["phases"][1]["populations"]["long"]
        self.assertEqual(stats["success_latency_ms"], {"p50": 2, "p95": 5, "p99": 9, "max": 10})

    def test_incomplete_drain_preserves_pending_cohort(self):
        records = report_fixture()
        records[-1]["drained"] = False
        records[-1]["totals"] = copy.deepcopy(records[-2]["totals"])
        records[-1]["origins"]["sustained"] = populations(counts(100, 98))
        summary = self.summarize(records)
        stats = summary["phases"][1]["populations"]["long"]
        self.assertFalse(summary["drained"])
        self.assertEqual(stats["cohort"]["outstanding"], 2)
        self.assertEqual(stats["success_pct"], 98)

    def test_invalid_accounting_and_missing_final_fail(self):
        records = report_fixture()
        records[4]["totals"]["long"]["scheduled"] += 1
        with self.assertRaisesRegex(ValueError, "line 5: scheduled"):
            self.summarize(records)
        with self.assertRaisesRegex(ValueError, "missing final"):
            self.summarize(report_fixture()[:-1])

    def test_interrupted_before_first_phase_is_accepted(self):
        records = report_fixture()
        final = {**phase_record("final", "drain", 0.1, counts()), "drained": True, "interrupted": True, "origins": {}}
        summary = self.summarize([records[0], final])
        self.assertEqual(summary["phases"], [])
        self.assertTrue(summary["interrupted"])


if __name__ == "__main__":
    unittest.main()
