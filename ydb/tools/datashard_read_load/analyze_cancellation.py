#!/usr/bin/env python3
"""Analyze cancellation.sh JSONL reports using only the Python standard library."""

import argparse
import json
import sys
from pathlib import Path

import analyze_retries as common


def percent(count, total):
    return 100 * count / total if total else None


def summarize(path, include_timeline=False):
    records = common.read_report(path)
    report = common.summarize_records(records, path, include_timeline)
    baseline = next((phase for phase in report["phases"] if phase["name"] == "baseline"), {})
    baseline_latency = baseline.get("populations", {}).get("probe", {}).get("latency_ms") or {}
    for phase in report["phases"]:
        points = [record for record in records[1:] if record.get("phase") == phase["name"]]
        start, end = points[0], points[-1]
        for population, stats in phase["populations"].items():
            cohort = stats["cohort"]
            stats["cancelled_pct"] = percent(cohort["statuses"].get("CANCELLED", 0), cohort["launched"])
            origin = end["origin"][population]
            stats["phase_end_cohort"] = {
                "completed": origin["completed"],
                "outstanding": origin["outstanding"],
                "statuses": origin["statuses"],
            }
            if population == "probe":
                median = (stats["latency_ms"] or {}).get("p50")
                base_median = baseline_latency.get("p50")
                stats["baseline_p50_ratio"] = median / base_median if median is not None and base_median else None
            if not include_timeline:
                continue
            # Collect(false) at phase boundaries does not reset interval histograms.
            # Only two consecutive interval records inside this phase describe
            # exactly the same window as a cumulative-difference timeline row.
            intervals = {
                after["elapsed_s"] - start["elapsed_s"]: after["interval"][population]
                for before, after in zip(points, points[1:])
                if before["kind"] == after["kind"] == "interval"
            }
            for row in stats["timeline"]:
                row["cancelled_rps"] = row["statuses"].get("CANCELLED", 0) / row["duration_s"]
                interval = intervals.get(row["end_s"])
                row["latency_ms"] = common.latency_ms(interval, "latency_us", interval["completed"]) if interval else None
    return report


def latency_text(latency):
    return "/".join(common.number((latency or {}).get(key), 3) for key in ("p50", "p99", "max"))


def probe_counts(cohort):
    return f"{cohort.get('statuses', {}).get('SUCCESS', 0)}/{cohort.get('launched', 0)}/{cohort.get('scheduled', 0)}"


def print_report(report, include_timeline):
    config = report["config"]
    print(f"\n{report['file']} ({report['started_at']})")
    print(f"threads={config.get('threads', '?')}, dispatch_batch={config.get('dispatch_batch', '?')}, "
          f"rows/read={config['rows_per_read']}, cancel_after={config['cancel_after_us'] / 1000:g}ms, "
          f"caps={config['caps']}")
    print(f"drained={report['drained']}, interrupted={report['interrupted']}, "
          f"drain={report['drain_s']:.3f}s, final outstanding={sum(report['outstanding'].values()):,}")
    load_rows, short_rows, probe_rows = [], [], []
    for phase in report["phases"]:
        for population, stats in phase["populations"].items():
            cohort = stats["cohort"]
            load_rows.append([
                f"{phase['name']}/{population}", common.number(stats["target_rps"]),
                common.number(stats["dispatch_rps"]), common.number(stats["delivery_pct"]),
                common.number(stats["window_success_rps"]),
                f"{stats['outstanding_start']:,} -> {stats['outstanding_end']:,}",
                f"{stats['sampled_peak_outstanding']:,}",
                f"{cohort['skipped_scheduler']:,}/{cohort['skipped_capacity']:,}",
            ])
            if population == "short":
                short_rows.append([
                    phase["name"], f"{cohort['completed']:,}/{cohort['launched']:,}",
                    common.format_statuses(cohort["statuses"]), common.number(stats["success_pct"], 2),
                    common.number(stats["cancelled_pct"], 2), latency_text(stats["latency_ms"]),
                    latency_text(stats["success_latency_ms"]),
                ])
            elif population == "probe":
                probe_rows.append([
                    phase["name"], probe_counts(cohort),
                    f"{stats['phase_end_cohort']['completed']}/{cohort['launched']}",
                    str(stats["phase_end_cohort"]["outstanding"]),
                    latency_text(stats["latency_ms"]), common.number(stats["baseline_p50_ratio"]),
                    common.format_statuses({key: value for key, value in cohort["statuses"].items() if key != "SUCCESS"}),
                ])
    print()
    common.table(["Phase/pop", "Target/s", "Dispatch/s", "Delivered%", "OK/s(window)", "Outstanding", "Peak*", "Skip sched/cap"], load_rows)
    print("\nShort-request outcomes (percentages of launched; latency in ms):")
    common.table(["Phase", "Done/launched", "Eventual statuses", "OK%", "Cancelled%", "All p50/p99/max", "Success p50/p99/max"], short_rows)
    print("\nProbes (latency includes all final outcomes):")
    common.table(["Phase", "OK/launched/scheduled", "Done at phase end", "Pending at phase end", "p50/p99/max ms", "p50/baseline", "Other outcomes"], probe_rows)
    for phase in report["phases"]:
        if phase["name"] != "recovery":
            continue
        tail = phase["populations"].get("short", {}).get("clean_tail")
        if tail:
            print(f"Recovery short-request clean tail from {tail['start_s']:.2f}s for {tail['duration_s']:.2f}s; "
                  f"{tail['success_rps']:,.1f} successes/s, sampled outstanding <= {tail['outstanding_threshold']}.")
        else:
            print("Recovery short-request clean tail: none observed.")
    if include_timeline:
        print("\nTimeline (seconds relative to phase_start; completions include earlier origins):")
        rows = []
        for phase in report["phases"]:
            for population, stats in phase["populations"].items():
                for row in stats["timeline"]:
                    latency = row["latency_ms"] or {}
                    rows.append([
                        f"{phase['name']}/{population}", f"{row['start_s']:.2f}-{row['end_s']:.2f}",
                        common.number(row["dispatch_rps"]), common.number(row["success_rps"]),
                        common.number(row["cancelled_rps"]), str(row["outstanding_end"]),
                        f"{row['skipped_scheduler']}/{row['skipped_capacity']}",
                        common.number(latency.get("p99"), 3), common.number(latency.get("max"), 3),
                        common.format_statuses({key: value for key, value in row["statuses"].items()
                                               if key not in ("SUCCESS", "CANCELLED")}),
                    ])
        common.table(["Phase/pop", "Seconds", "Dispatch/s", "OK/s", "Cancelled/s", "Outstanding", "Skip sched/cap", "All p99 ms", "All max ms", "Other outcomes"], rows)


def print_comparison(reports):
    reference = reports[0]["config"]
    for report in reports[1:]:
        config = report["config"]
        different = sorted(key for key in config.keys() | reference.keys() if config.get(key) != reference.get(key))
        if different:
            print(f"\nSettings differing from {reports[0]['file']} in {report['file']}: {', '.join(different)}")
    rows = []
    for report in reports:
        for phase in report["phases"]:
            if phase["name"] != "sustained":
                continue
            short, probe = (phase["populations"].get(population, {}) for population in ("short", "probe"))
            cohort = probe.get("cohort", {})
            latency = probe.get("latency_ms") or {}
            rows.append([
                report["file"], common.number(short.get("dispatch_rps")), common.number(short.get("window_success_rps")),
                common.number(short.get("cancelled_pct"), 2),
                common.number((short.get("latency_ms") or {}).get("p99"), 3),
                probe_counts(cohort),
                str(probe.get("phase_end_cohort", {}).get("outstanding", "-")),
                common.number(latency.get("p50"), 3), common.number(latency.get("max"), 3),
            ])
    if len(rows) > 1:
        print("\nSustained-phase comparison:")
        common.table(["Report", "Short dispatch/s", "Short OK/s(window)", "Cancelled%", "Short all p99 ms", "Probe OK/launched/sched", "Probe pending at end", "Probe p50 ms", "Probe max ms"], rows)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("reports", nargs="*", type=Path, help="JSONL files (default: cancellation.burst.jsonl)")
    parser.add_argument("--json", action="store_true", help="emit structured summaries as a JSON array")
    parser.add_argument("--timeline", action="store_true", help="include interval counts, rates, and latency")
    args = parser.parse_args(argv)
    reports = []
    for path in args.reports or [Path("cancellation.burst.jsonl")]:
        try:
            reports.append(summarize(path, args.timeline))
        except (OSError, ValueError, KeyError, TypeError) as error:
            print(f"{path}: {error}", file=sys.stderr)
            return 1
    if args.json:
        print(json.dumps(reports, indent=2))
        return 0
    print("Dispatch/s uses configured duration; Delivered% = launched/scheduled.")
    print("Statuses/latency are final phase-origin outcomes; OK/s(window) includes earlier origins.")
    print("Probe phase-end counts refer to that phase's launches, before later recovery or drain completions.")
    print("Peak* is sampled client outstanding. CANCELLED does not establish that server read execution was avoided.")
    print("Recovery clean tail: >=5s, outstanding <= max(1, 1% of cap), no errors/skips, dispatch and success >=95% of target.")
    if args.timeline:
        print("Timeline latency is omitted at phase boundaries where histogram and counter windows differ.")
    for report in reports:
        print_report(report, args.timeline)
    if len(reports) > 1:
        print_comparison(reports)
    return 0


if __name__ == "__main__":
    sys.exit(main())
