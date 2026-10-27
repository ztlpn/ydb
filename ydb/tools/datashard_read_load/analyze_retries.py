#!/usr/bin/env python3
"""Summarize datashard_read_load JSONL reports using only the Python standard library."""

import argparse
import json
import sys
from pathlib import Path


POPULATIONS = ("long", "short", "probe")
COUNTERS = ("scheduled", "launched", "completed", "skipped_scheduler", "skipped_capacity")


def difference(after, before):
    result = {key: after[key] - before[key] for key in COUNTERS}
    result["statuses"] = {
        status: after["statuses"].get(status, 0) - before["statuses"].get(status, 0)
        for status in after["statuses"].keys() | before["statuses"].keys()
    }
    if any(result[key] < 0 for key in COUNTERS) or any(v < 0 for v in result["statuses"].values()):
        raise ValueError("cumulative counters decreased")
    return result


def validate_counts(stats):
    if any(not isinstance(stats[key], int) or stats[key] < 0 for key in COUNTERS):
        raise ValueError("counters must be nonnegative integers")
    if stats["scheduled"] != stats["launched"] + stats["skipped_scheduler"] + stats["skipped_capacity"]:
        raise ValueError("scheduled != launched + skipped_scheduler + skipped_capacity")
    if stats["completed"] != sum(stats["statuses"].values()):
        raise ValueError("completed != sum(statuses)")
    if any(not isinstance(count, int) or count < 0 for count in stats["statuses"].values()):
        raise ValueError("status counts must be nonnegative integers")
    if stats["outstanding"] != stats["launched"] - stats["completed"] or stats["outstanding"] < 0:
        raise ValueError("outstanding != launched - completed")


def read_report(path):
    records = []
    with path.open(encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, 1):
            if not line.strip():
                continue
            try:
                record = json.loads(line)
                if not isinstance(record, dict):
                    raise ValueError("record must be a JSON object")
                for population_stats in (record.get("totals", {}), record.get("origin", {})):
                    for stats in population_stats.values():
                        validate_counts(stats)
                for origin in record.get("origins", {}).values():
                    for stats in origin.values():
                        validate_counts(stats)
            except (ValueError, KeyError, TypeError) as error:
                raise ValueError(f"line {line_number}: {error}") from error
            records.append(record)
    if not records or records[0].get("kind") != "run_start":
        raise ValueError("expected run_start as the first record")
    if sum(record.get("kind") == "run_start" for record in records) != 1:
        raise ValueError("expected one run per file; analyze separate files separately")
    if records[-1].get("kind") != "final":
        raise ValueError("missing final record; the run is unfinished or the report is truncated")
    previous = None
    for record in records[1:]:
        if previous is not None:
            if record["elapsed_s"] < previous["elapsed_s"]:
                raise ValueError("elapsed_s decreased")
            for population in POPULATIONS:
                difference(record["totals"][population], previous["totals"][population])
        previous = record
    return records


def latency_ms(stats, key, count):
    if not count:
        return None
    return {name: value / 1000 for name, value in stats[key].items()}


def phase_timeline(points, population, phase_start):
    """Use cumulative differences: interval histograms may straddle phase boundaries."""
    rows = []
    for before, after in zip(points, points[1:]):
        delta = difference(after["totals"][population], before["totals"][population])
        seconds = after["elapsed_s"] - before["elapsed_s"]
        if seconds == 0:
            if any(delta[key] for key in COUNTERS):
                raise ValueError("counters changed at identical elapsed_s timestamps")
            continue
        rows.append({
            "start_s": before["elapsed_s"] - phase_start,
            "end_s": after["elapsed_s"] - phase_start,
            "duration_s": seconds,
            "dispatch_rps": delta["launched"] / seconds,
            "success_rps": delta["statuses"].get("SUCCESS", 0) / seconds,
            "outstanding_start": before["totals"][population]["outstanding"],
            "outstanding_end": after["totals"][population]["outstanding"],
            **delta,
        })
    return rows


def clean_tail(timeline, cap, target_rps):
    """Describe a sampled suffix, not a claim about executor/server queue state."""
    if not timeline or not cap or not target_rps:
        return None
    threshold = max(1, cap // 100)
    tail = []
    for row in reversed(timeline):
        errors = sum(count for status, count in row["statuses"].items() if status != "SUCCESS")
        if (max(row["outstanding_start"], row["outstanding_end"]) > threshold
                or errors or row["skipped_capacity"] or row["skipped_scheduler"]):
            break
        tail.append(row)
    seconds = sum(row["duration_s"] for row in tail)
    if seconds < 5:
        return None
    success_rps = sum(row["statuses"].get("SUCCESS", 0) for row in tail) / seconds
    dispatch_rps = sum(row["launched"] for row in tail) / seconds
    if min(success_rps, dispatch_rps) < 0.95 * target_rps:
        return None
    return {
        "start_s": tail[-1]["start_s"],
        "duration_s": seconds,
        "outstanding_threshold": threshold,
        "sampled_peak_outstanding": max(max(row["outstanding_start"], row["outstanding_end"]) for row in tail),
        "success_rps": success_rps,
        "dispatch_rps": dispatch_rps,
    }


def summarize(path, include_timeline=False):
    records = read_report(path)
    config, final = records[0], records[-1]
    phases = []
    for spec in config["phases"]:
        name = spec["name"]
        points = [record for record in records[1:] if record.get("phase") == name]
        if not points:
            if not final.get("interrupted"):
                raise ValueError(f"missing phase {name!r}")
            continue
        if points[0]["kind"] != "phase_start" or points[-1]["kind"] != "phase_end":
            raise ValueError(f"missing phase boundaries for {name!r}")
        start, end = points[0], points[-1]
        window_s = end["elapsed_s"] - start["elapsed_s"]
        if window_s <= 0:
            raise ValueError(f"phase {name!r} has no measured duration")
        configured_s = spec["duration_us"] / 1_000_000
        populations = {}
        for population in POPULATIONS:
            cohort = final["origins"][name][population]
            before, after = start["totals"][population], end["totals"][population]
            window = difference(after, before)
            if not (cohort["scheduled"] or before["outstanding"] or window["completed"]):
                continue
            timeline = phase_timeline(points, population, start["elapsed_s"])
            success = cohort["statuses"].get("SUCCESS", 0)
            result = {
                "target_rps": spec["rates"][population],
                "dispatch_rps": cohort["launched"] / configured_s,
                "delivery_pct": 100 * cohort["launched"] / cohort["scheduled"] if cohort["scheduled"] else None,
                "cohort": cohort,
                "success_pct": 100 * success / cohort["launched"] if cohort["launched"] else None,
                "overloaded_pct": 100 * cohort["statuses"].get("OVERLOADED", 0) / cohort["launched"] if cohort["launched"] else None,
                "latency_ms": latency_ms(cohort, "latency_us", cohort["completed"]),
                "success_latency_ms": latency_ms(cohort, "success_latency_us", success),
                "window_statuses": window["statuses"],
                "window_success_rps": window["statuses"].get("SUCCESS", 0) / window_s,
                "outstanding_start": before["outstanding"],
                "outstanding_end": after["outstanding"],
                "sampled_peak_outstanding": max(point["totals"][population]["outstanding"] for point in points),
                "clean_tail": clean_tail(timeline, config["caps"][population], spec["rates"][population]),
            }
            if include_timeline:
                result["timeline"] = timeline
            populations[population] = result
        phases.append({
            "name": name,
            "configured_duration_s": configured_s,
            "window_s": window_s,
            "populations": populations,
        })
    last_end = max((record["elapsed_s"] for record in records if record.get("kind") == "phase_end"), default=0)
    return {
        "file": str(path),
        "started_at": records[1].get("timestamp"),
        "config": {key: value for key, value in config.items() if key not in ("kind", "query_plan")},
        "drained": final["drained"],
        "interrupted": final.get("interrupted", False),
        "elapsed_s": final["elapsed_s"],
        "drain_s": final["elapsed_s"] - last_end,
        "outstanding": {name: stats["outstanding"] for name, stats in final["totals"].items()},
        "phases": phases,
    }


def number(value, digits=1):
    return "-" if value is None else f"{value:,.{digits}f}"


def table(headers, rows):
    widths = [max(len(str(row[i])) for row in [headers, *rows]) for i in range(len(headers))]
    for row in [headers, *rows]:
        print("  ".join(str(cell).ljust(width) for cell, width in zip(row, widths)))


def format_statuses(statuses):
    return ", ".join(f"{key}={value:,}" for key, value in sorted(statuses.items()) if value) or "-"


def print_report(report, include_timeline):
    config = report["config"]
    print(f"\n{report['file']} ({report['started_at']})")
    print(f"threads={config.get('threads', '?')}, dispatch_batch={config.get('dispatch_batch', '?')}, "
          f"rows/read={config['rows_per_read']}, caps={config['caps']}")
    print(f"drained={report['drained']}, interrupted={report['interrupted']}, "
          f"drain={report['drain_s']:.3f}s, final outstanding={sum(report['outstanding'].values()):,}")
    load_rows, outcome_rows = [], []
    for phase in report["phases"]:
        for population, stats in phase["populations"].items():
            cohort = stats["cohort"]
            label = f"{phase['name']}/{population}"
            load_rows.append([
                label, number(stats["target_rps"]), number(stats["dispatch_rps"]),
                number(stats["delivery_pct"]), number(stats["window_success_rps"]),
                f"{stats['outstanding_start']:,} -> {stats['outstanding_end']:,}",
                f"{stats['sampled_peak_outstanding']:,}",
                f"{cohort['skipped_scheduler']:,}/{cohort['skipped_capacity']:,}",
            ])
            latency = stats["success_latency_ms"] or {}
            outcome_rows.append([
                label, f"{cohort['completed']:,}/{cohort['launched']:,}",
                format_statuses(cohort["statuses"]),
                "/".join(number(latency.get(key), 3) for key in ("p50", "p99", "max")),
            ])
    print()
    table(["Phase/pop", "Target/s", "Dispatch/s", "Delivered%", "OK/s(window)", "Outstanding", "Peak*", "Skip sched/cap"], load_rows)
    print()
    table(["Phase/pop", "Done/launched", "Eventual statuses", "Success p50/p99/max ms"], outcome_rows)
    for phase in report["phases"]:
        for population in ("long", "short"):
            tail = phase["populations"].get(population, {}).get("clean_tail")
            if tail:
                print(f"{phase['name']}/{population}: clean tail from {tail['start_s']:.2f}s "
                      f"for {tail['duration_s']:.2f}s; {tail['success_rps']:,.1f} successes/s, "
                      f"sampled outstanding <= {tail['outstanding_threshold']:,}.")
    if include_timeline:
        print("\nTimeline (seconds relative to phase_start; completions include earlier origins):")
        rows = []
        for phase in report["phases"]:
            for population, stats in phase["populations"].items():
                if population == "probe":
                    continue
                for row in stats["timeline"]:
                    rows.append([
                        f"{phase['name']}/{population}", f"{row['start_s']:.2f}-{row['end_s']:.2f}",
                        number(row["dispatch_rps"]), number(row["success_rps"]),
                        f"{row['outstanding_end']:,}",
                        f"{row['skipped_scheduler']:,}/{row['skipped_capacity']:,}",
                        format_statuses({key: value for key, value in row["statuses"].items() if key != "SUCCESS"}),
                    ])
        table(["Phase/pop", "Seconds", "Dispatch/s", "Success/s", "Outstanding", "Skip sched/cap", "Errors"], rows)


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
            if phase["name"] != "sustained" or "long" not in phase["populations"]:
                continue
            stats = phase["populations"]["long"]
            latency = stats["success_latency_ms"] or {}
            tail = stats["clean_tail"]
            rows.append([
                report["file"], number(stats["dispatch_rps"]), number(stats["window_success_rps"]),
                number(stats["overloaded_pct"], 2), f"{stats['outstanding_end']:,}",
                number(latency.get("p50"), 3), number(latency.get("p99"), 3),
                f"{tail['start_s']:.2f}s" if tail else "none",
            ])
    if len(rows) > 1:
        print("\nSustained long-read comparison:")
        table(["Report", "Dispatch/s", "OK/s(window)", "Cohort OL%", "End outstanding", "OK p50 ms", "OK p99 ms", "Clean tail from"], rows)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("reports", nargs="*", type=Path, help="JSONL files (default: retries.burst.jsonl)")
    parser.add_argument("--json", action="store_true", help="emit structured summaries as a JSON array")
    parser.add_argument("--timeline", action="store_true", help="include cumulative-difference interval statistics")
    args = parser.parse_args(argv)
    reports = []
    for path in args.reports or [Path("retries.burst.jsonl")]:
        try:
            reports.append(summarize(path, args.timeline))
        except (OSError, ValueError, KeyError, TypeError) as error:
            print(f"{path}: {error}", file=sys.stderr)
            return 1
    if args.json:
        print(json.dumps(reports, indent=2))
        return 0
    print("Dispatch/s uses configured phase duration; Delivered% = launched/scheduled.")
    print("Statuses and latency describe final phase-origin cohorts; OK/s(window) includes earlier origins.")
    print("Peak* and clean tails use sampled client outstanding, not server/executor queue depth.")
    print("Clean tail: >=5s, outstanding <= max(1, 1% of cap), no errors/skips, dispatch and success >=95% of target.")
    for report in reports:
        print_report(report, args.timeline)
    if len(reports) > 1:
        print_comparison(reports)
    return 0


if __name__ == "__main__":
    sys.exit(main())
