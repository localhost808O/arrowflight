#!/usr/bin/env python3
"""Measure resource use and reliability of a benchmark feasibility command."""

import argparse
import datetime
import json
import os
import platform
import re
import shutil
import signal
import statistics
import subprocess
import sys
import time
from pathlib import Path


SCHEMA_VERSION = "1.0.0"
SIZE_PATTERN = re.compile(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*([A-Za-z]+)\s*$")
SIZE_FACTORS = {
    "b": 1,
    "kb": 1000,
    "kib": 1024,
    "mb": 1000**2,
    "mib": 1024**2,
    "gb": 1000**3,
    "gib": 1024**3,
    "tb": 1000**4,
    "tib": 1024**4,
}


def utc_now():
    """Return the current UTC time in ISO-8601 form."""
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def parse_size(value):
    """Convert one Docker human-readable byte quantity to bytes."""
    match = SIZE_PATTERN.match(str(value))
    if not match:
        return 0
    factor = SIZE_FACTORS.get(match.group(2).lower())
    if factor is None:
        return 0
    return int(float(match.group(1)) * factor)


def parse_pair(value):
    """Convert a Docker current/limit or read/write pair to byte values."""
    parts = str(value).split("/", maxsplit=1)
    first = parse_size(parts[0])
    second = parse_size(parts[1]) if len(parts) == 2 else 0
    return first, second


def directory_size(path):
    """Return the recursive byte size of one artifact path."""
    if not path.exists():
        return 0
    if path.is_file():
        return path.stat().st_size
    return sum(
        item.stat().st_size
        for item in path.rglob("*")
        if item.is_file()
    )


def read_memory():
    """Read total and currently used host memory from procfs."""
    values = {}
    try:
        for line in Path("/proc/meminfo").read_text(encoding="utf-8").splitlines():
            name, raw = line.split(":", maxsplit=1)
            values[name] = int(raw.strip().split()[0]) * 1024
    except (FileNotFoundError, OSError, ValueError):
        return {"total_bytes": 0, "used_bytes": 0, "available_bytes": 0}
    total = values.get("MemTotal", 0)
    available = values.get("MemAvailable", values.get("MemFree", 0))
    return {
        "total_bytes": total,
        "used_bytes": max(0, total - available),
        "available_bytes": available,
    }


def command_json(command):
    """Run a command that returns JSON and tolerate unavailable tools."""
    try:
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=20,
        )
    except (FileNotFoundError, OSError, subprocess.TimeoutExpired):
        return None
    if completed.returncode != 0 or not completed.stdout.strip():
        return None
    try:
        return json.loads(completed.stdout)
    except json.JSONDecodeError:
        return None


def docker_info():
    """Capture Docker capacity and storage configuration."""
    raw = command_json(["docker", "info", "--format", "{{json .}}"])
    if not isinstance(raw, dict):
        return {"available": False}
    return {
        "available": True,
        "server_version": raw.get("ServerVersion"),
        "cpus": raw.get("NCPU"),
        "memory_bytes": raw.get("MemTotal"),
        "docker_root_dir": raw.get("DockerRootDir"),
        "storage_driver": raw.get("Driver"),
        "cgroup_version": raw.get("CgroupVersion"),
    }


def docker_system_df():
    """Capture Docker image, container, volume, and cache disk totals."""
    try:
        completed = subprocess.run(
            ["docker", "system", "df", "--format", "{{json .}}"],
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )
    except (FileNotFoundError, OSError, subprocess.TimeoutExpired):
        return []
    rows = []
    for line in completed.stdout.splitlines():
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return rows


def docker_usage():
    """Capture aggregate live-container memory and block I/O counters."""
    try:
        completed = subprocess.run(
            ["docker", "stats", "--no-stream", "--format", "{{json .}}"],
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )
    except (FileNotFoundError, OSError, subprocess.TimeoutExpired):
        return {
            "container_count": 0,
            "memory_bytes": 0,
            "block_read_bytes": 0,
            "block_write_bytes": 0,
        }
    memory = 0
    block_read = 0
    block_write = 0
    containers = 0
    for line in completed.stdout.splitlines():
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        containers += 1
        used, _ = parse_pair(row.get("MemUsage", "0B / 0B"))
        read_bytes, write_bytes = parse_pair(row.get("BlockIO", "0B / 0B"))
        memory += used
        block_read += read_bytes
        block_write += write_bytes
    return {
        "container_count": containers,
        "memory_bytes": memory,
        "block_read_bytes": block_read,
        "block_write_bytes": block_write,
    }


def host_sample(path):
    """Capture one host and Docker resource sample."""
    disk = shutil.disk_usage(path)
    memory = read_memory()
    return {
        "timestamp": utc_now(),
        "epoch_seconds": time.time(),
        "memory_used_bytes": memory["used_bytes"],
        "memory_available_bytes": memory["available_bytes"],
        "disk_used_bytes": disk.used,
        "disk_available_bytes": disk.free,
        "load_average_1m": os.getloadavg()[0] if hasattr(os, "getloadavg") else 0,
        "docker": docker_usage(),
    }


def terminate_process(process):
    """Terminate a benchmark process and its child process group."""
    if process.poll() is not None:
        return
    if os.name == "posix":
        os.killpg(process.pid, signal.SIGTERM)
    else:
        process.terminate()
    try:
        process.wait(timeout=20)
    except subprocess.TimeoutExpired:
        if os.name == "posix":
            os.killpg(process.pid, signal.SIGKILL)
        else:
            process.kill()
        process.wait()


def runner_identity():
    """Capture the runner and operating-system identity."""
    memory = read_memory()
    disk = shutil.disk_usage(Path.cwd())
    return {
        "github_actions": os.environ.get("GITHUB_ACTIONS") == "true",
        "runner_name": os.environ.get("RUNNER_NAME"),
        "runner_environment": os.environ.get("RUNNER_ENVIRONMENT"),
        "runner_os": os.environ.get("RUNNER_OS"),
        "runner_arch": os.environ.get("RUNNER_ARCH"),
        "image_os": os.environ.get("ImageOS"),
        "image_version": os.environ.get("ImageVersion"),
        "kernel": platform.release(),
        "platform": platform.platform(),
        "cpu_count": os.cpu_count(),
        "memory_total_bytes": memory["total_bytes"],
        "disk_total_bytes": disk.total,
        "disk_available_bytes": disk.free,
        "docker": docker_info(),
    }


def write_trial_markdown(report, path):
    """Write a human-readable view of one feasibility trial."""
    usage = report["usage"]
    lines = [
        f"# Benchmark feasibility trial: {report['label']}",
        "",
        f"- Exit code: `{report['exit_code']}`",
        f"- Timed out: `{str(report['timed_out']).lower()}`",
        f"- Wall time: `{report['wall_time_seconds']:.3f} s`",
        f"- Peak host memory: `{usage['peak_host_memory_used_bytes']} bytes`",
        f"- Peak incremental host memory: `{usage['peak_incremental_host_memory_bytes']} bytes`",
        f"- Peak Docker memory: `{usage['peak_docker_memory_bytes']} bytes`",
        f"- Peak disk growth: `{usage['peak_disk_growth_bytes']} bytes`",
        f"- Docker block read/write: `{usage['docker_block_read_bytes']}` / "
        f"`{usage['docker_block_write_bytes']} bytes`",
        f"- Result artifact size: `{usage['artifact_size_bytes']} bytes`",
        f"- Samples: `{report['sample_count']}`",
        "",
        "This is a feasibility/reliability probe, not a performance-regression result.",
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def run_trial(args):
    """Run one measured feasibility trial."""
    command = list(args.command)
    if command and command[0] == "--":
        command = command[1:]
    if not command:
        raise SystemExit("A command is required after --")
    args.output.mkdir(parents=True, exist_ok=True)
    started_at = utc_now()
    started_epoch = time.monotonic()
    baseline_runner = runner_identity()
    docker_before = docker_system_df()
    samples = [host_sample(Path.cwd())]
    process = subprocess.Popen(command, start_new_session=(os.name == "posix"))
    timed_out = False
    while process.poll() is None:
        if time.monotonic() - started_epoch >= args.timeout_seconds:
            timed_out = True
            terminate_process(process)
            break
        time.sleep(args.sample_seconds)
        samples.append(host_sample(Path.cwd()))
    samples.append(host_sample(Path.cwd()))
    elapsed = time.monotonic() - started_epoch
    exit_code = 124 if timed_out else process.returncode
    baseline_memory = samples[0]["memory_used_bytes"]
    baseline_disk = samples[0]["disk_used_bytes"]
    peak_memory = max(row["memory_used_bytes"] for row in samples)
    peak_disk = max(row["disk_used_bytes"] for row in samples)
    docker_samples = [row["docker"] for row in samples]
    report = {
        "schema_version": SCHEMA_VERSION,
        "kind": "github-hosted-benchmark-feasibility-trial",
        "label": args.label,
        "command": command,
        "started_at": started_at,
        "finished_at": utc_now(),
        "wall_time_seconds": round(elapsed, 3),
        "exit_code": exit_code,
        "timed_out": timed_out,
        "runner": baseline_runner,
        "usage": {
            "baseline_host_memory_used_bytes": baseline_memory,
            "peak_host_memory_used_bytes": peak_memory,
            "peak_incremental_host_memory_bytes": max(
                0, peak_memory - baseline_memory
            ),
            "baseline_disk_used_bytes": baseline_disk,
            "peak_disk_used_bytes": peak_disk,
            "peak_disk_growth_bytes": max(0, peak_disk - baseline_disk),
            "final_disk_used_bytes": samples[-1]["disk_used_bytes"],
            "peak_docker_memory_bytes": max(
                row["memory_bytes"] for row in docker_samples
            ),
            "peak_container_count": max(
                row["container_count"] for row in docker_samples
            ),
            "docker_block_read_bytes": max(
                row["block_read_bytes"] for row in docker_samples
            ),
            "docker_block_write_bytes": max(
                row["block_write_bytes"] for row in docker_samples
            ),
            "artifact_size_bytes": directory_size(args.artifact),
        },
        "sample_count": len(samples),
        "samples": samples,
        "docker_system_df_before": docker_before,
        "docker_system_df_after": docker_system_df(),
    }
    json_path = args.output / "feasibility-metrics.json"
    json_path.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    write_trial_markdown(report, args.output / "feasibility-metrics.md")
    return exit_code


def coefficient_of_variation(values):
    """Return population coefficient of variation for numeric samples."""
    if not values or len(values) == 1:
        return 0.0
    mean = statistics.fmean(values)
    return statistics.pstdev(values) / mean if mean else 0.0


def write_summary_markdown(summary, path):
    """Write the aggregate feasibility and flakiness report."""
    lines = [
        "# GitHub-hosted benchmark feasibility summary",
        "",
        f"- Trials: `{summary['trial_count']}`",
        f"- Successful trials: `{summary['successful_trials']}`",
        f"- Failure rate: `{summary['failure_rate']:.3f}`",
        f"- Wall-time coefficient of variation: "
        f"`{summary['wall_time_coefficient_of_variation']:.3f}`",
        f"- Reliable smoke gate: `{str(summary['reliable_smoke']).lower()}`",
        "",
        "| Trial | Exit | Timeout | Wall s | Peak host bytes | "
        "Peak Docker bytes | Disk growth bytes | Artifact bytes |",
        "|---|---:|---|---:|---:|---:|---:|---:|",
    ]
    for trial in summary["trials"]:
        usage = trial["usage"]
        lines.append(
            f"| {trial['label']} | {trial['exit_code']} | "
            f"{str(trial['timed_out']).lower()} | "
            f"{trial['wall_time_seconds']:.3f} | "
            f"{usage['peak_host_memory_used_bytes']} | "
            f"{usage['peak_docker_memory_bytes']} | "
            f"{usage['peak_disk_growth_bytes']} | "
            f"{usage['artifact_size_bytes']} |"
        )
    lines.extend(
        [
            "",
            "The reliability gate requires three or more successful trials, "
            "no timeout, and wall-time CV at or below 15%. Passing it permits "
            "only a diagnostic smoke check; it does not establish stable "
            "performance-regression detection.",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def summarize(args):
    """Aggregate trial reports and calculate a basic flakiness gate."""
    paths = sorted(args.root.glob("trial-*/feasibility-metrics.json"))
    if not paths:
        raise SystemExit(f"No trial metrics found under {args.root}")
    trials = [json.loads(path.read_text(encoding="utf-8")) for path in paths]
    elapsed = [float(trial["wall_time_seconds"]) for trial in trials]
    successful = sum(
        trial["exit_code"] == 0 and not trial["timed_out"] for trial in trials
    )
    failure_rate = 1 - successful / len(trials)
    wall_cv = coefficient_of_variation(elapsed)
    reliable = len(trials) >= 3 and failure_rate == 0 and wall_cv <= 0.15
    summary = {
        "schema_version": SCHEMA_VERSION,
        "kind": "github-hosted-benchmark-feasibility-summary",
        "generated_at": utc_now(),
        "trial_count": len(trials),
        "successful_trials": successful,
        "failure_rate": failure_rate,
        "wall_time_seconds": {
            "minimum": min(elapsed),
            "median": statistics.median(elapsed),
            "maximum": max(elapsed),
        },
        "wall_time_coefficient_of_variation": wall_cv,
        "maximum_peak_host_memory_used_bytes": max(
            trial["usage"]["peak_host_memory_used_bytes"] for trial in trials
        ),
        "maximum_peak_docker_memory_bytes": max(
            trial["usage"]["peak_docker_memory_bytes"] for trial in trials
        ),
        "maximum_peak_disk_growth_bytes": max(
            trial["usage"]["peak_disk_growth_bytes"] for trial in trials
        ),
        "maximum_artifact_size_bytes": max(
            trial["usage"]["artifact_size_bytes"] for trial in trials
        ),
        "reliable_smoke": reliable,
        "trials": trials,
    }
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / "feasibility-summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    write_summary_markdown(
        summary,
        args.output / "feasibility-summary.md",
    )
    return 0


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Measure a GitHub-hosted benchmark feasibility probe."
    )
    subparsers = parser.add_subparsers(dest="action", required=True)
    run = subparsers.add_parser("run", help="Run and measure one trial.")
    run.add_argument("--output", type=Path, required=True)
    run.add_argument("--artifact", type=Path, required=True)
    run.add_argument("--label", required=True)
    run.add_argument("--timeout-seconds", type=int, default=3600)
    run.add_argument("--sample-seconds", type=float, default=5)
    run.add_argument("command", nargs=argparse.REMAINDER)
    aggregate = subparsers.add_parser(
        "summarize", help="Aggregate trial metrics."
    )
    aggregate.add_argument("--root", type=Path, required=True)
    aggregate.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def main():
    """Run the selected feasibility measurement action."""
    args = parse_args()
    if args.action == "run":
        return run_trial(args)
    return summarize(args)


if __name__ == "__main__":
    sys.exit(main())
