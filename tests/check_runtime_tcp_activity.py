#!/usr/bin/env python3

import os
from pathlib import Path
import select
import socket
import subprocess
import sys
import tempfile
import time

from check_run import event, expect, frame, unused_port

from check_runtime_jsonl import (
    JsonlAgent,
    parse_messages,
    read_status,
    validate_stream,
)


def write_config(path, threshold):
    path.write_text(
        "version: 1\n"
        "tcp:\n"
        "  bind: 127.0.0.1\n"
        "  port: 0\n"
        "  max_connections: 2\n"
        f"  silent_after_ms: {threshold}\n"
        "upload:\n"
        "  batch_size: 100\n"
        "  flush_interval_ms: 60000\n"
        "recording:\n"
        "  enabled: true\n",
        encoding="utf-8",
    )


def choose_threshold(executable, config, root):
    failures = []

    for threshold in (5000, 10000, 30000):
        write_config(config, threshold)

        checked_dir = root / f"checked-{threshold}"

        result = subprocess.run(
            [
                str(executable),
                "--config", str(config),
                "--check-config",
                "--port", str(unused_port()),
                "--data-dir", str(checked_dir),
            ],
            capture_output=True,
            timeout=5,
            check=False,
        )

        expect(
            not checked_dir.exists(),
            "--check-config created output directories",
        )

        if result.returncode == 0:
            return threshold

        failures.append(
            f"{threshold}: code={result.returncode}, "
            f"stderr={result.stderr!r}"
        )

    raise AssertionError(
        "No accepted silent threshold:\n"
        + "\n".join(failures)
    )


def drain_stdout(agent):
    # 等待状态文件时持续读取 JSONL，避免管道背压影响观察。
    for _ in range(8):
        ready, _, _ = select.select(
            [agent.process.stdout],
            [],
            [],
            0,
        )

        if not ready:
            return

        chunk = os.read(
            agent.process.stdout.fileno(),
            65536,
        )

        if not chunk:
            return

        agent.captured.extend(chunk)


def validate_activity(snapshot, threshold):
    activity = snapshot["tcp_activity"]

    expect(activity["state"] == "VALID", activity)
    expect(activity["silent_after_ms"] == threshold, activity)

    rows = activity["connections"]

    expect(
        activity["connected_count"]
        == len(rows)
        == snapshot["active_connections"],
        snapshot,
    )

    expect(
        activity["accepted_connections"]
        - activity["closed_connections"]
        == len(rows),
        activity,
    )

    for state, field in (
        ("WAITING", "waiting_count"),
        ("ACTIVE", "active_count"),
        ("SILENT", "silent_count"),
    ):
        expect(
            activity[field]
            == sum(row["state"] == state for row in rows),
            activity,
        )

    return activity


def wait_snapshot(
    agent,
    data_dir,
    run_id,
    threshold,
    predicate,
    timeout=10,
):
    deadline = time.monotonic() + timeout
    latest = None

    while time.monotonic() < deadline:
        drain_stdout(agent)

        expect(
            agent.process.poll() is None,
            f"Agent exited early; latest={latest!r}",
        )

        latest = read_status(data_dir)

        if latest is not None and latest["run_id"] == run_id:
            activity = validate_activity(latest, threshold)

            if predicate(latest, activity):
                return latest

        time.sleep(0.03)

    raise AssertionError(
        f"Timed out waiting for TCP activity: {latest!r}"
    )


def segment(data_dir, run_id, connection):
    return data_dir / "raw" / (
        f"edge-raw-v1-{run_id}"
        f"-c{connection:020d}"
        "-p00000000000000000001.bin"
    )


def finish_checked(agent, data_dir, threshold):
    code, stdout, stderr = agent.finish()

    expect(
        code == 0 and stderr == b"",
        f"code={code}, stderr={stderr!r}",
    )

    messages = parse_messages(stdout)
    run_id = validate_stream(messages)

    final = read_status(data_dir)

    expect(final is not None, "Missing final status")
    expect(final["run_id"] == run_id, final)
    expect(final["process_state"] == "STOPPED", final)

    activity = validate_activity(final, threshold)

    expect(activity["connected_count"] == 0, activity)
    expect(activity["connections"] == [], activity)

    expect(
        messages[-1]["data"]["snapshot"]["tcp_activity"]
        == activity,
        "Final status and stopped JSONL disagree",
    )

    for message in messages:
        if message["type"] == "status":
            validate_activity(message["data"], threshold)

    expect(
        final["counters"]["storage_errors"] == 0
        and final["counters"]["upload_errors"] == 0,
        final,
    )

    recording = final["recording"]

    expect(recording["finished"] is True, recording)
    expect(recording["active_files"] == 0, recording)

    expect(
        recording["bytes_received"]
        == recording["bytes_written"]
        == activity["bytes_received"],
        final,
    )

    return final


def lifecycle_case(executable, root):
    config = root / "demo.yml"

    threshold = choose_threshold(
        executable,
        config,
        root,
    )

    print(
        f"Using silent_after_ms={threshold}",
        flush=True,
    )

    data_dir = root / "agent"

    noise = b"\x00\xff\x00"
    first_wire = frame(event(1))
    second_wire = frame(event(2))

    with JsonlAgent(executable, config, data_dir) as agent:
        ready = agent.wait_ready()
        run_id = ready["run_id"]

        wait_snapshot(
            agent, data_dir, run_id, threshold,
            lambda snapshot, activity: (
                activity["accepted_connections"] == 0
                and activity["connected_count"] == 0
            ),
        )

        with socket.create_connection(
            ("127.0.0.1", agent.port),
            timeout=5,
        ) as first:
            waiting = wait_snapshot(
                agent, data_dir, run_id, threshold,
                lambda snapshot, activity: (
                    activity["connected_count"] == 1
                    and activity["waiting_count"] == 1
                ),
            )

            row = waiting["tcp_activity"]["connections"][0]

            expect(row["connection_id"] == 1, row)
            expect(row["bytes_received"] == 0, row)
            expect(row["last_data_age_ms"] is None, row)

            silent = wait_snapshot(
                agent, data_dir, run_id, threshold,
                lambda snapshot, activity: (
                    activity["connected_count"] == 1
                    and activity["silent_count"] == 1
                ),
                timeout=threshold / 1000 + 10,
            )

            expect(
                silent["tcp_activity"]["connections"][0]
                ["last_data_age_ms"] is None,
                silent,
            )

            # 噪声也应恢复 TCP 活动，但不会产生业务事件。
            first.sendall(noise)

            wait_snapshot(
                agent, data_dir, run_id, threshold,
                lambda snapshot, activity: (
                    activity["active_count"] == 1
                    and activity["bytes_received"] == len(noise)
                    and snapshot["counters"]["stored_events"] == 0
                ),
            )

            first.sendall(first_wire)

            wait_snapshot(
                agent, data_dir, run_id, threshold,
                lambda snapshot, activity: (
                    activity["active_count"] == 1
                    and activity["bytes_received"]
                    == len(noise) + len(first_wire)
                    and snapshot["counters"]["stored_events"] == 1
                ),
            )

        wait_snapshot(
            agent, data_dir, run_id, threshold,
            lambda snapshot, activity: (
                activity["connected_count"] == 0
                and activity["closed_connections"] == 1
            ),
        )

        # 复用已释放槽位，连接编号和接收计数应重新建立。
        with socket.create_connection(
            ("127.0.0.1", agent.port),
            timeout=5,
        ) as second:
            reused = wait_snapshot(
                agent, data_dir, run_id, threshold,
                lambda snapshot, activity: (
                    activity["accepted_connections"] == 2
                    and activity["waiting_count"] == 1
                ),
            )

            row = reused["tcp_activity"]["connections"][0]

            expect(row["slot"] == 0, row)
            expect(row["connection_id"] == 2, row)
            expect(row["bytes_received"] == 0, row)
            expect(row["last_data_age_ms"] is None, row)

            second.sendall(second_wire)

            wait_snapshot(
                agent, data_dir, run_id, threshold,
                lambda snapshot, activity: (
                    activity["active_count"] == 1
                    and snapshot["counters"]["stored_events"] == 2
                ),
            )

            # 保持连接，由 Agent 停止流程关闭。
            final = finish_checked(
                agent,
                data_dir,
                threshold,
            )

    activity = final["tcp_activity"]

    expect(activity["accepted_connections"] == 2, activity)
    expect(activity["closed_connections"] == 2, activity)
    expect(activity["client_errors"] == 0, activity)

    expect(
        activity["bytes_received"]
        == len(noise) + len(first_wire) + len(second_wire),
        activity,
    )

    expect(
        segment(data_dir, run_id, 1).read_bytes()
        == noise + first_wire,
        "First connection recording differs",
    )

    expect(
        segment(data_dir, run_id, 2).read_bytes()
        == second_wire,
        "Second connection recording differs",
    )


def main():
    if len(sys.argv) != 2:
        print(
            "Usage: check_runtime_tcp_activity.py AGENT",
            file=sys.stderr,
        )
        return 2

    executable = Path(sys.argv[1]).resolve()

    with tempfile.TemporaryDirectory(
        prefix="edge-tcp-activity-",
    ) as directory:
        try:
            lifecycle_case(executable, Path(directory))
        except Exception as error:
            print(
                f"FAIL lifecycle_and_reuse: {error}",
                file=sys.stderr,
            )
            return 1

    print("PASS lifecycle_and_reuse", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())