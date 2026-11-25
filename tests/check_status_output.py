#!/usr/bin/env python3

import json
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time

from check_run import (
    Agent,
    check_result,
    event,
    event_id,
    expect,
    frame,
    read_rows,
    stats,
    unused_port,
    write_config,
)


def effective_status_path(executable, config, data_dir):
    result = subprocess.run(
        [
            str(executable),
            "--config", str(config),
            "--check-config",
            "--port", str(unused_port()),
            "--data-dir", str(data_dir),
        ],
        capture_output=True,
        timeout=5,
        check=False,
    )

    expect(
        result.returncode == 0 and result.stderr == b"",
        f"Configuration check failed: {result!r}",
    )

    expect(
        not data_dir.exists(),
        "--check-config created the data directory",
    )

    lines = [
        line[len("status_file: "):]
        for line in result.stdout.decode("utf-8").splitlines()
        if line.startswith("status_file: ")
    ]

    expect(len(lines) == 1, "Missing effective status_file")

    path = Path(lines[0])
    expect(path.is_absolute(), f"Status path is not absolute: {path}")

    return path


def wait_snapshot(path, predicate, timeout=5):
    deadline = time.monotonic() + timeout
    latest = None

    while time.monotonic() < deadline:
        try:
            contents = path.read_text(encoding="utf-8")
        except FileNotFoundError:
            time.sleep(0.02)
            continue

        # 已出现的文件若不是完整 JSON，立即失败。
        # 不能通过忽略解码错误掩盖半截输出。
        latest = json.loads(contents)

        if predicate(latest):
            return latest

        time.sleep(0.02)

    raise AssertionError(
        f"Timed out waiting for status: {path}, latest={latest!r}"
    )


def has_outbox(snapshot, total):
    return (
        snapshot["outbox"]["state"] == "VALID"
        and snapshot["outbox"]["value"]["total"] == total
    )


def lifecycle_case(executable, root):
    config = root / "demo.yml"
    write_config(config)

    data_dir = root / "agent"
    status_path = effective_status_path(
        executable, config, data_dir
    )

    with Agent(executable, config, data_dir) as agent:
        agent.wait_ready()

        initial = wait_snapshot(
            status_path,
            lambda value: (
                value["process_state"] == "RUNNING"
                and has_outbox(value, 0)
            ),
        )

        expect(initial["snapshot_version"] == 1, "Wrong version")
        expect(initial["active_connections"] == 0, "Wrong connection count")
        expect(initial["upload"]["state"] == "ENABLED", "Upload not enabled")

        with socket.create_connection(
            ("127.0.0.1", agent.port),
            timeout=3,
        ) as client:
            wait_snapshot(
                status_path,
                lambda value: value["active_connections"] == 1,
            )

            client.sendall(frame(event(1)) + frame(event(2)))
            agent.wait_line(f"Event stored: {event_id(2)}\n")

            running = wait_snapshot(
                status_path,
                lambda value: (
                    value["counters"]["stored_events"] == 2
                    and has_outbox(value, 2)
                ),
            )

            expect(
                running["outbox"]["value"]["unacked"] == 2
                and running["outbox"]["value"]["backlog"] == 2,
                f"Wrong Outbox counts: {running!r}",
            )

        wait_snapshot(
            status_path,
            lambda value: value["active_connections"] == 0,
        )

        expected = (
            agent.ready
            + (
                f"Event stored: {event_id(1)}\n"
                f"Event stored: {event_id(2)}\n"
            ).encode()
            + stats(completed=2, valid=2, stored=2)
        )

        check_result(agent.finish(), expected)

    stopped = json.loads(status_path.read_text(encoding="utf-8"))

    expect(
        stopped["process_state"] == "STOPPED"
        and stopped["upload"]["state"] == "STOPPED"
        and stopped["upload"]["finished"] is True
        and stopped["active_connections"] == 0,
        f"Invalid stopped snapshot: {stopped!r}",
    )

    expect(
        stopped["monitoring"]["previous_successful_writes"] >= 1
        and stopped["monitoring"]
                   ["previous_successful_snapshot_at_ms"] is not None,
        "Missing previous successful snapshot information",
    )

    # 相同数据库重新启动，本次计数归零，数据库存量保留。
    with Agent(executable, config, data_dir) as agent:
        agent.wait_ready()

        restarted = wait_snapshot(
            status_path,
            lambda value: (
                value["process_state"] == "RUNNING"
                and value["counters"]["stored_events"] == 0
                and has_outbox(value, 2)
            ),
        )

        expect(
            restarted["outbox"]["value"]["pending"] == 2,
            f"Restart changed pending records: {restarted!r}",
        )

        check_result(agent.finish(), agent.ready + stats())


def output_failure_case(executable, root):
    config = root / "demo.yml"
    write_config(config)

    data_dir = root / "agent"
    status_path = effective_status_path(
        executable, config, data_dir
    )

    # 用目录占据目标文件位置，稳定制造 rename 失败。
    status_path.mkdir(parents=True)

    with Agent(executable, config, data_dir) as agent:
        agent.wait_ready()

        agent.send(frame(event(1)))
        agent.wait_line(f"Event stored: {event_id(1)}\n")

        rows = read_rows(data_dir)
        expect(
            len(rows) == 1 and rows[0][0] == event_id(1),
            f"Output failure prevented persistence: {rows!r}",
        )

        deadline = time.monotonic() + 1.2

        while time.monotonic() < deadline:
            expect(
                agent.process.poll() is None,
                "Status output failure stopped the Agent",
            )
            time.sleep(0.05)

        # 消除故障，下一轮应自动恢复输出。
        status_path.rmdir()

        recovered = wait_snapshot(
            status_path,
            lambda value: (
                value["process_state"] == "RUNNING"
                and value["counters"]["stored_events"] == 1
                and value["monitoring"]["output_errors"] >= 1
            ),
        )

        expect(
            recovered["counters"]["storage_errors"] == 0,
            "Monitoring error was counted as a business storage error",
        )

        result = agent.finish()

        expected_stdout = (
            agent.ready
            + f"Event stored: {event_id(1)}\n".encode()
            + stats(completed=1, valid=1, stored=1)
        )

        stderr_lines = result[2].decode("utf-8").splitlines()

        expect(
            result[0] == 0
            and result[1] == expected_stdout
            and len(stderr_lines) == 2
            and stderr_lines[0].startswith("Status output failed: ")
            and stderr_lines[1] == "Status output recovered",
            f"Unexpected failure/recovery result: {result!r}",
        )

    stopped = json.loads(status_path.read_text(encoding="utf-8"))

    expect(
        stopped["process_state"] == "STOPPED"
        and stopped["monitoring"]["output_errors"] >= 1,
        f"Missing final monitoring result: {stopped!r}",
    )


def main():
    if len(sys.argv) != 2:
        print(
            "Usage: check_status_output.py AGENT",
            file=sys.stderr,
        )
        return 2

    executable = Path(sys.argv[1]).resolve()

    cases = [
        ("lifecycle_and_restart", lifecycle_case),
        ("output_failure_and_recovery", output_failure_case),
    ]

    with tempfile.TemporaryDirectory(
        prefix="edge-status-contract-",
    ) as directory:
        root = Path(directory)

        for name, action in cases:
            case_root = root / name
            case_root.mkdir()

            try:
                action(executable, case_root)
            except Exception as error:
                print(f"FAIL {name}: {error}", file=sys.stderr)
                return 1

            print(f"PASS {name}", flush=True)

    print("All 2 status output scenarios passed", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())