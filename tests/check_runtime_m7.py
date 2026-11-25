#!/usr/bin/env python3

import fcntl
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import time

from check_run import (
    event,
    event_id,
    expect,
    frame,
    unused_port,
)

from check_http_upload import (
    Receiver,
    stored_records,
    test_environment,
)

from check_outbox_upload import outbox_rows
from check_runtime_upload import write_upload_config

from check_runtime_jsonl import (
    parse_messages,
    read_status,
    validate_stream,
)


MAX_FILE_BYTES = 4 * 1024 * 1024
EVENT_COUNT = 200
INVALID_JSON_COUNT = 400


def wait_snapshot(process, data_dir, predicate, timeout=30):
    deadline = time.monotonic() + timeout
    latest = None

    while time.monotonic() < deadline:
        expect(
            process.poll() is None,
            f"Agent exited early: code={process.returncode}",
        )

        latest = read_status(data_dir)

        if latest is not None and predicate(latest):
            return latest

        time.sleep(0.03)

    raise AssertionError(
        f"Status observation timed out: {latest!r}"
    )


def close_fd(pipes, name, index):
    descriptor = pipes[name][index]

    if descriptor >= 0:
        os.close(descriptor)
        pipes[name][index] = -1


def read_all(descriptor):
    chunks = []

    while True:
        chunk = os.read(descriptor, 16384)

        if not chunk:
            return b"".join(chunks)

        chunks.append(chunk)


def descriptor_observations(pipes, original_flags):
    result = {}

    for name, descriptors in pipes.items():
        descriptor = descriptors[1]

        try:
            current = (
                fcntl.fcntl(descriptor, fcntl.F_GETFL)
                if descriptor >= 0
                else None
            )
        except OSError as error:
            current = str(error)

        result[name] = {
            "original_flags": original_flags.get(name),
            "current_flags": current,
        }

    return result


def verify_final(final, wire_size):
    expect(final["process_state"] == "STOPPED", final)
    expect(final["active_connections"] == 0, final)

    expected_counters = {
        "completed_frames":
            EVENT_COUNT + 2 + INVALID_JSON_COUNT + 1,

        "valid_events": EVENT_COUNT + 2,
        "json_errors": INVALID_JSON_COUNT,
        "validation_errors": 1,

        "stored_events": EVENT_COUNT,
        "duplicate_events": 1,
        "identity_conflicts": 1,

        "capacity_rejections": 0,
        "serialization_errors": 0,
        "storage_errors": 0,
        "storage_skipped": 0,

        "upload_claimed": EVENT_COUNT,
        "upload_acked": EVENT_COUNT,
        "upload_retry": 0,
        "upload_dead": 0,

        "upload_request_failures": 0,
        "upload_http_failures": 0,
        "upload_protocol_failures": 0,
        "upload_errors": 0,
        "upload_warnings": 0,
    }

    counters = final["counters"]

    for key, expected in expected_counters.items():
        expect(
            counters[key] == expected,
            f"{key}: expected={expected}, "
            f"actual={counters[key]}",
        )

    upload = final["upload"]

    expect(upload["state"] == "STOPPED", upload)
    expect(upload["finished"] is True, upload)
    expect(upload["failed"] is False, upload)
    expect(upload["blocked"] is False, upload)

    recording = final["recording"]

    expect(recording["state"] == "STOPPED", recording)
    expect(recording["finished"] is True, recording)

    expect(recording["connections_seen"] == 1, recording)
    expect(recording["bytes_received"] == wire_size, recording)
    expect(recording["bytes_written"] == wire_size, recording)
    expect(recording["bytes_skipped"] == 0, recording)

    expect(recording["opened_files"] == 2, recording)
    expect(recording["closed_files"] == 2, recording)

    expect(recording["active_sessions"] == 0, recording)
    expect(recording["active_files"] == 0, recording)

    expect(recording["files_deleted"] == 0, recording)
    expect(recording["retained_closed_files"] == 2, recording)

    for key in (
        "startup_errors",
        "event_errors",
        "session_errors",
        "retention_errors",
    ):
        expect(recording[key] == 0, recording)

    activity = final["tcp_activity"]

    expect(activity["state"] == "VALID", activity)
    expect(activity["accepted_connections"] == 1, activity)
    expect(activity["closed_connections"] == 1, activity)

    expect(activity["rejected_connections"] == 0, activity)
    expect(activity["client_errors"] == 0, activity)
    expect(activity["bytes_received"] == wire_size, activity)

    for key in (
        "connected_count",
        "waiting_count",
        "active_count",
        "silent_count",
    ):
        expect(activity[key] == 0, activity)

    expect(activity["connections"] == [], activity)

    # 管道背压应体现为展示损失。
    expect(final["display_dropped"] > 0, final)

    console = final["console"]

    expect(console["diagnostic_dropped"] > 0, console)
    expect(console["abandoned_lines"] > 0, console)

    expect(
        console["record_queue_drops"]
        + console["record_rate_drops"] > 0,
        console,
    )

    expect(console["encoding_errors"] == 0, console)
    expect(console["io_errors"] == 0, console)

    monitoring = final["monitoring"]

    for key in (
        "output_errors",
        "outbox_query_errors",
        "disk_query_errors",
    ):
        expect(monitoring[key] == 0, monitoring)


def combined_case(executable, receiver_script, root):
    data_dir = root / "agent"
    server_database = root / "receiver.db"
    config = root / "demo.yml"

    expected_events = {
        event_id(sequence): event(sequence)
        for sequence in range(1, EVENT_COUNT + 1)
    }

    changed = event(1)
    changed["value"] = 99.0

    # 超过一个录制文件上限，确保实际发生轮转。
    wire = b"\x00" * (MAX_FILE_BYTES + 257)

    wire += b"".join(
        frame(event(sequence))
        for sequence in range(1, EVENT_COUNT + 1)
    )

    wire += frame(event(1))       # 重复事件
    wire += frame(changed)        # 相同身份、不同内容
    wire += frame("{") * INVALID_JSON_COUNT
    wire += frame(event(0))       # 业务验证失败

    pipes = {}
    original_flags = {}

    process = None
    stage = "create_pipes"

    try:
        for name in ("stdout", "stderr"):
            read_fd, write_fd = os.pipe()

            pipes[name] = [read_fd, write_fd]

            # 固定缩小管道容量，使背压不依赖系统默认值。
            fcntl.fcntl(
                write_fd,
                fcntl.F_SETPIPE_SZ,
                4096,
            )

            original_flags[name] = fcntl.fcntl(
                write_fd,
                fcntl.F_GETFL,
            )

        with Receiver(
            receiver_script,
            server_database,
        ) as receiver:
            try:
                stage = "write_config"

                write_upload_config(
                    config,
                    receiver.url,
                    batch_size=20,
                )

                with config.open("a", encoding="utf-8") as output:
                    output.write(
                        "recording:\n"
                        "  enabled: true\n"
                        f"  max_file_bytes: {MAX_FILE_BYTES}\n"
                        "  keep_closed_files: 8\n"
                    )

                port = unused_port()

                stage = "start_agent"

                process = subprocess.Popen(
                    [
                        str(executable),
                        "--config", str(config),
                        "--run",
                        "--events-jsonl",
                        "--port", str(port),
                        "--data-dir", str(data_dir),
                    ],
                    stdout=pipes["stdout"][1],
                    stderr=pipes["stderr"][1],
                    env=test_environment(),
                    close_fds=True,
                )

                # 不读取管道，通过状态文件等待启动。
                stage = "wait_ready"

                wait_snapshot(
                    process,
                    data_dir,
                    lambda value: (
                        value["process_state"] == "RUNNING"
                        and value["upload"]["initialized"] is True
                        and value["recording"]["operational"] is True
                        and value["tcp_activity"]["state"] == "VALID"
                    ),
                    timeout=10,
                )

                for name in ("stdout", "stderr"):
                    flags = fcntl.fcntl(
                        pipes[name][1],
                        fcntl.F_GETFL,
                    )

                    expect(
                        flags & os.O_NONBLOCK,
                        f"{name} was not made nonblocking",
                    )

                stage = "send_input"

                with socket.create_connection(
                    ("127.0.0.1", port),
                    timeout=20,
                ) as client:
                    client.sendall(wire)

                stage = "wait_all_acked"

                wait_snapshot(
                    process,
                    data_dir,
                    lambda value: (
                        value["counters"]["stored_events"]
                        == EVENT_COUNT

                        and value["counters"]["upload_acked"]
                        == EVENT_COUNT

                        and value["counters"]["json_errors"]
                        == INVALID_JSON_COUNT

                        and value["counters"]["validation_errors"]
                        == 1

                        and value["recording"]["bytes_written"]
                        == len(wire)

                        and value["recording"]["closed_files"] == 2

                        and value["tcp_activity"]["connected_count"]
                        == 0
                    ),
                )

                # 停止与等待期间仍不读取 stdout/stderr。
                stage = "stop_with_unread_pipes"

                process.send_signal(signal.SIGTERM)
                process.wait(timeout=12)

                expect(
                    process.returncode == 0,
                    f"Unexpected exit code: {process.returncode}",
                )

                stage = "verify_descriptor_restoration"

                for name in ("stdout", "stderr"):
                    flags = fcntl.fcntl(
                        pipes[name][1],
                        fcntl.F_GETFL,
                    )

                    expect(
                        flags == original_flags[name],
                        f"{name} flags not restored: "
                        f"original={original_flags[name]}, "
                        f"current={flags}",
                    )

                # 子进程已退出，此时才关闭父进程保留的写端并读取。
                for name in ("stdout", "stderr"):
                    close_fd(pipes, name, 1)

                stdout = read_all(pipes["stdout"][0])
                stderr = read_all(pipes["stderr"][0])

                stage = "verify_final_status"

                final = read_status(data_dir)

                expect(final is not None, "Missing final status")

                verify_final(final, len(wire))

                stage = "verify_jsonl_prefix"

                # 有限收尾允许放弃尚未写完的尾行。
                # 已完成的 JSONL 行仍必须符合协议。
                messages = parse_messages(
                    stdout,
                    allow_partial=True,
                )

                run_id = validate_stream(
                    messages,
                    terminal=False,
                )

                expect(final["run_id"] == run_id, final)

                expect(
                    b"Event rejected: json" in stderr,
                    "No rejection diagnostic reached stderr",
                )

                stage = "verify_outbox_and_receiver"

                rows = outbox_rows(data_dir / "outbox.db")

                expect(
                    set(rows) == set(expected_events),
                    "Outbox event ID set differs",
                )

                expect(
                    all(
                        row["state"] == "ACKED"
                        and row["attempt_count"] == 1
                        and row["payload"] == expected_events[identity]
                        for identity, row in rows.items()
                    ),
                    "Outbox state, attempts or payload differs",
                )

                expect(
                    stored_records(server_database)
                    == expected_events,
                    "Receiver event set or payload differs",
                )

                stage = "verify_recorded_bytes"

                paths = sorted(
                    (data_dir / "raw").glob(
                        "edge-raw-v1-*.bin"
                    )
                )

                expected_paths = [
                    data_dir / "raw" / (
                        f"edge-raw-v1-{run_id}"
                        "-c00000000000000000001"
                        f"-p{part:020d}.bin"
                    )
                    for part in (1, 2)
                ]

                expect(
                    paths == expected_paths,
                    f"Unexpected recording files: {paths!r}",
                )

                expect(
                    [path.stat().st_size for path in paths]
                    == [
                        MAX_FILE_BYTES,
                        len(wire) - MAX_FILE_BYTES,
                    ],
                    "Recording rotation sizes differ",
                )

                expect(
                    b"".join(path.read_bytes() for path in paths)
                    == wire,
                    "Recorded raw bytes differ from input",
                )

            except Exception as error:
                try:
                    latest_status = read_status(data_dir)
                except Exception as status_error:
                    latest_status = {
                        "read_error": str(status_error),
                    }

                diagnostic = {
                    "stage": stage,
                    "reason": str(error),
                    "agent_returncode": (
                        process.poll()
                        if process is not None
                        else None
                    ),
                    "descriptors": descriptor_observations(
                        pipes,
                        original_flags,
                    ),
                    "status": latest_status,
                    "receiver_log_tail":
                        receiver.read_log()[-2000:],
                }

                raise AssertionError(
                    json.dumps(
                        diagnostic,
                        ensure_ascii=False,
                        indent=2,
                    )
                ) from None

            finally:
                if process is not None and process.poll() is None:
                    process.kill()
                    process.wait(timeout=5)

    finally:
        for name in tuple(pipes):
            close_fd(pipes, name, 0)
            close_fd(pipes, name, 1)


def main():
    if len(sys.argv) != 3:
        print(
            "Usage: check_runtime_m7.py AGENT RECEIVER",
            file=sys.stderr,
        )
        return 2

    executable = Path(sys.argv[1]).resolve()
    receiver_script = Path(sys.argv[2]).resolve()

    with tempfile.TemporaryDirectory(
        prefix="edge-m7-contract-",
    ) as directory:
        try:
            combined_case(
                executable,
                receiver_script,
                Path(directory),
            )
        except Exception as error:
            print(
                f"FAIL combined_backpressure: {error}",
                file=sys.stderr,
            )
            return 1

    print("PASS combined_backpressure", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())