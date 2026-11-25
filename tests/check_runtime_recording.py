#!/usr/bin/env python3

from pathlib import Path
import socket
import subprocess
import sys
import tempfile

from check_run import (
    event,
    event_id,
    expect,
    frame,
    unused_port,
    write_config,
)

from check_http_upload import (
    Receiver,
    stored_records,
)

from check_runtime_upload import write_upload_config

from check_runtime_jsonl import (
    JsonlAgent,
    parse_messages,
    read_status,
    validate_stream,
    wait_until,
)


MAX_FILE_BYTES = 4 * 1024 * 1024


def append_recording(path, enabled=True, keep=8):
    with path.open("a", encoding="utf-8") as output:
        output.write(
            "recording:\n"
            f"  enabled: {'true' if enabled else 'false'}\n"
            f"  max_file_bytes: {MAX_FILE_BYTES}\n"
            f"  keep_closed_files: {keep}\n"
        )


def recording_config(path, enabled=True, keep=8):
    write_config(path)
    append_recording(path, enabled=enabled, keep=keep)


def segment(data_dir, run_id, connection, part=1):
    return data_dir / "raw" / (
        f"edge-raw-v1-{run_id}"
        f"-c{connection:020d}"
        f"-p{part:020d}.bin"
    )


def recording_files(data_dir):
    return sorted(
        (data_dir / "raw").glob("edge-raw-v1-*.bin")
    )


def wait_snapshot(agent, data_dir, predicate):
    def query():
        code = agent.process.poll()

        expect(
            code is None,
            f"Agent exited before observation: code={code}",
        )

        return read_status(data_dir)

    return wait_until(
        query,
        lambda value: (
            value is not None and predicate(value)
        ),
        timeout=20,
    )


def finish_checked(agent, data_dir):
    code, stdout, stderr = agent.finish()

    expect(
        code == 0,
        f"Agent failed: code={code}, stderr={stderr!r}",
    )

    messages = parse_messages(stdout)
    run_id = validate_stream(messages)

    final = read_status(data_dir)

    expect(final is not None, "Missing final status")
    expect(final["run_id"] == run_id, final)
    expect(final["process_state"] == "STOPPED", final)

    recording = final["recording"]

    expect(recording["finished"] is True, recording)
    expect(recording["operational"] is False, recording)

    expect(recording["active_sessions"] == 0, recording)
    expect(recording["active_files"] == 0, recording)

    expect(
        messages[-1]["data"]["snapshot"]["recording"]
        == recording,
        "Stopped JSONL and final status disagree",
    )

    expect(
        final["counters"]["storage_errors"] == 0
        and final["counters"]["upload_errors"] == 0,
        final,
    )

    expect(final["upload"]["failed"] is False, final)

    for message in messages:
        if message["type"] == "status":
            expect(
                "recording" in message["data"],
                "Missing recording in JSONL status",
            )

    return final, stderr


def disabled_case(executable, receiver_script, root):
    config = root / "demo.yml"

    # 即使配置启用录制，单独检查配置也不能创建输出目录。
    recording_config(config, enabled=True)

    checked_dir = root / "checked-only"

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
        result.returncode == 0,
        f"Configuration check failed: {result!r}",
    )

    expect(
        not checked_dir.exists(),
        "--check-config created output directories",
    )

    recording_config(config, enabled=False)

    data_dir = root / "agent"

    with JsonlAgent(executable, config, data_dir) as agent:
        agent.wait_ready()

        agent.send(frame(event(1)))

        wait_snapshot(
            agent,
            data_dir,
            lambda value: (
                value["counters"]["stored_events"] == 1
            ),
        )

        final, stderr = finish_checked(agent, data_dir)

    expect(stderr == b"", stderr)

    recording = final["recording"]

    expect(recording["state"] == "DISABLED", recording)
    expect(recording["enabled"] is False, recording)

    expect(recording["bytes_received"] == 0, recording)
    expect(recording["bytes_written"] == 0, recording)

    expect(
        not (data_dir / "raw").exists(),
        "Disabled recording created a raw directory",
    )


def bytes_and_rotation_case(executable, receiver_script, root):
    config = root / "demo.yml"
    recording_config(config)

    data_dir = root / "agent"

    # 噪声超过一个文件上限，随后包含成功与失败的业务帧。
    wire = (
        b"\x00" * (MAX_FILE_BYTES + 17)
        + frame(event(1))
        + frame("{")
        + frame(event(0))
    )

    with JsonlAgent(executable, config, data_dir) as agent:
        agent.wait_ready()

        with socket.create_connection(
            ("127.0.0.1", agent.port),
            timeout=10,
        ) as client:
            client.sendall(wire[:3])
            client.sendall(wire[3:])

            running = wait_snapshot(
                agent,
                data_dir,
                lambda value: (
                    value["recording"]["bytes_received"]
                    == len(wire)
                    and value["recording"]["bytes_written"]
                    == len(wire)
                    and value["counters"]["stored_events"] == 1
                    and value["counters"]["json_errors"] == 1
                    and value["counters"]["validation_errors"] == 1
                ),
            )

            recording = running["recording"]
            run_id = running["run_id"]

            expect(recording["active_files"] == 1, recording)
            expect(recording["closed_files"] == 1, recording)

            paths = recording_files(data_dir)

            expect(
                paths == [
                    segment(data_dir, run_id, 1, 1),
                    segment(data_dir, run_id, 1, 2),
                ],
                f"Unexpected recording files: {paths!r}",
            )

            expect(
                [path.stat().st_size for path in paths]
                == [
                    MAX_FILE_BYTES,
                    len(wire) - MAX_FILE_BYTES,
                ],
                "Incorrect rotation sizes",
            )

            expect(
                b"".join(path.read_bytes() for path in paths)
                == wire,
                "Recorded bytes differ from received input",
            )

            # 客户端仍保持连接，由 Agent 停止流程关闭会话。
            final, stderr = finish_checked(agent, data_dir)

    recording = final["recording"]

    expect(recording["state"] == "STOPPED", recording)
    expect(recording["bytes_written"] == len(wire), recording)
    expect(recording["bytes_skipped"] == 0, recording)

    expect(recording["opened_files"] == 2, recording)
    expect(recording["closed_files"] == 2, recording)

    expect(recording["session_errors"] == 0, recording)
    expect(recording["retention_errors"] == 0, recording)

    expect(b"Event rejected: json" in stderr, stderr)
    expect(b"Event rejected: validation" in stderr, stderr)


def keep_zero_case(executable, receiver_script, root):
    config = root / "demo.yml"
    recording_config(config, keep=0)

    data_dir = root / "agent"

    first_wire = frame(event(1))
    second_wire = frame(event(2))

    with JsonlAgent(executable, config, data_dir) as agent:
        agent.wait_ready()

        with socket.create_connection(
            ("127.0.0.1", agent.port),
            timeout=5,
        ) as first:
            first.sendall(first_wire)

            initial = wait_snapshot(
                agent,
                data_dir,
                lambda value: (
                    value["recording"]["bytes_written"]
                    == len(first_wire)
                    and value["recording"]["active_files"] == 1
                ),
            )

            run_id = initial["run_id"]

            first_path = segment(data_dir, run_id, 1)
            second_path = segment(data_dir, run_id, 2)

            expect(first_path.exists(), "Active file missing")

            with socket.create_connection(
                ("127.0.0.1", agent.port),
                timeout=5,
            ) as second:
                second.sendall(second_wire)
                second.shutdown(socket.SHUT_WR)

            running = wait_snapshot(
                agent,
                data_dir,
                lambda value: (
                    value["recording"]["bytes_written"]
                    == len(first_wire) + len(second_wire)
                    and value["recording"]["closed_files"] == 1
                    and value["recording"]["files_deleted"] == 1
                    and value["recording"]["active_files"] == 1
                    and value["counters"]["stored_events"] == 2
                ),
            )

            expect(first_path.exists(), "Active file was deleted")
            expect(
                first_path.read_bytes() == first_wire,
                "Active file content changed",
            )

            expect(
                not second_path.exists(),
                "Closed file was retained with keep=0",
            )

            expect(
                running["recording"]["retained_closed_files"]
                == 0,
                running,
            )

            final, stderr = finish_checked(agent, data_dir)

    expect(stderr == b"", stderr)

    recording = final["recording"]

    expect(recording["files_deleted"] == 2, recording)
    expect(recording["retained_closed_files"] == 0, recording)

    expect(
        recording_files(data_dir) == [],
        "Closed recordings remain after shutdown",
    )


def startup_failure_upload_case(
    executable,
    receiver_script,
    root,
):
    config = root / "demo.yml"
    data_dir = root / "agent"

    data_dir.mkdir()

    raw_path = data_dir / "raw"
    raw_path.write_bytes(b"marker")

    receiver_database = root / "receiver.db"

    with Receiver(
        receiver_script,
        receiver_database,
    ) as receiver:
        write_upload_config(
            config,
            receiver.url,
            batch_size=1,
        )

        append_recording(config)

        with JsonlAgent(
            executable,
            config,
            data_dir,
        ) as agent:
            agent.wait_ready()

            initial = wait_snapshot(
                agent,
                data_dir,
                lambda value: (
                    value["recording"]["startup_errors"] == 1
                ),
            )

            expect(
                initial["process_state"] == "RUNNING",
                initial,
            )

            expect(
                initial["recording"]["state"] == "FAILED",
                initial,
            )

            wire = frame(event(1))
            agent.send(wire)

            wait_snapshot(
                agent,
                data_dir,
                lambda value: (
                    value["recording"]["bytes_received"]
                    == len(wire)
                    and value["recording"]["bytes_skipped"]
                    == len(wire)
                    and value["counters"]["stored_events"] == 1
                    and value["counters"]["upload_acked"] == 1
                ),
            )

            expect(
                stored_records(receiver_database)
                == {event_id(1): event(1)},
                "Recording failure interrupted HTTP upload",
            )

            final, stderr = finish_checked(agent, data_dir)

    recording = final["recording"]

    expect(recording["state"] == "FAILED", recording)
    expect(recording["startup_errors"] == 1, recording)

    expect(recording["bytes_written"] == 0, recording)
    expect(recording["bytes_skipped"] == len(wire), recording)

    expect(
        final["counters"]["upload_acked"] == 1,
        final,
    )

    expect(raw_path.read_bytes() == b"marker", "Marker changed")

    expect(
        b"Raw recording warning:" in stderr,
        f"Missing recording diagnostic: {stderr!r}",
    )


def main():
    if len(sys.argv) != 3:
        print(
            "Usage: check_runtime_recording.py AGENT RECEIVER",
            file=sys.stderr,
        )
        return 2

    executable = Path(sys.argv[1]).resolve()
    receiver_script = Path(sys.argv[2]).resolve()

    cases = [
        ("disabled_and_config_check", disabled_case),
        ("bytes_and_rotation", bytes_and_rotation_case),
        ("keep_zero", keep_zero_case),
        ("startup_failure_upload", startup_failure_upload_case),
    ]

    with tempfile.TemporaryDirectory(
        prefix="edge-runtime-recording-",
    ) as directory:
        root = Path(directory)

        for name, action in cases:
            case_root = root / name
            case_root.mkdir()

            try:
                action(
                    executable,
                    receiver_script,
                    case_root,
                )
            except Exception as error:
                print(
                    f"FAIL {name}: {error}",
                    file=sys.stderr,
                )
                return 1

            print(f"PASS {name}", flush=True)

    print(
        "All 4 runtime recording scenarios passed",
        flush=True,
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())