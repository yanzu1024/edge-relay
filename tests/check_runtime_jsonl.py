#!/usr/bin/env python3

import errno
import fcntl
import json
import os
from pathlib import Path
import re
import selectors
import signal
import socket
import subprocess
import sys
import tempfile
import time

from check_run import (
    Agent,
    event,
    event_id,
    expect,
    frame,
    stats,
    unused_port,
    write_config,
)
from check_http_upload import (
    Receiver,
    stored_records,
    test_environment,
)
from check_outbox_upload import outbox_rows
from check_runtime_upload import write_upload_config


def wait_until(query, predicate, timeout=20):
    deadline = time.monotonic() + timeout
    latest = None

    while time.monotonic() < deadline:
        latest = query()

        if predicate(latest):
            return latest

        time.sleep(0.03)

    raise AssertionError(f"Observation timed out: {latest!r}")


def read_status(data_dir):
    path = data_dir / "status.json"

    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None


def parse_messages(data, allow_partial=False):
    messages = []
    offset = 0

    while True:
        end = data.find(b"\n", offset)

        if end < 0:
            break

        line = data[offset:end + 1]

        expect(
            len(line) <= 65536,
            f"Oversize JSONL line: {len(line)}",
        )

        messages.append(json.loads(line.decode("utf-8")))
        offset = end + 1

    if not allow_partial:
        expect(offset == len(data), "Incomplete JSONL tail")

    return messages


def validate_stream(messages, terminal=True):
    expect(bool(messages), "Empty JSONL stream")

    first = messages[0]

    expect(
        first["type"] == "ready"
        and first["sequence"] == 1,
        f"Wrong first message: {first!r}",
    )

    run_id = first["run_id"]

    expect(
        re.fullmatch(r"[0-9a-f]{32}", run_id) is not None,
        f"Invalid run ID: {run_id!r}",
    )

    previous = 0

    for message in messages:
        expect(message["protocol_version"] == 1, message)
        expect(message["run_id"] == run_id, message)

        expect(
            type(message["sequence"]) is int
            and message["sequence"] > previous,
            f"Non-increasing sequence: {message!r}",
        )
        previous = message["sequence"]

        expect(
            type(message["emitted_at_ms"]) is int
            and message["emitted_at_ms"] > 0,
            message,
        )

        expect(
            message["type"] in {
                "ready", "status", "record", "stopped",
            },
            message,
        )

        if message["type"] == "status":
            expect(message["data"]["run_id"] == run_id, message)

        if message["type"] == "stopped":
            expect(
                message["data"]["snapshot"]["run_id"] == run_id,
                message,
            )

    expect(
        sum(message["type"] == "ready" for message in messages) == 1,
        "Repeated ready",
    )

    if terminal:
        expect(
            messages[-1]["type"] == "stopped"
            and sum(
                message["type"] == "stopped"
                for message in messages
            ) == 1,
            "Missing or misplaced stopped",
        )

    return run_id


def expected_counters(**arguments):
    result = {}

    for line in stats(**arguments).decode().splitlines()[1:]:
        key, value = line.split(": ", 1)

        if key not in {
            "upload_blocked",
            "upload_blocked_http_status",
        }:
            result[key] = int(value)

    return result


class JsonlAgent(Agent):
    def __init__(self, executable, config, data_dir, events=False):
        super().__init__(
            executable,
            config,
            data_dir,
            extra_options=(
                ("--events-jsonl",)
                if events
                else ("--jsonl",)
            ),
        )

    def wait_message(self, predicate):
        deadline = time.monotonic() + 5

        with selectors.DefaultSelector() as selector:
            selector.register(
                self.process.stdout,
                selectors.EVENT_READ,
            )

            while time.monotonic() < deadline:
                messages = parse_messages(
                    bytes(self.captured),
                    allow_partial=True,
                )

                for message in messages:
                    if predicate(message):
                        return message

                remaining = deadline - time.monotonic()

                if remaining <= 0:
                    break

                if not selector.select(remaining):
                    continue

                # 故意按小片段读取，不假定 read 就是一整行。
                chunk = os.read(
                    self.process.stdout.fileno(),
                    37,
                )

                expect(bool(chunk), "JSONL stdout closed early")
                self.captured.extend(chunk)

        raise AssertionError(
            f"Timed out waiting for JSONL: {bytes(self.captured)!r}"
        )

    def wait_ready(self):
        return self.wait_message(
            lambda message: message["type"] == "ready"
        )


def lifecycle_case(executable, root, events):
    config = root / "demo.yml"
    write_config(config)

    data_dir = root / "agent"

    with JsonlAgent(
        executable, config, data_dir, events=events
    ) as agent:
        ready = agent.wait_ready()

        expect(
            ready["data"]["events_jsonl"] is events,
            ready,
        )

        changed = event(1)
        changed["value"] = 99.0

        if events:
            agent.send(
                frame(event(1))
                + frame(event(1))
                + frame(changed)
                + frame("{")
                + frame(event(0))
                + frame(event(2))
            )

            agent.wait_message(
                lambda message: (
                    message["type"] == "record"
                    and message["data"]["event_id"] == event_id(2)
                )
            )

            expected = expected_counters(
                completed=6,
                valid=4,
                json_errors=1,
                validation_errors=1,
                stored=2,
                duplicates=1,
                conflicts=1,
            )

            expected_stderr = (
                f"Event rejected: identity_conflict {event_id(1)}\n"
                "Event rejected: json\n"
                "Event rejected: validation\n"
            ).encode()
        else:
            agent.send(frame(event(1)) + frame(event(2)))

            wait_until(
                lambda: read_status(data_dir),
                lambda value: (
                    value is not None
                    and value["counters"]["stored_events"] == 2
                ),
            )

            expected = expected_counters(
                completed=2, valid=2, stored=2
            )
            expected_stderr = b""

        result = agent.finish()

    expect(
        result[0] == 0 and result[2] == expected_stderr,
        f"Unexpected result: {result!r}",
    )

    messages = parse_messages(result[1])
    run_id = validate_stream(messages)

    final = messages[-1]["data"]["snapshot"]

    expect(final["counters"] == expected, final)
    expect(final["display_dropped"] == 0, final)
    expect(final["process_state"] == "STOPPED", final)

    records = [
        message["data"]
        for message in messages
        if message["type"] == "record"
    ]

    if events:
        expect(
            [item["disposition"] for item in records]
            == [
                "stored", "duplicate",
                "identity_conflict", "stored",
            ],
            records,
        )

        expect(
            [item["event"] for item in records]
            == [event(1), event(1), changed, event(2)],
            records,
        )
    else:
        expect(not records, "Records emitted without subscription")

    rows = outbox_rows(data_dir / "outbox.db")

    expect(
        {identity: row["payload"] for identity, row in rows.items()}
        == {event_id(1): event(1), event_id(2): event(2)},
        rows,
    )

    saved = read_status(data_dir)
    expect(saved["counters"] == final["counters"], saved)
    expect(saved["run_id"] == run_id, saved)

    return run_id


def upload_case(executable, receiver_script, root, blocked):
    mode = "http-401" if blocked else "normal"
    data_dir = root / "agent"
    server_db = root / "receiver.db"
    config = root / "demo.yml"

    with Receiver(receiver_script, server_db, mode) as receiver:
        write_upload_config(
            config,
            receiver.url,
            batch_size=1 if blocked else 2,
        )

        with JsonlAgent(executable, config, data_dir) as agent:
            agent.wait_ready()

            if blocked:
                agent.send(frame(event(1)))

                agent.wait_message(
                    lambda message: (
                        message["type"] == "status"
                        and message["data"]["upload"]["blocked"] is True
                    )
                )

                agent.send(frame(event(2)) + frame(event(3)))

                wait_until(
                    lambda: outbox_rows(data_dir / "outbox.db"),
                    lambda rows: len(rows) == 3,
                )

                time.sleep(0.3)

                expect(
                    receiver.read_log().count(
                        '"POST /v1/events/batch '
                    ) == 1,
                    "Blocked uploader sent another request",
                )

                expected = expected_counters(
                    completed=3,
                    valid=3,
                    stored=3,
                    upload_cycles=1,
                    upload_claimed=1,
                    upload_retry=1,
                    upload_http_failures=1,
                )

                expected_stderr = (
                    b"Upload blocked: http_status=401 recovery=restart\n"
                )
            else:
                agent.send(frame(event(1)) + frame(event(2)))

                agent.wait_message(
                    lambda message: (
                        message["type"] == "status"
                        and message["data"]["counters"]["upload_acked"] == 2
                    )
                )

                expected = expected_counters(
                    completed=2,
                    valid=2,
                    stored=2,
                    upload_cycles=1,
                    upload_claimed=2,
                    upload_acked=2,
                )

                expected_stderr = b""

            result = agent.finish()

        expect(
            result[0] == 0 and result[2] == expected_stderr,
            f"Unexpected upload result: {result!r}",
        )

        messages = parse_messages(result[1])
        validate_stream(messages)

        final = messages[-1]["data"]["snapshot"]
        expect(final["counters"] == expected, final)

        rows = outbox_rows(data_dir / "outbox.db")

        if blocked:
            expect(
                {
                    identity: (row["state"], row["attempt_count"])
                    for identity, row in rows.items()
                }
                == {
                    event_id(1): ("RETRY", 1),
                    event_id(2): ("PENDING", 0),
                    event_id(3): ("PENDING", 0),
                },
                rows,
            )

            expect(
                final["upload"]["blocked"] is True
                and final["upload"]["blocked_http_status"] == 401,
                final,
            )
        else:
            expect(
                all(row["state"] == "ACKED" for row in rows.values())
                and stored_records(server_db)
                == {event_id(1): event(1), event_id(2): event(2)},
                rows,
            )


def raw_output_case(
    executable,
    receiver_script,
    root,
    close_stdout,
):
    data_dir = root / "agent"
    server_db = root / "receiver.db"
    config = root / "demo.yml"

    count = 2 if close_stdout else 200
    invalid_count = 0 if close_stdout else 400

    out_read, out_write = os.pipe()
    err_read, err_write = os.pipe()

    process = None
    stage = "prepare"

    def inspect(query):
        try:
            return query()
        except Exception as error:
            return {
                "error": f"{type(error).__name__}: {error}",
            }

    def inspect_flags(descriptor, original):
        current = fcntl.fcntl(descriptor, fcntl.F_GETFL)

        return {
            "original": original,
            "current": current,
            "nonblocking": bool(current & os.O_NONBLOCK),
        }

    def inspect_states():
        rows = outbox_rows(data_dir / "outbox.db")
        counts = {}

        for row in rows.values():
            state = row["state"]
            counts[state] = counts.get(state, 0) + 1

        return {
            "records": len(rows),
            "states": counts,
        }

    def inspect_threads():
        result = {}

        if process is None:
            return result

        task_directory = Path(
            f"/proc/{process.pid}/task"
        )

        for path in task_directory.glob("*/wchan"):
            try:
                result[path.parent.name] = (
                    path.read_text().strip()
                )
            except OSError as error:
                result[path.parent.name] = str(error)

        return result

    try:
        fcntl.fcntl(out_write, fcntl.F_SETPIPE_SZ, 4096)
        fcntl.fcntl(err_write, fcntl.F_SETPIPE_SZ, 4096)

        original_out = fcntl.fcntl(out_write, fcntl.F_GETFL)
        original_err = fcntl.fcntl(err_write, fcntl.F_GETFL)

        if close_stdout:
            os.close(out_read)
            out_read = -1

        with Receiver(receiver_script, server_db) as receiver:
            write_upload_config(
                config,
                receiver.url,
                batch_size=2 if close_stdout else 20,
            )

            port = unused_port()

            try:
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
                    stdout=out_write,
                    stderr=err_write,
                    env=test_environment(),
                    close_fds=True,
                )

                def check_alive():
                    expect(
                        process.poll() is None,
                        f"Agent exited early: code={process.returncode}",
                    )

                def observe_status():
                    check_alive()
                    return read_status(data_dir)

                def observe_outbox():
                    check_alive()
                    return outbox_rows(data_dir / "outbox.db")

                # 第一步：先确认 Agent 已经进入 RUNNING。
                # 此时还没有发送测试事件。
                stage = "wait_running"

                wait_until(
                    observe_status,
                    lambda value: (
                        value is not None
                        and value["process_state"] == "RUNNING"
                    ),
                )

                expect(
                    bool(
                        fcntl.fcntl(out_write, fcntl.F_GETFL)
                        & os.O_NONBLOCK
                    ),
                    "Agent stdout is not nonblocking",
                )

                expect(
                    bool(
                        fcntl.fcntl(err_write, fcntl.F_GETFL)
                        & os.O_NONBLOCK
                    ),
                    "Agent stderr is not nonblocking",
                )

                expected_events = {
                    event_id(sequence): event(sequence)
                    for sequence in range(1, count + 1)
                }

                wire = b"".join(
                    frame(event(sequence))
                    for sequence in range(1, count + 1)
                )
                wire += frame("{") * invalid_count

                # 第二步：发送事件，全程不读取 stdout/stderr。
                stage = "send_tcp"

                with socket.create_connection(
                    ("127.0.0.1", port),
                    timeout=10,
                ) as client:
                    client.sendall(wire)

                # 第三步：发送完成后，才等待数据库全部 ACKED。
                stage = "wait_all_acked"

                rows = wait_until(
                    observe_outbox,
                    lambda value: (
                        len(value) == count
                        and all(
                            row["state"] == "ACKED"
                            for row in value.values()
                        )
                    ),
                )

                expect(
                    {
                        identity: row["payload"]
                        for identity, row in rows.items()
                    } == expected_events,
                    "Local event identities or payloads changed",
                )

                expect(
                    stored_records(server_db) == expected_events,
                    "Output failure changed receiver event set",
                )

                stage = "wait_status_counters"

                wait_until(
                    observe_status,
                    lambda value: (
                        value is not None
                        and value["counters"]["stored_events"] == count
                        and value["counters"]["json_errors"]
                            == invalid_count
                    ),
                )

                stage = "stop_agent"

                process.send_signal(signal.SIGTERM)

                # wait 不消费输出，收尾时管道依旧不可写。
                process.wait(timeout=8)

                expect(
                    process.returncode == 0,
                    f"Unexpected exit code: {process.returncode}",
                )

                stage = "check_final_state"

                expect(
                    fcntl.fcntl(out_write, fcntl.F_GETFL)
                        == original_out
                    and fcntl.fcntl(err_write, fcntl.F_GETFL)
                        == original_err,
                    "Output descriptor flags were not restored",
                )

                final = read_status(data_dir)

                expect(
                    final["process_state"] == "STOPPED",
                    final,
                )
                expect(
                    final["counters"]["stored_events"] == count,
                    final,
                )
                expect(
                    final["counters"]["upload_acked"] == count,
                    final,
                )
                expect(
                    final["counters"]["storage_errors"] == 0,
                    final,
                )
                expect(final["display_dropped"] > 0, final)

                if close_stdout:
                    expect(
                        final["console"]["stdout_enabled"] is False
                        and final["console"]["stdout_error"]
                            == errno.EPIPE,
                        final,
                    )
                else:
                    expect(
                        final["console"]["diagnostic_dropped"] > 0
                        and final["console"]["abandoned_lines"] > 0,
                        final,
                    )

            except Exception as error:
                # 在停止接收端、清理临时目录之前取得诊断。
                diagnostic = {
                    "stage": stage,
                    "reason": str(error).split(": ", 1)[0],
                    "agent_pid": (
                        process.pid if process is not None else None
                    ),
                    "agent_returncode": (
                        process.poll()
                        if process is not None
                        else None
                    ),
                    "expected_records": count,
                    "outbox": inspect(inspect_states),
                    "stdout": inspect(
                        lambda: inspect_flags(
                            out_write, original_out
                        )
                    ),
                    "stderr": inspect(
                        lambda: inspect_flags(
                            err_write, original_err
                        )
                    ),
                    "thread_waits": inspect(inspect_threads),
                    "status": inspect(
                        lambda: read_status(data_dir)
                    ),
                    "receiver_log_tail": receiver.read_log()[-3000:],
                }

                raise AssertionError(
                    "Backpressure diagnostics:\n"
                    + json.dumps(
                        diagnostic,
                        ensure_ascii=False,
                        indent=2,
                    )
                ) from None

        os.close(out_write)
        out_write = -1

        os.close(err_write)
        err_write = -1

        if out_read >= 0:
            captured = bytearray()

            while True:
                chunk = os.read(out_read, 4096)

                if not chunk:
                    break

                captured.extend(chunk)

            # 长期不读取时，允许最后一个未完成行被放弃。
            # 所有已经完整输出的行仍必须满足协议。
            messages = parse_messages(
                bytes(captured),
                allow_partial=True,
            )

            validate_stream(messages, terminal=False)

    finally:
        if process is not None and process.poll() is None:
            process.kill()
            process.wait(timeout=5)

        for descriptor in (
            out_read, out_write, err_read, err_write,
        ):
            if descriptor >= 0:
                os.close(descriptor)


def main():
    if len(sys.argv) != 3:
        print(
            "Usage: check_runtime_jsonl.py AGENT RECEIVER_SCRIPT",
            file=sys.stderr,
        )
        return 2

    executable = Path(sys.argv[1]).resolve()
    receiver_script = Path(sys.argv[2]).resolve()

    cases = [
        (
            "status_without_records",
            lambda root: lifecycle_case(executable, root, False),
        ),
        (
            "records_with_dispositions",
            lambda root: lifecycle_case(executable, root, True),
        ),
        (
            "jsonl_upload",
            lambda root: upload_case(
                executable, receiver_script, root, False
            ),
        ),
        (
            "jsonl_upload_blocked",
            lambda root: upload_case(
                executable, receiver_script, root, True
            ),
        ),
        (
            "unread_stdout_and_stderr",
            lambda root: raw_output_case(
                executable, receiver_script, root, False
            ),
        ),
        (
            "closed_stdout_reader",
            lambda root: raw_output_case(
                executable, receiver_script, root, True
            ),
        ),
    ]

    run_ids = []

    with tempfile.TemporaryDirectory(
        prefix="edge-runtime-jsonl-",
    ) as directory:
        root = Path(directory)

        for name, action in cases:
            case_root = root / name
            case_root.mkdir()

            try:
                result = action(case_root)
                if isinstance(result, str):
                    run_ids.append(result)
            except Exception as error:
                print(f"FAIL {name}: {error}", file=sys.stderr)
                return 1

            print(f"PASS {name}", flush=True)

    expect(
        len(run_ids) == len(set(run_ids)),
        "Different runs reused a run ID",
    )

    print("All 6 runtime JSONL scenarios passed", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())